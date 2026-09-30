//! The answer cache: what a backend said about one question of one state,
//! kept for a while so an identical call does not pay for it again.
//!
//! An entry is keyed by the batch key and the question key, both computed in
//! `wire`. The batch key covers the model, the state and every extra
//! top-level field, so an answer is only ever reused for exactly the same
//! model, state, extras and question.
//!
//! Answers are kept as the raw JSON the vendor sent, next to the layout of
//! the response they came in (`Envelope`), which is what lets a response be
//! rebuilt without a call. The cache never logs an answer: states and
//! questions, and so answers, can hold personal or financial data.
//!
//! Every entry lives for the same `ttl`, so the order in which entries were
//! written is also the order in which they expire. The store leans on that:
//! it keeps a queue in write order, drops the expired from its front, and
//! makes room by dropping the oldest. A read does not extend an entry's life,
//! so an answer is never served past `ttl` after the backend gave it.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use serde_json::value::RawValue;
use tokio::time::Instant;

use crate::config::{CacheConfig, millis};
use crate::wire::{BatchKey, ObjectWriter, QuestionKey, UpstreamAnswers};

/// Usage reported for a response made of cached answers only: the backend
/// billed nothing for them.
const NO_USAGE: &str = r#"{"input_tokens":0,"output_tokens":0}"#;

/// The layout of a vendor response: its fields in the order they were sent,
/// with the two the gateway fills in itself marked.
#[derive(Debug)]
pub struct Envelope(Vec<Part>);

#[derive(Debug)]
enum Part {
    Answers,
    Usage,
    /// Any other field, such as `model`, kept as sent.
    Field(String, Box<RawValue>),
}

impl Envelope {
    pub fn of(upstream: &UpstreamAnswers) -> Self {
        Self(
            upstream
                .envelope
                .iter()
                .map(|(name, raw)| match name.as_str() {
                    "answers" => Part::Answers,
                    "usage" => Part::Usage,
                    _ => Part::Field(name.clone(), raw.clone()),
                })
                .collect(),
        )
    }

    /// A response body holding `answers`, an encoded JSON object, and no
    /// billed tokens. A vendor that sent no `usage` gets none.
    pub fn render_free(&self, answers: &str) -> String {
        let mut out = ObjectWriter::with_capacity(answers.len() + 128);
        for part in &self.0 {
            match part {
                Part::Answers => out.field("answers", answers),
                Part::Usage => out.field("usage", NO_USAGE),
                Part::Field(name, raw) => out.field(name, raw.get()),
            }
        }
        out.finish()
    }
}

/// A cached answer, and the response it came in.
#[derive(Debug, Clone)]
pub struct Cached {
    pub answer: Box<RawValue>,
    pub envelope: Arc<Envelope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    batch: BatchKey,
    question: QuestionKey,
}

struct Entry {
    cached: Cached,
    expires: Instant,
    /// Tells this entry's slot in the queue from the slot of an older entry
    /// that it replaced under the same key.
    seq: u64,
}

#[derive(Default)]
struct Store {
    entries: HashMap<Key, Entry>,
    /// One slot per write, oldest first. A slot whose `seq` no longer
    /// matches its entry is stale: the entry was replaced or removed.
    queue: VecDeque<(Key, u64)>,
    next_seq: u64,
}

impl Store {
    /// Drops what has expired. The queue is in write order and every entry
    /// lives for the same time, so the expired ones are all at the front.
    fn purge(&mut self, now: Instant) {
        while let Some(&(key, seq)) = self.queue.front() {
            match self.entries.get(&key) {
                Some(entry) if entry.seq == seq => {
                    if entry.expires > now {
                        break;
                    }
                    self.entries.remove(&key);
                }
                _ => {}
            }
            self.queue.pop_front();
        }
    }

    /// Drops the oldest entry. False if there was none.
    fn evict_oldest(&mut self) -> bool {
        while let Some((key, seq)) = self.queue.pop_front() {
            if self.entries.get(&key).is_some_and(|entry| entry.seq == seq) {
                self.entries.remove(&key);
                return true;
            }
        }
        false
    }

    fn insert(&mut self, key: Key, cached: Cached, expires: Instant, max_entries: usize) {
        if !self.entries.contains_key(&key) {
            while self.entries.len() >= max_entries && self.evict_oldest() {}
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.entries.insert(
            key,
            Entry {
                cached,
                expires,
                seq,
            },
        );
        self.queue.push_back((key, seq));
        // A key written again leaves a stale slot behind. Sweep them before
        // they outnumber the entries, so the queue stays bounded too.
        if self.queue.len() > 2 * max_entries + 16 {
            let entries = &self.entries;
            self.queue
                .retain(|(key, seq)| entries.get(key).is_some_and(|entry| entry.seq == *seq));
        }
    }
}

pub struct AnswerCache {
    ttl: Duration,
    max_entries: usize,
    store: Mutex<Store>,
    /// How many entries the store holds, for `/metrics`.
    size: Gauge,
}

impl AnswerCache {
    pub fn new(config: &CacheConfig, size: Gauge) -> Self {
        Self {
            ttl: millis(config.ttl_ms),
            max_entries: config.max_entries,
            store: Mutex::new(Store::default()),
            size,
        }
    }

    /// The fresh answer to each of `questions` asked about `batch`, in the
    /// same order, or `None` where there is none. `now` is the time the
    /// lookup is made at; an expired entry is dropped on the way.
    pub fn lookup(
        &self,
        batch: BatchKey,
        questions: impl IntoIterator<Item = QuestionKey>,
        now: Instant,
    ) -> Vec<Option<Cached>> {
        let mut store = self.lock();
        let found = questions
            .into_iter()
            .map(|question| {
                let key = Key { batch, question };
                let entry = store.entries.get(&key)?;
                if entry.expires > now {
                    return Some(entry.cached.clone());
                }
                store.entries.remove(&key);
                None
            })
            .collect();
        self.size.set(store.entries.len() as i64);
        found
    }

    /// Keeps `answers`, received at `now` in a response laid out as
    /// `envelope`. An answer already held for the same question is replaced.
    pub fn store(
        &self,
        batch: BatchKey,
        answers: impl IntoIterator<Item = (QuestionKey, Box<RawValue>)>,
        envelope: &Arc<Envelope>,
        now: Instant,
    ) {
        let expires = now + self.ttl;
        let mut store = self.lock();
        store.purge(now);
        for (question, answer) in answers {
            let cached = Cached {
                answer,
                envelope: Arc::clone(envelope),
            };
            store.insert(Key { batch, question }, cached, expires, self.max_entries);
        }
        self.size.set(store.entries.len() as i64);
    }

    /// The store holds plain data and is consistent between statements, so
    /// a panic elsewhere is no reason to stop serving.
    fn lock(&self) -> MutexGuard<'_, Store> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{PreparedRequest, TokenEstimator};

    /// Real keys, as the gateway computes them: different `seed`s give
    /// different batch keys, different `question`s different question keys.
    fn keys(seed: &str, questions: &[&str]) -> (BatchKey, Vec<QuestionKey>) {
        let questions: Vec<String> = questions
            .iter()
            .map(|q| format!(r#""{q}":{{"type":"noul","instructions":"{q}?"}}"#))
            .collect();
        let body = format!(
            r#"{{"state":"{seed}","model":"jev-latest","questions":{{{}}}}}"#,
            questions.join(",")
        );
        let request = PreparedRequest::parse(body.as_bytes(), &TokenEstimator::new(3.0)).unwrap();
        (
            request.key,
            request.questions.iter().map(|q| q.key).collect(),
        )
    }

    fn envelope() -> Arc<Envelope> {
        let body =
            br#"{"model":"jev-1.13.0","answers":{},"usage":{"input_tokens":5,"output_tokens":1}}"#;
        Arc::new(Envelope::of(&UpstreamAnswers::parse(body).unwrap()))
    }

    fn answer(text: &str) -> Box<RawValue> {
        RawValue::from_string(format!(r#"{{"type":"noul","noul":0.5,"note":"{text}"}}"#)).unwrap()
    }

    fn cache(ttl_ms: u64, max_entries: usize) -> (AnswerCache, Gauge) {
        let size = Gauge::default();
        let config = CacheConfig {
            enabled: true,
            ttl_ms,
            max_entries,
        };
        (AnswerCache::new(&config, size.clone()), size)
    }

    fn found(cache: &AnswerCache, batch: BatchKey, question: QuestionKey, now: Instant) -> bool {
        cache.lookup(batch, [question], now)[0].is_some()
    }

    #[test]
    fn an_answer_is_served_until_its_ttl_and_not_after() {
        let (cache, size) = cache(1_000, 10);
        let (batch, questions) = keys("a", &["urgent"]);
        let t0 = Instant::now();
        cache.store(batch, [(questions[0], answer("x"))], &envelope(), t0);

        let hit = cache.lookup(batch, [questions[0]], t0 + Duration::from_millis(999));
        assert_eq!(
            hit[0].as_ref().unwrap().answer.get(),
            r#"{"type":"noul","noul":0.5,"note":"x"}"#
        );
        assert_eq!(size.get(), 1);

        // The answer expires exactly at ttl, and is dropped when it is found out.
        assert!(!found(
            &cache,
            batch,
            questions[0],
            t0 + Duration::from_millis(1_000)
        ));
        assert_eq!(size.get(), 0);
    }

    #[test]
    fn a_read_does_not_extend_an_entrys_life() {
        let (cache, _) = cache(1_000, 10);
        let (batch, questions) = keys("a", &["urgent"]);
        let t0 = Instant::now();
        cache.store(batch, [(questions[0], answer("x"))], &envelope(), t0);
        assert!(found(
            &cache,
            batch,
            questions[0],
            t0 + Duration::from_millis(900)
        ));
        assert!(!found(
            &cache,
            batch,
            questions[0],
            t0 + Duration::from_millis(1_100)
        ));
    }

    #[test]
    fn the_same_question_about_another_state_is_another_entry() {
        let (cache, _) = cache(1_000, 10);
        let (batch_a, questions_a) = keys("a", &["urgent"]);
        let (batch_b, questions_b) = keys("b", &["urgent"]);
        assert_eq!(questions_a[0], questions_b[0]);
        let t0 = Instant::now();
        cache.store(batch_a, [(questions_a[0], answer("x"))], &envelope(), t0);
        assert!(found(&cache, batch_a, questions_a[0], t0));
        assert!(!found(&cache, batch_b, questions_b[0], t0));
    }

    #[test]
    fn a_full_cache_drops_its_oldest_entry() {
        let (cache, size) = cache(60_000, 3);
        let (batch, questions) = keys("a", &["q1", "q2", "q3", "q4"]);
        let t0 = Instant::now();
        for (i, question) in questions.iter().enumerate() {
            let at = t0 + Duration::from_millis(i as u64);
            cache.store(batch, [(*question, answer("x"))], &envelope(), at);
        }
        assert_eq!(size.get(), 3);
        assert!(!found(&cache, batch, questions[0], t0));
        for question in &questions[1..] {
            assert!(found(&cache, batch, *question, t0));
        }
    }

    #[test]
    fn writing_a_question_again_renews_it_and_needs_no_room() {
        let (cache, size) = cache(1_000, 2);
        let (batch, questions) = keys("a", &["q1", "q2", "q3"]);
        let t0 = Instant::now();
        cache.store(batch, [(questions[0], answer("old"))], &envelope(), t0);
        cache.store(batch, [(questions[1], answer("x"))], &envelope(), t0);
        // q1 is written again: it is now the newest, and nothing is evicted.
        let later = t0 + Duration::from_millis(500);
        cache.store(batch, [(questions[0], answer("new"))], &envelope(), later);
        assert_eq!(size.get(), 2);
        // A third question pushes out q2, now the oldest.
        cache.store(batch, [(questions[2], answer("x"))], &envelope(), later);
        assert!(!found(&cache, batch, questions[1], later));
        let renewed = cache.lookup(batch, [questions[0]], later + Duration::from_millis(700));
        assert!(renewed[0].as_ref().unwrap().answer.get().contains("new"));
    }

    #[test]
    fn expired_entries_leave_before_live_ones_are_evicted() {
        let (cache, size) = cache(1_000, 2);
        let (batch, questions) = keys("a", &["q1", "q2", "q3"]);
        let t0 = Instant::now();
        cache.store(batch, [(questions[0], answer("x"))], &envelope(), t0);
        let later = t0 + Duration::from_millis(800);
        cache.store(batch, [(questions[1], answer("x"))], &envelope(), later);
        // q1 has expired by now; q2 is still fresh and stays.
        let much_later = t0 + Duration::from_millis(1_200);
        cache.store(
            batch,
            [(questions[2], answer("x"))],
            &envelope(),
            much_later,
        );
        assert_eq!(size.get(), 2);
        assert!(found(&cache, batch, questions[1], much_later));
        assert!(found(&cache, batch, questions[2], much_later));
    }

    #[test]
    fn rewriting_one_key_does_not_grow_the_queue_without_bound() {
        let (cache, _) = cache(60_000, 2);
        let (batch, questions) = keys("a", &["q1"]);
        let t0 = Instant::now();
        for _ in 0..1_000 {
            cache.store(batch, [(questions[0], answer("x"))], &envelope(), t0);
        }
        assert!(cache.lock().queue.len() <= 2 * 2 + 16 + 1);
    }

    #[test]
    fn a_response_made_of_cached_answers_bills_nothing() {
        let body =
            br#"{"model":"jev-1.13.0","answers":{},"usage":{"input_tokens":5,"output_tokens":1}}"#;
        let envelope = Envelope::of(&UpstreamAnswers::parse(body).unwrap());
        assert_eq!(
            envelope.render_free(r#"{"a":{"noul":0.5}}"#),
            r#"{"model":"jev-1.13.0","answers":{"a":{"noul":0.5}},"usage":{"input_tokens":0,"output_tokens":0}}"#
        );
        // No usage from the vendor, none from the cache.
        let body = br#"{"answers":{},"model":"m"}"#;
        let envelope = Envelope::of(&UpstreamAnswers::parse(body).unwrap());
        assert_eq!(envelope.render_free("{}"), r#"{"answers":{},"model":"m"}"#);
    }
}
