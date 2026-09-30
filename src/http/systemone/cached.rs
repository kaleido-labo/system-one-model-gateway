//! Serving a call from the answer cache, in whole or in part, and feeding the
//! cache with what the backend answers.
//!
//! A call whose questions are all cached never reaches the batcher. A call
//! with some of them cached sends only the others upstream, and the cached
//! answers are put back under the caller's own question ids when the reply
//! comes. Nothing here logs a question or an answer.

use axum::http::{HeaderMap, header::CACHE_CONTROL};
use bytes::Bytes;
use serde_json::value::RawValue;
use std::sync::Arc;
use tokio::time::Instant;

use crate::cache::{AnswerCache, Cached, Envelope};
use crate::error::GatewayError;
use crate::metrics::Metrics;
use crate::scheduling::Outcome;
use crate::wire::{BatchKey, ObjectWriter, PreparedRequest, QuestionKey, UpstreamAnswers};

/// Whether the caller asked for a fresh answer with `cache-control: no-cache`.
/// The cache is still written to, so the next caller benefits.
pub(super) fn wants_fresh(headers: &HeaderMap) -> bool {
    headers
        .get_all(CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|directive| {
            let name = directive.split('=').next().unwrap_or_default();
            name.trim().eq_ignore_ascii_case("no-cache")
        })
}

/// One question of the call, as the caller asked it.
struct Slot {
    id: String,
    key: QuestionKey,
    /// The cached answer, when there is a fresh one.
    cached: Option<Cached>,
}

/// What the cache knows about one call.
pub(super) struct Reuse<'a> {
    cache: &'a AnswerCache,
    backend: &'a str,
    batch: BatchKey,
    /// One slot per question, in the caller's order.
    slots: Vec<Slot>,
}

impl<'a> Reuse<'a> {
    /// Looks up every question of `request` (unless `bypass`) and takes the
    /// cached ones out of it, so the request that goes upstream asks only for
    /// the rest.
    pub(super) fn lookup(
        cache: &'a AnswerCache,
        metrics: &Metrics,
        backend: &'a str,
        request: &mut PreparedRequest,
        bypass: bool,
        now: Instant,
    ) -> Self {
        let asked = request.questions.len();
        let found = if bypass {
            (0..asked).map(|_| None).collect()
        } else {
            cache.lookup(request.key, request.questions.iter().map(|q| q.key), now)
        };
        let hits = found.iter().flatten().count() as u64;
        // A bypassed call looks nothing up, so it is neither a hit nor a miss.
        if !bypass {
            let labels = Metrics::backend(backend);
            metrics.cache_hits.get_or_create(&labels).inc_by(hits);
            metrics
                .cache_misses
                .get_or_create(&labels)
                .inc_by(asked as u64 - hits);
        }

        let mut slots = Vec::with_capacity(asked);
        for (question, cached) in std::mem::take(&mut request.questions)
            .into_iter()
            .zip(found)
        {
            slots.push(Slot {
                id: question.id.clone(),
                key: question.key,
                cached: cached.clone(),
            });
            if cached.is_none() {
                request.questions.push(question);
            }
        }
        Self {
            cache,
            backend,
            batch: request.key,
            slots,
        }
    }

    fn hits(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.cached.is_some())
            .count()
    }

    /// Whether every question has a cached answer.
    pub(super) fn is_complete(&self) -> bool {
        self.hits() == self.slots.len()
    }

    /// The value of the `x-systemone-gateway-cache` header.
    pub(super) fn label(&self) -> &'static str {
        match self.hits() {
            0 => "miss",
            hits if hits == self.slots.len() => "hit",
            _ => "partial",
        }
    }

    /// The response body for a call answered from the cache alone. Its usage
    /// is zero: the backend billed nothing for these answers. The layout is
    /// that of the first question's cached response.
    pub(super) fn body(&self) -> Bytes {
        let first = self.slots[0]
            .cached
            .as_ref()
            .expect("a complete lookup has an answer for every question");
        let answers = self
            .encode_answers(|slot| slot.cached.as_ref().map(|cached| cached.answer.get()))
            .expect("a complete lookup has an answer for every question");
        Bytes::from(first.envelope.render_free(&answers))
    }

    /// The `answers` object, under the caller's ids and in the caller's order.
    fn encode_answers<'x>(
        &'x self,
        answer: impl Fn(&'x Slot) -> Option<&'x str>,
    ) -> Result<String, String> {
        let mut answers = ObjectWriter::with_capacity(self.slots.len() * 128);
        for slot in &self.slots {
            let raw =
                answer(slot).ok_or_else(|| format!("no answer for question {:?}", slot.id))?;
            answers.field(&slot.id, raw);
        }
        Ok(answers.finish())
    }

    /// Takes a backend's outcome for the questions that went upstream:
    /// keeps its answers for later and, when some questions were served from
    /// the cache, puts those answers back into the response. Anything that is
    /// not a successful answer is passed on as it is, and never cached.
    pub(super) fn absorb(&self, outcome: Outcome, now: Instant) -> Outcome {
        let Outcome::Answered {
            body,
            request_id,
            input_tokens,
            batch_callers,
        } = outcome
        else {
            return outcome;
        };
        let merged = match self.remember(&body, now) {
            Ok(merged) => merged,
            // Nothing cached to put back: the body goes on as the backend
            // sent it, readable or not.
            Err(_) if self.hits() == 0 => None,
            Err(reason) => {
                return Outcome::Failed(GatewayError::upstream(format!(
                    "backend {:?}'s response could not be read to add the cached answers: {reason}",
                    self.backend
                )));
            }
        };
        Outcome::Answered {
            body: merged.unwrap_or(body),
            request_id,
            input_tokens,
            batch_callers,
        }
    }

    /// Stores the answers the backend gave, and returns the response with
    /// the cached answers merged in. `None` when no question was cached.
    fn remember(&self, body: &Bytes, now: Instant) -> Result<Option<Bytes>, String> {
        let upstream = UpstreamAnswers::parse(body)?;
        let envelope = Arc::new(Envelope::of(&upstream));
        let fresh: Vec<(QuestionKey, Box<RawValue>)> = self
            .slots
            .iter()
            .filter(|slot| slot.cached.is_none())
            .filter_map(|slot| {
                let answer = upstream.answers.get(&slot.id)?;
                // An answer is always an object; anything else is not one.
                answer
                    .get()
                    .trim_start()
                    .starts_with('{')
                    .then(|| (slot.key, answer.clone()))
            })
            .collect();
        self.cache.store(self.batch, fresh, &envelope, now);
        if self.hits() == 0 {
            return Ok(None);
        }

        let answers = self.encode_answers(|slot| match &slot.cached {
            Some(cached) => Some(cached.answer.get()),
            None => upstream.answers.get(&slot.id).map(|raw| raw.get()),
        })?;
        let mut out = ObjectWriter::with_capacity(answers.len() + 128);
        for (name, raw) in &upstream.envelope {
            match name.as_str() {
                "answers" => out.field(name, &answers),
                _ => out.field(name, raw.get()),
            }
        }
        Ok(Some(Bytes::from(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;
    use crate::wire::TokenEstimator;
    use axum::http::HeaderValue;
    use prometheus_client::metrics::gauge::Gauge;
    use serde_json::{Value, json};
    use std::time::Duration;

    fn request(questions: &[&str]) -> PreparedRequest {
        let questions: Vec<String> = questions
            .iter()
            .map(|q| format!(r#""{q}":{{"type":"noul","instructions":"{q}?"}}"#))
            .collect();
        let body = format!(
            r#"{{"state":"s","model":"jev-latest","questions":{{{}}}}}"#,
            questions.join(",")
        );
        PreparedRequest::parse(body.as_bytes(), &TokenEstimator::new(3.0)).unwrap()
    }

    fn cache() -> AnswerCache {
        let config = CacheConfig {
            enabled: true,
            ttl_ms: 60_000,
            max_entries: 100,
        };
        AnswerCache::new(&config, Gauge::default())
    }

    fn answered(body: Value) -> Outcome {
        Outcome::Answered {
            body: Bytes::from(body.to_string()),
            request_id: Some("req_1".to_owned()),
            input_tokens: Some(40),
            batch_callers: 1,
        }
    }

    fn body_of(outcome: Outcome) -> Value {
        match outcome {
            Outcome::Answered { body, .. } => serde_json::from_slice(&body).unwrap(),
            other => panic!("not an answer: {other:?}"),
        }
    }

    #[test]
    fn only_no_cache_asks_for_a_fresh_answer() {
        let headers = |values: &[&'static str]| {
            let mut map = HeaderMap::new();
            for value in values {
                map.append(CACHE_CONTROL, HeaderValue::from_static(value));
            }
            map
        };
        assert!(!wants_fresh(&HeaderMap::new()));
        assert!(wants_fresh(&headers(&["no-cache"])));
        assert!(wants_fresh(&headers(&["max-age=0, No-Cache"])));
        assert!(wants_fresh(&headers(&["private", "no-cache"])));
        assert!(!wants_fresh(&headers(&["max-age=60", "no-cachey"])));
    }

    #[test]
    fn a_partial_hit_sends_only_the_missing_questions_and_merges_the_rest() {
        let cache = cache();
        let metrics = Metrics::new();
        let now = Instant::now();

        // First call: nothing cached, everything goes upstream and is kept.
        let mut first = request(&["urgent", "angry"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut first, false, now);
        assert_eq!(reuse.label(), "miss");
        assert_eq!(first.questions.len(), 2);
        let upstream = json!({
            "model": "jev-1.13.0",
            "answers": {"urgent": {"type": "noul", "noul": 0.9}, "angry": {"type": "noul", "noul": 0.1}},
            "usage": {"input_tokens": 40, "output_tokens": 20},
        });
        let passed = body_of(reuse.absorb(answered(upstream.clone()), now));
        assert_eq!(passed, upstream);

        // Second call: one question is known, one is new.
        let mut second = request(&["angry", "billing"]);
        let later = now + Duration::from_secs(1);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut second, false, later);
        assert_eq!(reuse.label(), "partial");
        assert!(!reuse.is_complete());
        let sent: Vec<_> = second.questions.iter().map(|q| q.id.as_str()).collect();
        assert_eq!(sent, ["billing"]);

        let upstream = json!({
            "model": "jev-1.14.0",
            "answers": {"billing": {"type": "noul", "noul": 0.4}},
            "usage": {"input_tokens": 12, "output_tokens": 10},
        });
        let merged = reuse.absorb(answered(upstream), later);
        let Outcome::Answered { input_tokens, .. } = &merged else {
            panic!("not an answer");
        };
        // The caller is charged for what went upstream, nothing for the rest.
        assert_eq!(*input_tokens, Some(40));
        let merged = body_of(merged);
        assert_eq!(merged["model"], "jev-1.14.0");
        assert_eq!(
            merged["usage"],
            json!({"input_tokens": 12, "output_tokens": 10})
        );
        let ids: Vec<_> = merged["answers"].as_object().unwrap().keys().collect();
        assert_eq!(ids, ["angry", "billing"]);
        assert_eq!(merged["answers"]["angry"]["noul"], 0.1);
        assert_eq!(merged["answers"]["billing"]["noul"], 0.4);

        let text = metrics.render();
        assert!(
            text.contains(r#"systemone_gateway_cache_hits_total{backend="typesafe"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"systemone_gateway_cache_misses_total{backend="typesafe"} 3"#),
            "{text}"
        );
    }

    #[test]
    fn a_full_hit_is_answered_under_the_callers_ids_with_no_billed_tokens() {
        let cache = cache();
        let metrics = Metrics::new();
        let now = Instant::now();
        let mut first = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut first, false, now);
        let upstream = json!({
            "model": "jev-1.13.0",
            "answers": {"urgent": {"type": "noul", "noul": 0.9}},
            "usage": {"input_tokens": 40, "output_tokens": 10},
        });
        reuse.absorb(answered(upstream), now);

        let mut again = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut again, false, now);
        assert!(reuse.is_complete());
        assert_eq!(reuse.label(), "hit");
        assert!(again.questions.is_empty());
        let body: Value = serde_json::from_slice(&reuse.body()).unwrap();
        assert_eq!(
            body,
            json!({
                "model": "jev-1.13.0",
                "answers": {"urgent": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 0, "output_tokens": 0},
            })
        );
    }

    #[test]
    fn a_bypassed_lookup_still_writes_and_counts_nothing() {
        let cache = cache();
        let metrics = Metrics::new();
        let now = Instant::now();
        let mut first = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut first, false, now);
        let upstream = json!({"model": "m", "answers": {"urgent": {"type": "noul", "noul": 0.9}}});
        reuse.absorb(answered(upstream.clone()), now);

        let mut fresh = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut fresh, true, now);
        assert_eq!(reuse.label(), "miss");
        assert_eq!(fresh.questions.len(), 1);
        assert!(
            !metrics
                .render()
                .contains("cache_hits_total{backend=\"typesafe\"} 1")
        );
        // Only the first call's lookup was counted.
        assert!(
            metrics
                .render()
                .contains(r#"systemone_gateway_cache_misses_total{backend="typesafe"} 1"#)
        );
    }

    #[test]
    fn errors_are_passed_on_and_never_cached() {
        let cache = cache();
        let metrics = Metrics::new();
        let now = Instant::now();
        let mut first = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut first, false, now);
        let failed = Outcome::Rejected {
            status: axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            body: Bytes::from_static(br#"{"detail":"no"}"#),
            retry_after: None,
            request_id: None,
        };
        assert!(matches!(
            reuse.absorb(failed, now),
            Outcome::Rejected { .. }
        ));
        // A body that cannot be read is passed on as it is, and kept nowhere.
        let garbage = Outcome::Answered {
            body: Bytes::from_static(b"<html>"),
            request_id: None,
            input_tokens: None,
            batch_callers: 1,
        };
        assert!(matches!(
            reuse.absorb(garbage, now),
            Outcome::Answered { .. }
        ));

        let mut again = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut again, false, now);
        assert_eq!(reuse.label(), "miss");
    }

    #[test]
    fn a_missing_answer_for_a_question_sent_fails_a_partial_call() {
        let cache = cache();
        let metrics = Metrics::new();
        let now = Instant::now();
        let mut first = request(&["urgent"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut first, false, now);
        reuse.absorb(
            answered(json!({"model": "m", "answers": {"urgent": {"type": "noul", "noul": 0.9}}})),
            now,
        );

        let mut second = request(&["urgent", "angry"]);
        let reuse = Reuse::lookup(&cache, &metrics, "typesafe", &mut second, false, now);
        let outcome = reuse.absorb(answered(json!({"model": "m", "answers": {}})), now);
        assert!(matches!(outcome, Outcome::Failed(_)), "{outcome:?}");
    }
}
