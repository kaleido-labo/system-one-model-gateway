//! The services allowed to call the gateway, and what each one may use.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::config::{ModelPattern, ServiceConfig, best_match, parse_all};
use crate::scheduling::{Limiter, Limiters};

pub type ServiceId = usize;

pub struct Service {
    pub name: String,
    allowed_models: Option<Vec<ModelPattern>>,
    rate: Option<Limiter>,
    in_flight: Option<Arc<Semaphore>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The service used up its own share of requests per minute.
    RateLimited { retry_after: Duration },
    /// The service already has `max_concurrent` calls in flight.
    TooManyInFlight,
}

/// Proof a call was admitted. Dropping it frees the service's in-flight slot.
#[must_use]
pub struct Admission {
    _in_flight: Option<OwnedSemaphorePermit>,
}

impl Service {
    fn from_config(config: &ServiceConfig, limiters: &Limiters) -> Self {
        Self {
            name: config.name.clone(),
            allowed_models: config
                .allowed_models
                .as_ref()
                .map(|models| parse_all(models)),
            rate: config
                .requests_per_minute
                .zip(config.burst())
                .map(|(rpm, burst)| {
                    limiters.per_minute(
                        &format!("service:{}:requests", config.name),
                        f64::from(rpm),
                        u64::from(burst),
                    )
                }),
            // In flight calls are counted per process, even with a shared
            // rate: a slot is held by a call running in this process.
            in_flight: config.max_concurrent.map(|n| Arc::new(Semaphore::new(n))),
        }
    }

    pub fn allows_model(&self, model: &str) -> bool {
        self.allowed_models
            .as_ref()
            .is_none_or(|models| best_match(models, model).is_some())
    }

    pub async fn admit(&self, now: Instant) -> Result<Admission, Refusal> {
        // Take the in-flight slot first, so a call refused for concurrency
        // does not also spend the service's rate budget.
        let in_flight = match &self.in_flight {
            Some(slots) => Some(
                Arc::clone(slots)
                    .try_acquire_owned()
                    .map_err(|_| Refusal::TooManyInFlight)?,
            ),
            None => None,
        };
        if let Some(rate) = &self.rate {
            rate.try_book(now, 1, Duration::ZERO)
                .await
                .map_err(|retry_after| Refusal::RateLimited { retry_after })?;
        }
        Ok(Admission {
            _in_flight: in_flight,
        })
    }
}

pub struct ServiceRegistry {
    services: Vec<Service>,
    by_key_hash: HashMap<[u8; 32], ServiceId>,
}

impl ServiceRegistry {
    /// Builds the registry from configuration that already passed validation.
    pub fn from_config(configs: &[ServiceConfig], limiters: &Limiters) -> Self {
        let mut services = Vec::with_capacity(configs.len());
        let mut by_key_hash = HashMap::new();
        for (id, config) in configs.iter().enumerate() {
            for hash in &config.key_sha256 {
                let mut digest = [0u8; 32];
                hex::decode_to_slice(hash, &mut digest)
                    .expect("key hashes are checked by Config::validate");
                by_key_hash.insert(digest, id);
            }
            services.push(Service::from_config(config, limiters));
        }
        Self {
            services,
            by_key_hash,
        }
    }

    /// Finds the service whose key is in the `Authorization: Bearer` header.
    ///
    /// Only the key's SHA-256 is looked up, so the registry never holds a
    /// usable key, and a lookup leaks nothing about how close a guess was.
    pub fn authenticate(&self, headers: &HeaderMap) -> Option<ServiceId> {
        let key = bearer_token(headers)?;
        self.by_key_hash.get(&hash_key(key)).copied()
    }

    pub fn get(&self, id: ServiceId) -> &Service {
        &self.services[id]
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.trim().split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

pub fn hash_key(key: &str) -> [u8; 32] {
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&Sha256::digest(key.as_bytes()));
    digest
}

/// A new service key: a recognisable prefix and 256 random bits.
pub fn generate_key() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("the operating system's random source is available");
    format!("s1gw_{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn config(name: &str, key: &str) -> ServiceConfig {
        ServiceConfig {
            name: name.to_owned(),
            key_sha256: vec![hex::encode(hash_key(key))],
            requests_per_minute: None,
            burst: None,
            max_concurrent: None,
            allowed_models: None,
        }
    }

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn authenticates_by_bearer_key() {
        let registry = ServiceRegistry::from_config(
            &[config("a", "key-a"), config("b", "key-b")],
            &Limiters::local(),
        );
        assert_eq!(registry.authenticate(&headers("Bearer key-b")), Some(1));
        assert_eq!(registry.authenticate(&headers("bearer  key-a ")), Some(0));
        assert_eq!(registry.authenticate(&headers("Bearer key-c")), None);
        assert_eq!(registry.authenticate(&headers("Basic key-a")), None);
        assert_eq!(registry.authenticate(&headers("Bearer")), None);
        assert_eq!(registry.authenticate(&HeaderMap::new()), None);
    }

    #[test]
    fn generated_keys_are_distinct_and_prefixed() {
        let a = generate_key();
        let b = generate_key();
        assert!(a.starts_with("s1gw_"));
        assert_eq!(a.len(), 5 + 64);
        assert_ne!(a, b);
    }

    #[test]
    fn allowed_models_restrict_only_when_set() {
        let mut restricted = config("a", "k");
        restricted.allowed_models = Some(vec!["jev-latest".to_owned()]);
        let registry =
            ServiceRegistry::from_config(&[restricted, config("b", "l")], &Limiters::local());
        assert!(registry.get(0).allows_model("jev-latest"));
        assert!(!registry.get(0).allows_model("jev-preview"));
        assert!(registry.get(1).allows_model("anything"));

        let mut family = config("c", "m");
        family.allowed_models = Some(vec!["Qwen/*".to_owned()]);
        let registry = ServiceRegistry::from_config(&[family], &Limiters::local());
        assert!(registry.get(0).allows_model("Qwen/Qwen2.5-7B-Instruct"));
        assert!(!registry.get(0).allows_model("jev-latest"));
    }

    #[tokio::test]
    async fn admission_enforces_the_service_rate() {
        let mut limited = config("a", "k");
        limited.requests_per_minute = Some(60);
        limited.burst = Some(1);
        let registry = ServiceRegistry::from_config(&[limited], &Limiters::local());
        let service = registry.get(0);
        let now = Instant::now();
        let _first = service.admit(now).await.unwrap();
        match service.admit(now).await {
            Err(Refusal::RateLimited { retry_after }) => {
                assert_eq!(retry_after, Duration::from_secs(1));
            }
            other => panic!("expected a rate limit, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn admission_enforces_the_in_flight_cap_until_dropped() {
        let mut capped = config("a", "k");
        capped.max_concurrent = Some(1);
        let registry = ServiceRegistry::from_config(&[capped], &Limiters::local());
        let service = registry.get(0);
        let first = service.admit(Instant::now()).await.unwrap();
        assert_eq!(
            service.admit(Instant::now()).await.err(),
            Some(Refusal::TooManyInFlight)
        );
        drop(first);
        assert!(service.admit(Instant::now()).await.is_ok());
    }
}
