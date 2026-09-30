//! Token estimates for payloads the gateway has not sent yet.
//!
//! TypeSafe does not publish Jev's tokenizer, so the gateway guesses from byte
//! length. The guess drives three decisions: whether a merged call still fits
//! the context window, how many tokens to book against the per-second limit,
//! and how to split a merged call's `usage` between callers. The first two
//! want an overestimate, hence the conservative default of 3 bytes per token
//! (JSON punctuation and French accents both tokenize worse than English
//! prose). The third only uses ratios, so the constant cancels out there.

/// Fixed cost counted for each question on top of its text: the question
/// type, its id and the framing the vendor adds around it.
pub const QUESTION_OVERHEAD: u32 = 8;

#[derive(Debug, Clone, Copy)]
pub struct TokenEstimator {
    bytes_per_token: f64,
}

impl TokenEstimator {
    pub fn new(bytes_per_token: f64) -> Self {
        assert!(bytes_per_token > 0.0, "bytes_per_token must be positive");
        Self { bytes_per_token }
    }

    /// Estimated tokens for `len` bytes of minified JSON text.
    pub fn estimate(&self, len: usize) -> u32 {
        let tokens = (len as f64 / self.bytes_per_token).ceil();
        // A state is capped far below u32::MAX by the body size limit.
        tokens.min(u32::MAX as f64) as u32
    }

    /// Estimated tokens for one question, including its fixed overhead.
    pub fn question(&self, len: usize) -> u32 {
        self.estimate(len).saturating_add(QUESTION_OVERHEAD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_rounds_up() {
        let estimator = TokenEstimator::new(3.0);
        assert_eq!(estimator.estimate(0), 0);
        assert_eq!(estimator.estimate(1), 1);
        assert_eq!(estimator.estimate(3), 1);
        assert_eq!(estimator.estimate(4), 2);
    }

    #[test]
    fn question_adds_the_fixed_overhead() {
        let estimator = TokenEstimator::new(4.0);
        assert_eq!(estimator.question(40), 10 + QUESTION_OVERHEAD);
    }
}
