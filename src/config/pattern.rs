//! Model names and patterns, as written in `models` and `allowed_models`.

use anyhow::{bail, ensure};

/// An exact model name, or a prefix when the text ends with `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelPattern {
    Exact(String),
    Prefix(String),
}

/// How closely a pattern matched a name. A higher value is more specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Specificity {
    Prefix(usize),
    Exact,
}

impl ModelPattern {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        ensure!(
            !text.trim().is_empty(),
            "model names and patterns must not be empty"
        );
        let pattern = match text.strip_suffix('*') {
            Some(prefix) => Self::Prefix(prefix.to_owned()),
            None => Self::Exact(text.to_owned()),
        };
        let inner = match &pattern {
            Self::Exact(name) | Self::Prefix(name) => name,
        };
        if inner.contains('*') {
            bail!("{text:?}: `*` is only allowed at the end, as in \"jev-*\"");
        }
        Ok(pattern)
    }

    pub fn matches(&self, model: &str) -> Option<Specificity> {
        match self {
            Self::Exact(name) => (name == model).then_some(Specificity::Exact),
            Self::Prefix(prefix) => model
                .starts_with(prefix.as_str())
                .then_some(Specificity::Prefix(prefix.len())),
        }
    }

    /// The name itself, for a pattern that names exactly one model.
    pub fn exact(&self) -> Option<&str> {
        match self {
            Self::Exact(name) => Some(name),
            Self::Prefix(_) => None,
        }
    }
}

/// Parses patterns that `Config::validate` has already checked.
pub fn parse_all(texts: &[String]) -> Vec<ModelPattern> {
    texts
        .iter()
        .map(|text| ModelPattern::parse(text).expect("validated with the configuration"))
        .collect()
}

/// The best match of `model` among `patterns`, if any.
pub fn best_match(patterns: &[ModelPattern], model: &str) -> Option<Specificity> {
    patterns.iter().filter_map(|p| p.matches(model)).max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_names_and_prefixes() {
        let exact = ModelPattern::parse("jev-latest").unwrap();
        assert_eq!(exact.matches("jev-latest"), Some(Specificity::Exact));
        assert_eq!(exact.matches("jev-latest2"), None);
        assert_eq!(exact.exact(), Some("jev-latest"));

        let prefix = ModelPattern::parse("Qwen/*").unwrap();
        assert_eq!(
            prefix.matches("Qwen/Qwen2.5-7B-Instruct"),
            Some(Specificity::Prefix(5))
        );
        assert_eq!(prefix.matches("qwen/x"), None);
        assert_eq!(prefix.exact(), None);

        let any = ModelPattern::parse("*").unwrap();
        assert_eq!(any.matches("anything"), Some(Specificity::Prefix(0)));
    }

    #[test]
    fn an_exact_name_beats_the_longest_prefix() {
        assert!(Specificity::Exact > Specificity::Prefix(1_000));
        assert!(Specificity::Prefix(4) > Specificity::Prefix(0));
        let patterns = parse_all(&["*".to_owned(), "jev-*".to_owned()]);
        assert_eq!(
            best_match(&patterns, "jev-latest"),
            Some(Specificity::Prefix(4))
        );
    }

    #[test]
    fn rejects_empty_names_and_inner_stars() {
        assert!(ModelPattern::parse("").is_err());
        assert!(ModelPattern::parse(" ").is_err());
        assert!(ModelPattern::parse("a*b").is_err());
        assert!(ModelPattern::parse("**").is_err());
    }
}
