//! Splitting one upstream call's token usage between the callers it served.

/// Splits `total` into integer shares proportional to `weights`, using the
/// largest-remainder method: the shares always add up to `total` exactly, so
/// the per-service token counters reconcile with the vendor's bill.
///
/// Weights that are all zero (or not finite) split `total` evenly.
pub fn apportion(total: u64, weights: &[f64]) -> Vec<u64> {
    if weights.is_empty() {
        return Vec::new();
    }
    let usable = |w: f64| if w.is_finite() && w > 0.0 { w } else { 0.0 };
    let sum: f64 = weights.iter().copied().map(usable).sum();
    let weights: Vec<f64> = if sum > 0.0 {
        weights.iter().copied().map(usable).collect()
    } else {
        vec![1.0; weights.len()]
    };
    let sum: f64 = weights.iter().sum();

    let quotas: Vec<f64> = weights.iter().map(|w| total as f64 * w / sum).collect();
    let mut shares: Vec<u64> = quotas.iter().map(|q| q.floor() as u64).collect();

    // Largest fractional part first; ties go to the earliest caller so the
    // split is deterministic.
    let mut order: Vec<usize> = (0..quotas.len()).collect();
    order.sort_by(|&a, &b| {
        let fa = quotas[a] - quotas[a].floor();
        let fb = quotas[b] - quotas[b].floor();
        fb.total_cmp(&fa).then(a.cmp(&b))
    });

    let assigned: u64 = shares.iter().sum();
    // Hand the units lost to rounding to the largest fractional parts.
    let mut missing = total.saturating_sub(assigned);
    for &index in order.iter().cycle() {
        if missing == 0 {
            break;
        }
        shares[index] += 1;
        missing -= 1;
    }
    // Floating-point error can round a quota up past its true value; take the
    // surplus back from the smallest fractional parts.
    let mut surplus = assigned.saturating_sub(total);
    for &index in order.iter().rev().cycle() {
        if surplus == 0 {
            break;
        }
        if shares[index] > 0 {
            shares[index] -= 1;
            surplus -= 1;
        }
    }
    shares
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_follow_the_weights_and_add_up() {
        assert_eq!(apportion(100, &[1.0, 1.0, 2.0]), vec![25, 25, 50]);
        assert_eq!(apportion(10, &[1.0, 1.0, 1.0]), vec![4, 3, 3]);
        let shares = apportion(1_000_003, &[0.3, 0.3, 0.4]);
        assert_eq!(shares.iter().sum::<u64>(), 1_000_003);
    }

    #[test]
    fn zero_or_invalid_weights_split_evenly() {
        assert_eq!(apportion(9, &[0.0, 0.0, 0.0]), vec![3, 3, 3]);
        assert_eq!(apportion(2, &[f64::NAN, -1.0]), vec![1, 1]);
    }

    #[test]
    fn a_zero_weight_gets_nothing_when_others_have_weight() {
        assert_eq!(apportion(7, &[0.0, 1.0]), vec![0, 7]);
    }

    #[test]
    fn edge_cases() {
        assert!(apportion(5, &[]).is_empty());
        assert_eq!(apportion(0, &[1.0, 2.0]), vec![0, 0]);
        assert_eq!(apportion(1, &[1.0, 1.0]), vec![1, 0]);
    }
}
