// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Computed fragment placement by rendezvous (highest-random-weight) hashing.
//!
//! Placement is **derived, not stored**: any party that knows the object identity —
//! the writer choosing where to put a fragment, a reader resolving where to fetch it,
//! or a rebuilder picking a replacement — computes the same answer from the same target
//! roster with no manifest and no lookup. This is the placement counterpart to the
//! computed chunk id in [`kp2::ChunkId::for_fragment`]: identity in, location out.
//!
//! For each `(stripe)` the candidate targets are scored by a SHA-256 rendezvous weight
//! over `(cluster_salt, object_id, object_version, stripe_index, target_id)`. The whole
//! stripe shares one ranking — every fragment of a stripe is placed against the same
//! sorted candidate list — and fragment index `i` takes the `i`-th candidate that lands
//! in a failure domain not yet used by the stripe. Walking the descending-score list and
//! taking only first-seen domains spreads the `N = data + parity` fragments across `N`
//! distinct failure domains.
//!
//! The defining property of rendezvous hashing is **minimal reshuffle**: each target's
//! score depends only on that target's id (and the stripe identity), so removing or
//! adding one target moves only the fragments that were on (or would now outrank at) the
//! changed target. Fragments whose chosen targets are untouched keep their placement.
//!
//! The roster a caller passes need not be filtered: unhealthy targets, targets outside
//! an active lifecycle state, and ids in the `excluded` set are dropped before scoring.
//! The `excluded` set is how a rebuilder asks for a replacement that avoids both the
//! failed target and the still-live fragments of the stripe.
//!
//! This module is pure: no I/O, no async, no clock. Same inputs, same output, forever.

use crate::proto::{FailureDomain, TargetLifecycleState, TargetRecord};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fmt;

/// Domain-separation prefix for the rendezvous score derivation. Bump the trailing
/// version if the score derivation ever changes, so weights from different schemes
/// cannot be compared against each other.
const PLACEMENT_DOMAIN: &[u8] = b"keinfs/placement/hrw/v1";

/// Domain-separation prefix for the topology-epoch derivation.
const TOPOLOGY_EPOCH_DOMAIN: &[u8] = b"keinfs/topology-epoch/v1";

/// A target offered to the placement function.
///
/// Built from a [`TargetRecord`] via [`PlacementTarget::from_record`]; the fields here
/// are exactly what placement reads. `usable` collapses the lifecycle gate to a single
/// boolean (an active target that can hold a fragment), so the scorer never needs the
/// raw lifecycle enum. The failure-domain grouping is read from the appropriate record
/// field at scoring time per the requested [`FailureDomain`], mirroring how KAS keys
/// failure domains in `poc/kas/src/fdb_store.rs` (`failure_domain_key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementTarget {
    pub target_id: String,
    pub server_id: String,
    pub rack_id: String,
    pub healthy: bool,
    pub usable: bool,
}

impl PlacementTarget {
    /// Derive a placement candidate from a control-plane target record.
    ///
    /// `usable` is true only when the record's lifecycle state is `ACTIVE`; a draining,
    /// unhealthy, retired, or unspecified target is never a placement target. This is the
    /// same lifecycle gate KAS applies before reserving a granule
    /// (`lifecycle_state == TargetLifecycleState::Active`).
    pub fn from_record(record: &TargetRecord) -> Self {
        Self {
            target_id: record.target_id.clone(),
            server_id: record.server_id.clone(),
            rack_id: record.rack_id.clone(),
            healthy: record.healthy,
            usable: record.lifecycle_state == TargetLifecycleState::Active as i32,
        }
    }

    fn accepts_fragments(&self) -> bool {
        self.healthy && self.usable
    }

    /// The failure-domain key for this target under the requested domain.
    ///
    /// This mirrors `failure_domain_key` in `poc/kas/src/fdb_store.rs` so the two sides
    /// group targets identically: drive-domain keys on `target_id`, node keys on
    /// `server_id`, rack keys on `rack_id`. An unspecified domain has no grouping rule.
    fn failure_domain_key(&self, failure_domain: FailureDomain) -> Result<String, PlacementError> {
        match failure_domain {
            FailureDomain::DriveDomainLab => Ok(format!("target:{}", self.target_id)),
            FailureDomain::Node => Ok(format!("node:{}", self.server_id)),
            FailureDomain::Rack => Ok(format!("rack:{}", self.rack_id)),
            FailureDomain::Unspecified => Err(PlacementError::UnspecifiedFailureDomain),
        }
    }
}

/// Why placement could not produce a full stripe layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementError {
    /// The requested failure domain is `UNSPECIFIED`, which has no grouping rule.
    UnspecifiedFailureDomain,
    /// A stripe of `requested` fragments needs `requested` distinct failure domains, but
    /// only `available` usable domains exist in the roster after filtering.
    NotEnoughFailureDomains { requested: usize, available: usize },
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlacementError::UnspecifiedFailureDomain => {
                f.write_str("failure domain must be specified")
            }
            PlacementError::NotEnoughFailureDomains {
                requested,
                available,
            } => write!(
                f,
                "stripe needs {requested} distinct failure domains, but only {available} are usable"
            ),
        }
    }
}

impl std::error::Error for PlacementError {}

/// Rendezvous weight for `(stripe, target)`, scoped to a cluster by `cluster_salt`.
///
/// The score is per-`(stripe, target)`, NOT per-`(fragment, target)`: every fragment of
/// a stripe is ranked against the same scores. Inputs are hashed domain-separated, with a
/// length-prefixed salt and fixed-width little-endian scalars, then the target id as a
/// length-prefixed byte string — the same unambiguous framing used by
/// [`kp2::ChunkId::for_fragment`]. The leading 8 bytes of the digest become a `u64`
/// weight; ties (astronomically unlikely with SHA-256) break by target id at ranking
/// time, so the order is total and deterministic.
fn rendezvous_score(
    cluster_salt: &[u8],
    object_id: u32,
    object_version: u16,
    stripe_index: u32,
    target_id: &str,
) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(PLACEMENT_DOMAIN);
    hasher.update((cluster_salt.len() as u32).to_le_bytes());
    hasher.update(cluster_salt);
    hasher.update(object_id.to_le_bytes());
    hasher.update(object_version.to_le_bytes());
    hasher.update(stripe_index.to_le_bytes());
    hasher.update((target_id.len() as u32).to_le_bytes());
    hasher.update(target_id.as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("sha-256 digest is 32 bytes"))
}

/// Targets that can hold a fragment, ranked by descending rendezvous score for a stripe.
///
/// Unhealthy targets, targets not in an active lifecycle state, and ids in `excluded` are
/// dropped before scoring. The result is a total order: descending score, ties broken by
/// target id. This is the raw ranking that both [`place_stripe`] and
/// [`ranked_candidates_for_stripe`] build on.
fn scored_targets<'a>(
    cluster_salt: &[u8],
    object_id: u32,
    object_version: u16,
    stripe_index: u32,
    targets: &'a [PlacementTarget],
    excluded: &HashSet<String>,
) -> Vec<&'a PlacementTarget> {
    let mut scored: Vec<(u64, &PlacementTarget)> = targets
        .iter()
        .filter(|target| target.accepts_fragments())
        .filter(|target| !excluded.contains(&target.target_id))
        .map(|target| {
            let score = rendezvous_score(
                cluster_salt,
                object_id,
                object_version,
                stripe_index,
                &target.target_id,
            );
            (score, target)
        })
        .collect();
    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.target_id.cmp(&right.1.target_id))
    });
    scored.into_iter().map(|(_, target)| target).collect()
}

/// Choose the target that holds each fragment of one stripe.
///
/// Returns one `target_id` per fragment position, length `fragments_per_stripe`, where the
/// `i`-th entry holds fragment index `i`. The chosen targets are guaranteed to lie in
/// `fragments_per_stripe` distinct failure domains: candidates are walked in descending
/// rendezvous score and a target is taken only if its failure domain has not already been
/// used by this stripe.
///
/// Errors if `failure_domain` is `UNSPECIFIED`, or if fewer than `fragments_per_stripe`
/// distinct usable failure domains exist after filtering — placement never doubles two
/// fragments of a stripe into one failure domain.
#[allow(clippy::too_many_arguments)]
pub fn place_stripe(
    cluster_salt: &[u8],
    object_id: u32,
    object_version: u16,
    stripe_index: u32,
    fragments_per_stripe: usize,
    failure_domain: FailureDomain,
    targets: &[PlacementTarget],
    excluded: &HashSet<String>,
) -> Result<Vec<String>, PlacementError> {
    let chosen = pick_distinct_domains(
        cluster_salt,
        object_id,
        object_version,
        stripe_index,
        Some(fragments_per_stripe),
        failure_domain,
        targets,
        excluded,
    )?;
    if chosen.len() < fragments_per_stripe {
        return Err(PlacementError::NotEnoughFailureDomains {
            requested: fragments_per_stripe,
            available: chosen.len(),
        });
    }
    Ok(chosen)
}

/// Every usable target for a stripe, ranked by descending rendezvous score and filtered
/// to one target per failure domain (the first, i.e. highest-scoring, seen for each
/// domain).
///
/// The prefix of length `data + parity` is exactly what [`place_stripe`] would return; the
/// tail is the ordered fallback a rebuilder walks when a primary placement is unavailable.
/// Fragment position `i` should prefer `result[i]`, then `result[i + 1]`, and so on —
/// each successive candidate is in a distinct failure domain, so a fallback never collides
/// with a live fragment's domain.
///
/// Errors only if `failure_domain` is `UNSPECIFIED`.
pub fn ranked_candidates_for_stripe(
    cluster_salt: &[u8],
    object_id: u32,
    object_version: u16,
    stripe_index: u32,
    failure_domain: FailureDomain,
    targets: &[PlacementTarget],
    excluded: &HashSet<String>,
) -> Result<Vec<String>, PlacementError> {
    pick_distinct_domains(
        cluster_salt,
        object_id,
        object_version,
        stripe_index,
        None,
        failure_domain,
        targets,
        excluded,
    )
}

/// Walk the scored candidates, taking the first target seen in each failure domain.
///
/// With `limit = Some(n)` it stops once `n` distinct domains are chosen (the stripe-sized
/// answer); with `limit = None` it ranks every usable distinct domain (the fallback list).
#[allow(clippy::too_many_arguments)]
fn pick_distinct_domains(
    cluster_salt: &[u8],
    object_id: u32,
    object_version: u16,
    stripe_index: u32,
    limit: Option<usize>,
    failure_domain: FailureDomain,
    targets: &[PlacementTarget],
    excluded: &HashSet<String>,
) -> Result<Vec<String>, PlacementError> {
    // Reject an unspecified domain before scoring so the error is the same whether or not
    // the roster is empty.
    if failure_domain == FailureDomain::Unspecified {
        return Err(PlacementError::UnspecifiedFailureDomain);
    }
    let ranked = scored_targets(
        cluster_salt,
        object_id,
        object_version,
        stripe_index,
        targets,
        excluded,
    );
    let mut used_domains: HashSet<String> = HashSet::new();
    let mut chosen: Vec<String> = Vec::new();
    for target in ranked {
        if let Some(limit) = limit {
            if chosen.len() == limit {
                break;
            }
        }
        let domain_key = target.failure_domain_key(failure_domain)?;
        if used_domains.insert(domain_key) {
            chosen.push(target.target_id.clone());
        }
    }
    Ok(chosen)
}

/// A content-derived identifier for the placement-relevant topology.
///
/// It changes exactly when the set of targets, their failure-domain attributes
/// (`server_id`/`rack_id`), or their usability (`healthy`/active) change, and it is
/// identical on every node that observes the same roster — so independent KAS instances
/// agree on it without any coordination. Routine heartbeats, which carry no membership
/// or health change, leave it unchanged, so it does not churn.
///
/// It is a content hash, NOT a monotonic counter: equality means "the same placement
/// topology", which is all a staleness or rebalance check needs (an object whose head
/// records a different epoch than the live one may need its placement re-evaluated). The
/// roster order does not matter — the rows are sorted by target id before hashing.
pub fn topology_epoch(targets: &[PlacementTarget]) -> u64 {
    let mut rows: Vec<&PlacementTarget> = targets.iter().collect();
    rows.sort_by(|left, right| left.target_id.cmp(&right.target_id));
    let mut hasher = Sha256::new();
    hasher.update(TOPOLOGY_EPOCH_DOMAIN);
    hasher.update((rows.len() as u32).to_le_bytes());
    for target in rows {
        for field in [&target.target_id, &target.server_id, &target.rack_id] {
            hasher.update((field.len() as u32).to_le_bytes());
            hasher.update(field.as_bytes());
        }
        hasher.update([target.healthy as u8, target.usable as u8]);
    }
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("sha-256 digest is 32 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SALT: &[u8] = b"cluster-salt-alpha";

    fn target(id: &str, server: &str, rack: &str) -> PlacementTarget {
        PlacementTarget {
            target_id: id.to_string(),
            server_id: server.to_string(),
            rack_id: rack.to_string(),
            healthy: true,
            usable: true,
        }
    }

    /// A roster with one target per node so the node domain has as many domains as targets.
    fn roster(count: usize) -> Vec<PlacementTarget> {
        (0..count)
            .map(|index| {
                target(
                    &format!("target-{index:02}"),
                    &format!("node-{index:02}"),
                    &format!("rack-{}", index % 3),
                )
            })
            .collect()
    }

    fn no_exclusions() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn placement_is_deterministic_across_calls() {
        let targets = roster(12);
        let first = place_stripe(
            SALT,
            42,
            1,
            7,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        let second = place_stripe(
            SALT,
            42,
            1,
            7,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        assert_eq!(first, second, "same inputs must give identical placement");
        assert_eq!(first.len(), 10, "one target per fragment");
    }

    #[test]
    fn stripe_fragments_land_in_distinct_failure_domains() {
        let targets = roster(12);
        let placed = place_stripe(
            SALT,
            100,
            0,
            3,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        let domains: HashSet<String> = placed
            .iter()
            .map(|id| {
                let t = targets.iter().find(|t| &t.target_id == id).unwrap();
                t.failure_domain_key(FailureDomain::Node).unwrap()
            })
            .collect();
        assert_eq!(
            domains.len(),
            placed.len(),
            "every fragment must be in its own failure domain"
        );
    }

    #[test]
    fn removing_one_target_moves_only_its_fragments() {
        // The defining rendezvous property: drop one chosen target from the roster and the
        // chosen set changes by exactly one element — the dropped target leaves and a single
        // new target (the next-ranked distinct domain) takes its place. Every other fragment
        // keeps the same target. With one target per failure domain the chosen set is the
        // set of fragment holders, so this is precisely "only the fragments on the dropped
        // target move". (Position order may compact, since fragment positions are filled in
        // rank order, but no surviving target gains or loses a fragment.)
        let targets = roster(16);
        let before = place_stripe(
            SALT,
            555,
            2,
            1,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");

        // Pick a target the stripe actually used and remove it from the roster entirely.
        let dropped = before[4].clone();
        let reduced: Vec<PlacementTarget> = targets
            .iter()
            .filter(|t| t.target_id != dropped)
            .cloned()
            .collect();
        let after = place_stripe(
            SALT,
            555,
            2,
            1,
            10,
            FailureDomain::Node,
            &reduced,
            &no_exclusions(),
        )
        .expect("placement still succeeds");

        assert_eq!(before.len(), after.len());
        assert!(!after.contains(&dropped), "dropped target is not reused");

        let before_set: HashSet<&String> = before.iter().collect();
        let after_set: HashSet<&String> = after.iter().collect();
        // Exactly one target left the set (the dropped one) and exactly one joined it.
        let removed: Vec<&&String> = before_set.difference(&after_set).collect();
        let added: Vec<&&String> = after_set.difference(&before_set).collect();
        assert_eq!(removed, vec![&&dropped], "only the dropped target leaves the set");
        assert_eq!(added.len(), 1, "exactly one new target joins the set");
        // The kept targets are exactly the survivors of the original placement: removing one
        // target reshuffles nothing among the fragments that were not on it.
        let kept: HashSet<&String> = before_set.intersection(&after_set).copied().collect();
        assert_eq!(
            kept.len(),
            before.len() - 1,
            "all but the dropped fragment keep their target"
        );
    }

    #[test]
    fn excluded_target_is_never_chosen() {
        let targets = roster(12);
        let unconstrained = place_stripe(
            SALT,
            7,
            0,
            0,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        // Exclude a target that the unconstrained placement actually used.
        let banned = unconstrained[2].clone();
        let mut excluded = HashSet::new();
        excluded.insert(banned.clone());
        let constrained = place_stripe(
            SALT,
            7,
            0,
            0,
            10,
            FailureDomain::Node,
            &targets,
            &excluded,
        )
        .expect("placement succeeds with one excluded");
        assert!(
            !constrained.contains(&banned),
            "excluded target must never be placed"
        );
        assert_eq!(constrained.len(), 10);
    }

    #[test]
    fn different_salt_changes_placement() {
        let targets = roster(12);
        let placed_a = place_stripe(
            b"cluster-salt-alpha",
            314,
            0,
            5,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        let placed_b = place_stripe(
            b"cluster-salt-beta",
            314,
            0,
            5,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        assert_ne!(
            placed_a, placed_b,
            "a different cluster salt must scope to a different placement"
        );
    }

    #[test]
    fn too_few_domains_is_an_error_not_a_collision() {
        // Twelve targets but only three racks: a 10-fragment stripe under the rack domain
        // cannot be placed in 10 distinct domains, so it must error rather than double up.
        let targets = roster(12);
        let result = place_stripe(
            SALT,
            1,
            0,
            0,
            10,
            FailureDomain::Rack,
            &targets,
            &no_exclusions(),
        );
        assert_eq!(
            result,
            Err(PlacementError::NotEnoughFailureDomains {
                requested: 10,
                available: 3,
            })
        );
    }

    #[test]
    fn unspecified_failure_domain_is_rejected() {
        let targets = roster(12);
        let result = place_stripe(
            SALT,
            1,
            0,
            0,
            10,
            FailureDomain::Unspecified,
            &targets,
            &no_exclusions(),
        );
        assert_eq!(result, Err(PlacementError::UnspecifiedFailureDomain));
    }

    #[test]
    fn unhealthy_and_inactive_targets_are_skipped() {
        let mut targets = roster(12);
        targets[0].healthy = false;
        targets[1].usable = false;
        let placed = place_stripe(
            SALT,
            9,
            0,
            0,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("ten healthy active targets remain");
        assert!(!placed.contains(&targets[0].target_id));
        assert!(!placed.contains(&targets[1].target_id));
    }

    #[test]
    fn ranked_candidates_extends_the_stripe_with_fallbacks() {
        let targets = roster(16);
        let stripe = place_stripe(
            SALT,
            21,
            0,
            2,
            10,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("placement succeeds");
        let ranked = ranked_candidates_for_stripe(
            SALT,
            21,
            0,
            2,
            FailureDomain::Node,
            &targets,
            &no_exclusions(),
        )
        .expect("ranking succeeds");
        // The stripe-sized prefix of the ranked list is exactly the chosen placement.
        assert_eq!(&ranked[..stripe.len()], stripe.as_slice());
        // The tail provides distinct-domain fallbacks for the rebuilder.
        assert!(ranked.len() > stripe.len(), "fallback candidates are offered");
        let domains: HashSet<String> = ranked
            .iter()
            .map(|id| {
                let t = targets.iter().find(|t| &t.target_id == id).unwrap();
                t.failure_domain_key(FailureDomain::Node).unwrap()
            })
            .collect();
        assert_eq!(
            domains.len(),
            ranked.len(),
            "ranked candidates are one-per-failure-domain"
        );
    }

    #[test]
    fn from_record_maps_lifecycle_to_usable() {
        let mut record = TargetRecord {
            target_id: "t".into(),
            server_id: "s".into(),
            rack_id: "r".into(),
            healthy: true,
            lifecycle_state: TargetLifecycleState::Active as i32,
            ..Default::default()
        };
        assert!(PlacementTarget::from_record(&record).usable);
        record.lifecycle_state = TargetLifecycleState::Draining as i32;
        assert!(!PlacementTarget::from_record(&record).usable);
    }

    #[test]
    fn topology_epoch_is_order_independent_and_change_sensitive() {
        let targets = roster(6);
        let epoch = topology_epoch(&targets);

        // Reordering the roster must not change the epoch.
        let mut shuffled = targets.clone();
        shuffled.reverse();
        assert_eq!(epoch, topology_epoch(&shuffled), "epoch is order-independent");

        // Flipping a target's health changes the epoch.
        let mut unhealthy = targets.clone();
        unhealthy[2].healthy = false;
        assert_ne!(epoch, topology_epoch(&unhealthy), "health flip changes the epoch");

        // Removing a target changes the epoch.
        let removed = &targets[..targets.len() - 1];
        assert_ne!(epoch, topology_epoch(removed), "membership change moves the epoch");

        // A lifecycle (usable) change changes the epoch.
        let mut draining = targets.clone();
        draining[0].usable = false;
        assert_ne!(epoch, topology_epoch(&draining), "lifecycle change moves the epoch");
    }

    #[test]
    fn failure_domain_key_matches_kas_semantics() {
        // Mirrors poc/kas/src/fdb_store.rs failure_domain_key: drive keys on target_id,
        // node on server_id, rack on rack_id.
        let t = target("target-x", "node-y", "rack-z");
        assert_eq!(
            t.failure_domain_key(FailureDomain::DriveDomainLab).unwrap(),
            "target:target-x"
        );
        assert_eq!(
            t.failure_domain_key(FailureDomain::Node).unwrap(),
            "node:node-y"
        );
        assert_eq!(
            t.failure_domain_key(FailureDomain::Rack).unwrap(),
            "rack:rack-z"
        );
    }
}
