#![allow(dead_code)]
/// Vendored from `crdts` crate.
/// License: Apache-2.0 https://github.com/rust-crdt/rust-crdt/blob/master/LICENSE
use std::{error, fmt};

/// `LWWReg` is a simple CRDT that contains an arbitrary value
/// along with an `Ord` that tracks causality. It is the responsibility
/// of the user to guarantee that the source of the causal element
/// is monotonic. Don't use timestamps unless you are comfortable
/// with divergence.
///
/// `M` is a marker. It must grow monotonically *and* must be globally unique
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LWWReg<V, M> {
    /// `val` is the opaque element contained within this CRDT
    pub val: V,
    /// `marker` should be a monotonic value associated with this val
    pub marker: M,
}

impl<V: Default, M: Default> Default for LWWReg<V, M> {
    fn default() -> Self {
        Self {
            val: V::default(),
            marker: M::default(),
        }
    }
}

/// The Type of validation errors that may occur for an LWWReg.
#[derive(Debug, PartialEq)]
pub enum Validation {
    /// A conflicting change to a CRDT is witnessed by a dot that already exists.
    ConflictingMarker,
}

impl error::Error for Validation {
    fn description(&self) -> &str {
        match self {
            Validation::ConflictingMarker => {
                "A marker must be used exactly once, re-using the same marker breaks associativity"
            }
        }
    }
}

impl fmt::Display for Validation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<V: PartialEq, M: Ord> LWWReg<V, M> {
    /// Construct a new LwwReg initialized with the given value and marker
    pub fn new(val: V, marker: M) -> Self {
        LWWReg { val, marker }
    }

    /// Updates value witnessed by the given marker.
    pub fn update(&mut self, val: V, marker: M) {
        if self.marker < marker {
            self.val = val;
            self.marker = marker;
        }
    }

    /// An update is invalid if the marker is exactly the same as
    pub fn validate_update(&self, val: &V, marker: &M) -> Result<(), Validation> {
        if &self.marker == marker && val != &self.val {
            Err(Validation::ConflictingMarker)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default() {
        let reg = LWWReg::default();
        assert_eq!(reg, LWWReg { val: "", marker: 0 });
    }

    #[test]
    fn test_update() {
        let mut reg = LWWReg {
            val: 123,
            marker: 0,
        };

        // normal update: new marker is a descended of current marker
        // EXPECTED: success, the val and marker are update
        reg.update(32, 2);
        assert_eq!(reg, LWWReg { val: 32, marker: 2 });

        // stale update: new marker is an ancester of the current marker
        // EXPECTED: succes, no-op
        reg.update(57, 1);
        assert_eq!(reg, LWWReg { val: 32, marker: 2 });

        // redundant update: new marker and val is same as of the current state
        // EXPECTED: success, no-op
        reg.update(32, 2);
        assert_eq!(reg, LWWReg { val: 32, marker: 2 });

        // bad update: new marker same as of the current marker but not value
        // EXPECTED: error
        assert_eq!(
            reg.validate_update(&4000, &2),
            Err(Validation::ConflictingMarker)
        );

        // Applying the update despite the validation error is a no-op
        reg.update(4000, 2);
        assert_eq!(reg, LWWReg { val: 32, marker: 2 });
    }
}
