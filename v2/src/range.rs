use crate::value::UserKey;
use std::ops::Bound;

#[must_use]
#[allow(clippy::module_name_repititions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    let mut end = prefix.to_vec();

    for i in (0..end.len()).rev() {
        let byte = end.get_mut(i).expect("Should be in bounds");

        if *byte < 255 {
            *byte += 1;
            end.truncate(i + 1);

            return (Included(prefix.into()), Excluded(end.into()));
        }
    }

    (Included(prefix.into()), Unbounded)
}
