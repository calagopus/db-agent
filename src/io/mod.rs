use std::ops::{Bound, RangeBounds};

pub mod compression;
pub mod line_buffer;
pub mod tail;

fn resolve_range(range: impl RangeBounds<usize>, len: usize) -> std::io::Result<(usize, usize)> {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_add(1),
        Bound::Unbounded => 0,
    };

    let end = match range.end_bound() {
        Bound::Included(&n) => n.saturating_add(1),
        Bound::Excluded(&n) => n,
        Bound::Unbounded => len,
    };

    if crate::unlikely(start > end) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range start exceeds range end",
        ));
    }

    if crate::unlikely(end > len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range end exceeds slice length",
        ));
    }

    Ok((start, end))
}

pub trait SafeSliceExt<T>: AsRef<[T]> {
    fn get_slice(&self, range: impl RangeBounds<usize>) -> std::io::Result<&[T]> {
        let slice = self.as_ref();
        let (start, end) = resolve_range(range, slice.len())?;

        // SAFETY: resolve_range guarantees start <= end <= slice.len()
        Ok(unsafe { slice.get_unchecked(start..end) })
    }
}
impl<T, Tr: AsRef<[T]> + ?Sized> SafeSliceExt<T> for Tr {}

pub trait SafeSliceMutExt<T>: AsMut<[T]> {
    fn get_slice_mut(&mut self, range: impl RangeBounds<usize>) -> std::io::Result<&mut [T]> {
        let slice = self.as_mut();
        let (start, end) = resolve_range(range, slice.len())?;

        // SAFETY: resolve_range guarantees start <= end <= slice.len()
        Ok(unsafe { slice.get_unchecked_mut(start..end) })
    }
}
impl<T, Tr: AsMut<[T]> + ?Sized> SafeSliceMutExt<T> for Tr {}
