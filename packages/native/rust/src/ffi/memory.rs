//! Views of caller memory.
//!
//! An entry point creates each view, unsafely, from one pointer argument
//! under the clause of its contract that covers that pointer. Using a view is
//! then safe. Output views write through raw pointers and never form
//! references, since caller memory may be uninitialized.

use crate::error::BridgeError;
use crate::peer::ReactorEffectPeer;
use std::ptr::{self, NonNull};
use std::slice;

/// Caller memory for one `T`.
pub(super) struct Out<T>(NonNull<T>);

impl<T> Out<T> {
    /// A view of `ptr`, or `None` when it is null.
    ///
    /// # Safety
    /// A non-null `ptr` must be aligned and valid for writes of one `T` until
    /// the entry point returns.
    pub(super) unsafe fn new(ptr: *mut T) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    pub(super) fn write(&mut self, value: T) {
        // SAFETY: `new`'s caller made the location writable until the entry
        // point returns, and views never outlive their entry point.
        unsafe { self.0.as_ptr().write(value) }
    }
}

/// Caller memory for up to `capacity` elements. It may be null when the
/// caller only asks for the sizes of what is queued.
pub(super) struct OutSlice<T> {
    ptr: *mut T,
    capacity: usize,
}

impl<T: Copy> OutSlice<T> {
    /// # Safety
    /// A non-null `ptr` must be aligned and valid for writes of `capacity`
    /// elements until the entry point returns.
    pub(super) unsafe fn new(ptr: *mut T, capacity: usize) -> Self {
        Self { ptr, capacity }
    }

    pub(super) fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// Whether `len` elements fit.
    pub(super) fn holds(&self, len: usize) -> bool {
        len <= self.capacity && (len == 0 || !self.ptr.is_null())
    }

    /// Copy `items` to the start of the buffer.
    ///
    /// # Panics
    /// If the buffer cannot hold them; callers check [`holds`](Self::holds).
    pub(super) fn copy_from(&mut self, items: &[T]) {
        assert!(
            self.holds(items.len()),
            "the caller's buffer cannot hold the item"
        );
        if items.is_empty() {
            return;
        }
        // SAFETY: `holds` shows the buffer is non-null with room for `items`,
        // `new`'s caller made it writable, and caller memory cannot overlap
        // the bridge-owned `items`.
        unsafe { ptr::copy_nonoverlapping(items.as_ptr(), self.ptr, items.len()) }
    }
}

/// Borrow `len` bytes of caller memory. A zero length needs no pointer.
///
/// # Safety
/// When `len` is nonzero and `data` is non-null, `data` must be valid for
/// reads of `len` bytes that nothing writes until the entry point returns.
pub(super) unsafe fn input<'call>(data: *const u8, len: usize) -> Result<&'call [u8], BridgeError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(BridgeError::invalid("null input with a nonzero length"));
    }
    // SAFETY: the caller made `data` valid for reads of `len` bytes, unchanged
    // for the call.
    Ok(unsafe { slice::from_raw_parts(data, len) })
}

/// Borrow the peer behind a handle.
///
/// # Safety
/// `peer` must be null or a handle from `reactor_effect_peer_create` that
/// stays undestroyed until the entry point returns.
pub(super) unsafe fn peer_ref<'call>(
    peer: *mut ReactorEffectPeer,
) -> Result<&'call ReactorEffectPeer, BridgeError> {
    // SAFETY: a non-null `peer` is a live `Box::into_raw` pointer for the
    // call, per the caller.
    unsafe { peer.as_ref() }.ok_or_else(|| BridgeError::invalid("null native peer handle"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_view_holds_what_fits_and_null_holds_only_nothing() {
        let mut buffer = [0u8; 4];
        // SAFETY: `buffer` is writable for 4 bytes for the whole test.
        let view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) };
        assert!(view.holds(0) && view.holds(4) && !view.holds(5));
        // SAFETY: a null view is never written.
        let null = unsafe { OutSlice::<u8>::new(ptr::null_mut(), 8) };
        assert!(null.is_null());
        assert!(null.holds(0) && !null.holds(1));
    }

    #[test]
    fn a_slice_view_copies_to_its_start() {
        let mut buffer = [9u8; 4];
        // SAFETY: `buffer` is writable for 4 bytes for the whole test.
        let mut view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) };
        view.copy_from(&[1, 2]);
        view.copy_from(&[]);
        assert_eq!(buffer, [1, 2, 9, 9]);
    }

    #[test]
    #[should_panic(expected = "cannot hold")]
    fn a_slice_view_refuses_an_item_it_cannot_hold() {
        let mut buffer = [0u8; 1];
        // SAFETY: `buffer` is writable for 1 byte for the whole test.
        let mut view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) };
        view.copy_from(&[1, 2]);
    }

    #[test]
    fn input_needs_a_pointer_only_for_bytes() {
        // SAFETY: a zero length reads nothing.
        let empty = unsafe { input(ptr::null(), 0) };
        assert_eq!(empty, Ok(&[][..]));
        // SAFETY: a null pointer is rejected before any read.
        let null = unsafe { input(ptr::null(), 3) };
        assert_eq!(
            null,
            Err(BridgeError::invalid("null input with a nonzero length"))
        );
        let bytes = [1, 2, 3];
        // SAFETY: `bytes` is readable for its length for the whole test.
        let read = unsafe { input(bytes.as_ptr(), bytes.len()) };
        assert_eq!(read, Ok(&bytes[..]));
    }
}
