//! Views of caller memory.
//!
//! An entry point creates each view, unsafely, from one pointer argument
//! under the clause of its contract that covers that pointer. Using a view is
//! then safe. Output views write through raw pointers and never form
//! references, since caller memory may be uninitialized.
//!
//! A misaligned pointer, or a length over its bound, is refused before any
//! caller memory is touched: no valid caller passes one, and using it would be
//! undefined behavior.

use crate::error::{BridgeError, FailureClass};
use crate::peer::ReactorEffectPeer;
use std::any::type_name;
use std::ptr::{self, NonNull};
use std::slice;

/// Caller memory for one `T`.
pub(super) struct Out<T>(NonNull<T>);

impl<T> Out<T> {
    /// A view of `ptr`, or `None` when it is null.
    ///
    /// # Errors
    /// Invalid input when `ptr` is not aligned for `T`.
    ///
    /// # Safety
    /// A non-null, aligned `ptr` must be valid for writes of one `T` until the
    /// entry point returns.
    pub(super) unsafe fn new(ptr: *mut T) -> Result<Option<Self>, BridgeError> {
        aligned(ptr)?;
        Ok(NonNull::new(ptr).map(Self))
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
    /// A view of `capacity` elements at `ptr`, which may be null.
    ///
    /// # Errors
    /// Invalid input when `ptr` is not aligned for `T`.
    ///
    /// # Safety
    /// A non-null, aligned `ptr` must be valid for writes of `capacity`
    /// elements until the entry point returns.
    pub(super) unsafe fn new(ptr: *mut T, capacity: usize) -> Result<Self, BridgeError> {
        aligned(ptr)?;
        Ok(Self { ptr, capacity })
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
    /// # Errors
    /// A native failure, copying nothing, if the buffer cannot hold them.
    /// Callers rule that out with [`holds`](Self::holds) first.
    pub(super) fn copy_from(&mut self, items: &[T]) -> Result<(), BridgeError> {
        if !self.holds(items.len()) {
            return Err(BridgeError::new(
                FailureClass::Native,
                "the caller's buffer cannot hold the item",
            ));
        }
        if !items.is_empty() {
            // SAFETY: `holds` shows the buffer is non-null with room for
            // `items`, `new`'s caller made it writable, and caller memory
            // cannot overlap the bridge-owned `items`.
            unsafe { ptr::copy_nonoverlapping(items.as_ptr(), self.ptr, items.len()) }
        }
        Ok(())
    }
}

/// Borrow `len` bytes of caller memory, refusing more than `max` before
/// reading any. A zero length needs no pointer.
///
/// # Errors
/// Overflow when `len` exceeds `max`, and invalid input for a null `data`
/// with a nonzero length.
///
/// # Safety
/// When `len` is nonzero and at most `max`, and `data` is non-null, `data`
/// must be valid for reads of `len` bytes that nothing writes until the entry
/// point returns.
pub(super) unsafe fn input<'call>(
    data: *const u8,
    len: usize,
    max: usize,
) -> Result<&'call [u8], BridgeError> {
    if len > max {
        return Err(BridgeError::overflow(format!(
            "input of {len} bytes exceeds its {max}-byte bound"
        )));
    }
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
/// # Errors
/// Invalid input for a null or misaligned handle.
///
/// # Safety
/// An aligned, non-null `peer` must be a handle from
/// `reactor_effect_peer_create` that stays undestroyed until the entry point
/// returns.
pub(super) unsafe fn peer_ref<'call>(
    peer: *mut ReactorEffectPeer,
) -> Result<&'call ReactorEffectPeer, BridgeError> {
    aligned(peer)?;
    // SAFETY: a non-null, aligned `peer` is a live `Box::into_raw` pointer for
    // the call, per the caller.
    unsafe { peer.as_ref() }.ok_or_else(|| BridgeError::invalid("null native peer handle"))
}

/// Refuse a pointer that is not aligned for `T`. A null pointer is aligned.
fn aligned<T>(ptr: *const T) -> Result<(), BridgeError> {
    if ptr.is_aligned() {
        return Ok(());
    }
    let name = type_name::<T>().rsplit("::").next().unwrap_or_default();
    Err(BridgeError::invalid(format!("misaligned {name} pointer")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_view_holds_what_fits_and_null_holds_only_nothing() {
        let mut buffer = [0u8; 4];
        // SAFETY: `buffer` is writable for 4 bytes for the whole test.
        let view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) }.unwrap();
        assert!(view.holds(0) && view.holds(4) && !view.holds(5));
        // SAFETY: a null view is never written.
        let null = unsafe { OutSlice::<u8>::new(ptr::null_mut(), 8) }.unwrap();
        assert!(null.is_null());
        assert!(null.holds(0) && !null.holds(1));
    }

    #[test]
    fn a_slice_view_copies_to_its_start() {
        let mut buffer = [9u8; 4];
        // SAFETY: `buffer` is writable for 4 bytes for the whole test.
        let mut view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) }.unwrap();
        assert_eq!(view.copy_from(&[1, 2]), Ok(()));
        assert_eq!(view.copy_from(&[]), Ok(()));
        assert_eq!(buffer, [1, 2, 9, 9]);
    }

    #[test]
    fn a_slice_view_refuses_an_item_it_cannot_hold_and_copies_nothing() {
        let mut buffer = [0u8; 1];
        // SAFETY: `buffer` is writable for 1 byte for the whole test.
        let mut view = unsafe { OutSlice::new(buffer.as_mut_ptr(), buffer.len()) }.unwrap();
        let error = view.copy_from(&[1, 2]).unwrap_err();
        assert_eq!(error.class, FailureClass::Native);
        // SAFETY: a null view is never written.
        let mut null = unsafe { OutSlice::<u8>::new(ptr::null_mut(), 8) }.unwrap();
        assert_eq!(
            null.copy_from(&[1]).unwrap_err().class,
            FailureClass::Native
        );
        assert_eq!(null.copy_from(&[]), Ok(()));
        assert_eq!(buffer, [0]);
    }

    #[test]
    fn input_needs_a_pointer_only_for_bytes() {
        // SAFETY: a zero length reads nothing.
        let empty = unsafe { input(ptr::null(), 0, 8) };
        assert_eq!(empty, Ok(&[][..]));
        // SAFETY: a null pointer is rejected before any read.
        let null = unsafe { input(ptr::null(), 3, 8) };
        assert_eq!(
            null,
            Err(BridgeError::invalid("null input with a nonzero length"))
        );
        let bytes = [1, 2, 3];
        // SAFETY: `bytes` is readable for its length for the whole test.
        let read = unsafe { input(bytes.as_ptr(), bytes.len(), 3) };
        assert_eq!(read, Ok(&bytes[..]));
    }

    #[test]
    fn input_refuses_a_length_over_its_bound_before_reading() {
        // SAFETY: nothing is readable behind a dangling pointer, and a length
        // over the bound is refused before any read.
        let over = unsafe { input(NonNull::dangling().as_ptr(), 9, 8) };
        assert_eq!(over.unwrap_err().class, FailureClass::Overflow);
    }

    #[test]
    fn views_refuse_misaligned_pointers_and_accept_null() {
        let mut words = [0u32; 2];
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "the test needs a misaligned pointer"
        )]
        let misaligned = words
            .as_mut_ptr()
            .cast::<u8>()
            .wrapping_add(1)
            .cast::<u32>();
        // SAFETY: a misaligned pointer is refused before any write.
        let out = unsafe { Out::new(misaligned) };
        assert_eq!(
            out.err(),
            Some(BridgeError::invalid("misaligned u32 pointer"))
        );
        // SAFETY: as above.
        let slice = unsafe { OutSlice::new(misaligned, 1) };
        assert_eq!(
            slice.err().map(|error| error.class),
            Some(FailureClass::InvalidInput)
        );
        // SAFETY: a null view is never written.
        let null = unsafe { Out::<u32>::new(ptr::null_mut()) };
        assert!(matches!(null, Ok(None)));
        // SAFETY: a misaligned handle is refused before it is dereferenced.
        let handle = unsafe { peer_ref(misaligned.cast()) };
        assert_eq!(
            handle.err().map(|error| error.class),
            Some(FailureClass::InvalidInput)
        );
        assert_eq!(words, [0, 0]);
    }
}
