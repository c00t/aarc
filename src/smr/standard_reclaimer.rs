use crate::smr::drc::{Protect, ProtectPtr, Retire};
use crate::utils::unrolled_linked_list::UnrolledLinkedList;
use crate::utils::unsafe_arc::UnsafeArc;
use std::cell::RefCell;
use std::collections::HashSet;
use std::mem;
use std::ptr::null_mut;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};
use std::sync::OnceLock;

const SLOTS_PER_NODE: usize = 32;

/// The default memory reclamation strategy.
pub struct StandardReclaimer;

mod standard_reclaimer_static {
    use std::cell::RefCell;
    use crate::smr::standard_reclaimer::SlotHandle;

    bubble_core::thread_local! {
        pub(super) static SLOT_HANDLE: RefCell<SlotHandle> = RefCell::default();
    }
}

impl StandardReclaimer {
    
    /// # Safety
    /// TODO: write docs for this and make it pub
    #[allow(dead_code)]
    pub(crate) unsafe fn cleanup() {
        for slot in Self::get_all_slots().iter(SeqCst) {
            drop(slot.tlocal_batch.take());
            slot.primary_list.detach_head();
            for snapshot_ptr in slot.snapshots.iter(SeqCst) {
                snapshot_ptr.conflicts.detach_head();
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn cleanup_owned_slot() {
        let handle = standard_reclaimer_static::SLOT_HANDLE.with_borrow(|h| h.0);
        if let Some(slot) = handle {
            drop(slot.tlocal_batch.take());
            slot.primary_list.detach_head();
            for snapshot_ptr in slot.snapshots.iter(SeqCst) {
                snapshot_ptr.conflicts.detach_head();
            }
        }
    }
    
    /// Get slots of all threads.
    fn get_all_slots() -> &'static UnrolledLinkedList<Slot, SLOTS_PER_NODE> {
        // It's not so huge for a PC
        bubble_core::lazy_static! {
            static ref SLOTS: OnceLock<UnrolledLinkedList<Slot, SLOTS_PER_NODE>> = OnceLock::new();
        }
        SLOTS.get_or_init(UnrolledLinkedList::default)
    }
    
    /// Get slot of current thread.
    fn get_or_claim_slot() -> &'static Slot {
        standard_reclaimer_static::SLOT_HANDLE.with_borrow_mut(|handle| {
            if let Some(slot) = handle.0 {
                slot
            } else {
                let claimed = Self::get_all_slots().try_for_each_with_append(|slot| {
                    slot.is_claimed
                        .compare_exchange(false, true, SeqCst, SeqCst)
                        .is_ok()
                });
                handle.0 = Some(claimed);
                claimed
            }
        })
    }
}

/// [`RegionGuard`] is used for protecting that memory will not be reclaimed during the lifetime of this guard.
pub struct RegionGuard {
    slot: &'static Slot,
}

impl Drop for RegionGuard {
    fn drop(&mut self) {
        // No critical_sections in ptrs that managed by this slot
        // Retire all ptrs in primary_list
        if self.slot.critical_sections.fetch_sub(1, SeqCst) == 1 {
            self.slot.primary_list.detach_head();
        }
    }
}

impl Protect for StandardReclaimer {
    type Guard = RegionGuard;

    fn protect() -> Self::Guard {
        let slot = Self::get_or_claim_slot();
        slot.critical_sections.fetch_add(1, SeqCst);
        RegionGuard { slot }
    }
}

/// [`PtrGuard`] is used for protecting a pointer, ie. a raw pointer without ref count support.
pub struct PtrGuard {
    snapshot_ptr: &'static SnapshotPtr,
}

impl Drop for PtrGuard {
    fn drop(&mut self) {
        self.snapshot_ptr.ptr.store(null_mut(), SeqCst);
        self.snapshot_ptr.conflicts.detach_head();
    }
}

impl ProtectPtr for StandardReclaimer {
    type Guard = PtrGuard;

    fn protect_ptr(ptr: *mut u8) -> Self::Guard {
        // TODO: don't search from the beginning every time
        let snapshot_ptr = Self::get_or_claim_slot()
            .snapshots
            .try_for_each_with_append(|s| {
                s.ptr
                    .compare_exchange(null_mut(), ptr, SeqCst, SeqCst)
                    .is_ok()
            });
        PtrGuard { snapshot_ptr }
    }
}

impl Retire for StandardReclaimer {
    fn retire(ptr: *mut u8, f: fn(*mut u8)) {
        // Just add it to the local batch
        let mut borrowed = Self::get_or_claim_slot().tlocal_batch.borrow_mut();
        borrowed.functions.push((ptr, f));
        borrowed.ptrs.insert(ptr);
        if borrowed.functions.len() < borrowed.functions.capacity() {
            return;
        }
        // If batch is full, check retire status of all slots
        let all_slots = Self::get_all_slots();
        // batch_size is equal to the number of slots(aka. active theads)
        let next_batch_size = all_slots.get_nodes_count() * SLOTS_PER_NODE;
        let batch = mem::replace(
            &mut *borrowed,
            Batch {
                functions: Vec::with_capacity(next_batch_size),
                ptrs: HashSet::with_capacity(next_batch_size),
            },
        );
        // Drop the borrow before proceeding in case there is a recursive call to this function.
        drop(borrowed);
        let batch_arc = UnsafeArc::new(batch, 1);
        for slot in all_slots.iter(SeqCst) {
            if slot.critical_sections.load(SeqCst) > 0 {
                // If a thread is in a critical section, it must be made aware of **any** retirements.
                // The snapshots will be checked when that thread exits the critical section.
                slot.primary_list.insert(batch_arc.clone(), Some(slot));
            } else {
                // Otherwise, the snapshots must be checked immediately.
                for snapshot_ptr in slot.snapshots.iter(SeqCst) {
                    let p = snapshot_ptr.ptr.load(SeqCst);
                    // If any snapshot pointer in other threads still points to the pointer going to be retired
                    if !p.is_null() && batch_arc.ptrs.contains(&p) {
                        snapshot_ptr.conflicts.insert(batch_arc.clone(), None);
                    }
                }
            }
        }
    }
}

const SNAPSHOT_PTRS_PER_NODE: usize = 8;

/// A [`Slot`] is a thread-local storage unit for managing memory reclamation.
/// 
/// 1. Each thread claims its own [`Slot`] when it first interacts with the reclamation system.
/// 2. The thread uses this Slot to manage its critical sections, protect pointers (snapshots), and collect retired objects.
/// 3. When objects are retired, they're first added to the thread-local batch.
/// 4. When the batch is full or when necessary, it's moved to the primary list for processing.
/// 5. The reclaimer uses the information in the Slot (critical sections, snapshots) to determine when it's safe to actually free retired objects.
/// 
/// It's [`Sync`] and [`Sync`] because that other theads can access the Slot's 
/// critical_sections(retire), primary_list(retire), is_claimed(get slot) and snapshots(retire&batch drop) fields, but not the tlocal_batch field.
#[derive(Default)]
struct Slot {

    /// List of batches that need to be processed for memory relcamation when critical sections are active.
    /// 
    /// It's used for region guard, it may store batches from other threads.
    primary_list: CollectionList,

    /// Thread-local of objects that have been retired but not yet added to the primary list.
    tlocal_batch: RefCell<Batch>,

    /// List of snapshot pointers that the thread is currently protecting from reclamation.
    /// 
    /// It's used for pointer guard
    snapshots: UnrolledLinkedList<SnapshotPtr, SNAPSHOT_PTRS_PER_NODE>,

    // TODO: snapshots could share entries if their pointers are equal
    // snapshots_by_addr_count: RefCell<HashMap<usize, usize>>,

    /// Counter keeps track of how many critical sections(aka. region guard) are currently active for this thread.
    /// 
    /// While the critical_sections counter is non-zero, 
    /// the memory reclamation system will not deallocate any objects 
    /// that might be accessed in this critical section.
    critical_sections: AtomicUsize,
    
    /// Flag indicates whether this slot is currently claimed by a thread,
    /// ensure that each thread gets its own unique slot.
    is_claimed: AtomicBool,
}

unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

#[derive(Default)]
struct SnapshotPtr {
    ptr: AtomicPtr<u8>,
    conflicts: CollectionList,
}

#[derive(Default)]
struct CollectionList {
    head: AtomicPtr<CollectionNode>,
}

impl CollectionList {
    fn insert(&self, batch: UnsafeArc<Batch>, check_on_drop: Option<&'static Slot>) {
        let mut new = UnsafeArc::new(
            CollectionNode {
                batch,
                next: None,
                check_on_drop,
            },
            2,
        );
        let next = self.head.swap(UnsafeArc::as_ptr(&new), SeqCst);
        if !next.is_null() {
            unsafe {
                new.next = Some(UnsafeArc::from_raw(next));
            }
        }
    }
    fn detach_head(&self) {
        unsafe {
            let ptr = self.head.swap(null_mut(), SeqCst);
            if !ptr.is_null() {
                drop(UnsafeArc::from_raw(ptr));
            }
        }
    }
}

/// A [`CollectionNode`] is just a node of Batch and its associated slot.
struct CollectionNode {
    batch: UnsafeArc<Batch>,
    next: Option<UnsafeArc<CollectionNode>>,
    /// Optional reference to a associated Slot, used for conflict checking when the node is dropped.
    check_on_drop: Option<&'static Slot>,
}

impl Drop for CollectionNode {
    fn drop(&mut self) {
        if let Some(slot) = self.check_on_drop {
            for snapshot_ptr in slot.snapshots.iter(SeqCst) {
                let ptr = snapshot_ptr.ptr.load(SeqCst);
                if !ptr.is_null() && self.batch.ptrs.contains(&ptr) {
                    // TODO: figure out how to do this by moving instead of cloning (RefCell?)
                    snapshot_ptr.conflicts.insert(self.batch.clone(), None);
                }
            }
        }
    }
}

/// Group of multiple objects that have been retired and are waiting to be freed.
/// 
/// 1. When an object is retired (i.e., it's no longer in use 
///    but can't be immediately freed due to potential concurrent access),it's added to a Batch.
/// 2. The object's pointer and its deallocation function are added to the functions vector.
/// 3. The pointer is also added to the ptrs set for quick lookup.
/// 4. Objects in a Batch are held until it's **determined(by check conflict)** safe to deallocate them,
///    by drop the batch itself.
/// 
/// The `functions` and `ptrs` vec are used as fixed capacity vec, with variable capacity per batch
#[derive(Default)]
struct Batch {
    /// 0 -> raw pointer (*mut u8) to the object that needs to be freed.
    /// 1 -> function pointer (fn(*mut u8)) that will be called to free the object.
    /// ensure type safety
    // (type is not over-complex)
    #[allow(clippy::type_complexity)]
    functions: Vec<(*mut u8, fn(*mut u8))>,
    /// Set of raw pointers to all the objects in the batch.
    /// 
    /// Used to fast check if a pointer is in the batch.
    ptrs: HashSet<*mut u8>,
}

impl Drop for Batch {
    fn drop(&mut self) {
        for (ptr, f) in &self.functions {
            (*f)(*ptr);
        }
    }
}

#[derive(Default)]
struct SlotHandle(Option<&'static Slot>);

impl Drop for SlotHandle {
    fn drop(&mut self) {
        if let Some(slot) = self.0 {
            slot.is_claimed.store(false, SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::smr::drc::{Protect, ProtectPtr, Retire};
    use crate::smr::standard_reclaimer::{Batch, StandardReclaimer};
    use crate::utils::helpers::{alloc_box_ptr, dealloc_box_ptr};
    use std::cell::Cell;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering::SeqCst;

    const TEST_PTR: *mut u8 = usize::MAX as *mut u8;

    fn with_flag<F: Fn(*mut Cell<bool>, fn(*mut u8))>(f: F) {
        let flag_ptr = alloc_box_ptr(Cell::new(false));
        let flag_fn = Box::new(|ptr: *mut u8| unsafe {
            (*ptr.cast::<Cell<bool>>()).set(true);
        });
        f(flag_ptr, *flag_fn);
        unsafe {
            dealloc_box_ptr(flag_ptr);
        }
    }

    #[test]
    fn test_protect() {
        let slot = StandardReclaimer::get_or_claim_slot();
        let guard1 = StandardReclaimer::protect();
        let guard2 = StandardReclaimer::protect();
        assert_eq!(slot.critical_sections.load(SeqCst), 2);
        drop(guard1);
        drop(guard2);
        assert_eq!(slot.critical_sections.load(SeqCst), 0);
    }

    #[test]
    fn test_protect_and_retire() {
        with_flag(|flag_ptr, flag_fn| unsafe {
            let slot = StandardReclaimer::get_or_claim_slot();
            let guard = StandardReclaimer::protect();

            StandardReclaimer::retire(flag_ptr.cast::<u8>(), flag_fn);
            assert!(!(*flag_ptr).get());

            drop(guard);

            drop(slot.tlocal_batch.take());
            assert!((*flag_ptr).get());
        });
    }

    #[test]
    fn test_protect_ptr() {
        let guard = StandardReclaimer::protect_ptr(TEST_PTR);
        let tmp = guard.snapshot_ptr;
        assert_eq!(tmp.ptr.load(SeqCst), TEST_PTR);
        drop(guard);
        assert!(tmp.ptr.load(SeqCst).is_null());
    }

    #[test]
    fn test_protect_ptr_and_release() {
        with_flag(|flag_ptr, flag_fn| unsafe {
            let slot = StandardReclaimer::get_or_claim_slot();
            slot.tlocal_batch.replace(Batch {
                functions: Vec::with_capacity(1),
                ptrs: HashSet::with_capacity(1),
            });

            let guard = StandardReclaimer::protect_ptr(flag_ptr.cast::<u8>());

            StandardReclaimer::retire(flag_ptr.cast::<u8>(), flag_fn);
            assert!(!(*flag_ptr).get());

            drop(guard);
            assert!((*flag_ptr).get());
        });
    }
}
