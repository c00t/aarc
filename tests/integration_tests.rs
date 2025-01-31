use std::ptr::{null, null_mut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;

use rand::random;

use aarc::{Arc, AsPtr, AtomicArc, AtomicWeak, Guard};

fn test_stack(threads_count: usize, iters_per_thread: usize) {
    #[derive(Default)]
    struct StackNode {
        val: usize,
        next: Option<Arc<Self>>,
    }

    #[derive(Default)]
    struct Stack {
        top: AtomicArc<StackNode>,
    }

    unsafe impl Send for Stack {}
    unsafe impl Sync for Stack {}

    impl Stack {
        fn push(&self, val: usize) {
            let mut top = self.top.load();
            loop {
                let top_ptr = top.as_ref().map_or(null(), AsPtr::as_ptr);
                let new_node = Arc::new(StackNode {
                    val,
                    next: top.as_ref().map(Arc::from),
                });
                match self.top.compare_exchange(top_ptr, Some(&new_node)) {
                    Ok(()) => break,
                    Err(before) => top = before,
                }
            }
        }
        fn pop(&self) -> Option<Guard<StackNode>> {
            let mut top = self.top.load();
            while let Some(top_node) = top.as_ref() {
                match self
                    .top
                    .compare_exchange(top_node.as_ptr(), top_node.next.as_ref())
                {
                    Ok(()) => return top,
                    Err(actual_top) => top = actual_top,
                }
            }
            None
        }
    }

    let stack = Stack::default();

    thread::scope(|s| {
        for _ in 0..threads_count {
            s.spawn(|| {
                for i in 0..iters_per_thread {
                    stack.push(i);
                }
            });
        }
    });

    let val_counts: Vec<AtomicUsize> = (0..iters_per_thread)
        .map(|_| AtomicUsize::default())
        .collect();
    thread::scope(|s| {
        for _ in 0..threads_count {
            s.spawn(|| {
                for _ in 0..iters_per_thread {
                    let node = stack.pop().unwrap();
                    val_counts[node.val].fetch_add(1, Relaxed);
                }
            });
        }
    });

    // Verify that no nodes were lost.

    for count in &val_counts {
        assert_eq!(count.load(Relaxed), threads_count);
    }
}

#[test]
fn test_stack_small() {
    test_stack(5, 10);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_stack_full() {
    test_stack(8, 500);
}

fn test_sorted_linked_list(threads_count: usize, iters_per_thread: usize) {
    #[derive(Default)]
    struct ListNode {
        val: usize,
        prev: AtomicWeak<Self>,
        next: AtomicArc<Self>,
    }

    struct LinkedList {
        head: AtomicArc<ListNode>,
    }

    impl LinkedList {
        fn insert_sorted(&self, val: usize) {
            let mut curr_node = self.head.load().unwrap();
            let mut next = curr_node.next.load();
            loop {
                if next.is_none() || val < next.as_ref().unwrap().val {
                    let new = Arc::new(ListNode {
                        val,
                        prev: AtomicWeak::from(&curr_node),
                        next: next.as_ref().map_or(AtomicArc::default(), AtomicArc::from),
                    });
                    match curr_node.next.compare_exchange(
                        next.as_ref().map_or(null_mut(), Guard::as_ptr),
                        Some(&new),
                    ) {
                        Ok(()) => {
                            if let Some(next_node) = next {
                                // This is technically incorrect; another node could've been
                                // inserted, but it's not crucial for this test.
                                next_node.prev.store(Some(&new));
                            }
                            break;
                        }
                        Err(actual_next) => next = actual_next,
                    }
                } else {
                    curr_node = next.unwrap();
                    next = curr_node.next.load();
                }
            }
        }
    }

    let list = LinkedList {
        head: AtomicArc::new(Some(ListNode::default())),
    };

    thread::scope(|s| {
        for _ in 0..threads_count {
            s.spawn(|| {
                for _ in 0..iters_per_thread {
                    list.insert_sorted(random::<usize>());
                }
            });
        }
    });

    // Verify that no nodes were lost and that the list is in sorted order.
    let mut i = 0;
    let mut curr_node = list.head.load().unwrap();
    loop {
        let next = curr_node.next.load();
        if let Some(next_node) = next {
            assert!(curr_node.val <= next_node.val);
            curr_node = next_node;
            i += 1;
        } else {
            break;
        }
    }
    assert_eq!(threads_count * iters_per_thread, i);
    // Iterate in reverse order using the weak ptrs.
    while let Some(prev_node) = curr_node.prev.load() {
        assert!(curr_node.val >= prev_node.val);
        curr_node = prev_node;
    }
}

#[test]
fn test_sorted_linked_list_small() {
    test_sorted_linked_list(5, 10);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_sorted_linked_list_full() {
    test_sorted_linked_list(8, 500);
}
