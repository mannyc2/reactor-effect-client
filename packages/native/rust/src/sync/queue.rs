//! Bounded FIFO queues between libwebrtc producers and one host reader.

use super::lock;
use std::collections::VecDeque;
use std::sync::Mutex;

/// An item whose size counts against its queue's byte bound.
pub(crate) trait QueueItem {
    /// The bytes this item holds.
    fn byte_len(&self) -> usize;
}

impl QueueItem for Vec<u8> {
    fn byte_len(&self) -> usize {
        self.len()
    }
}

/// What [`Queue::try_push`] did with an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Push {
    Accepted,
    /// The queue is closed; the item was discarded without being counted.
    Closed,
    /// The queue is full; the item was refused and counted as dropped.
    Overflow,
}

/// What [`Queue::take`] found at the front of the queue.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Taken<T> {
    Item(T),
    /// The reader cannot hold the front item, which stays queued.
    TooSmall,
    Empty,
    Closed,
}

/// A queue's accounting. Every item pushed while the queue is open is
/// eventually counted exactly once: dropped, taken or still queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counts {
    pub(crate) dropped: u64,
    pub(crate) taken: u64,
    pub(crate) queued: usize,
    pub(crate) bytes: usize,
}

struct State<T> {
    items: VecDeque<T>,
    bytes: usize,
    dropped: u64,
    taken: u64,
    closed: bool,
}

/// A bounded FIFO between libwebrtc producers and one host reader, limited to
/// `max_items` items holding at most `max_bytes` bytes together.
pub(crate) struct Queue<T> {
    state: Mutex<State<T>>,
    max_items: usize,
    max_bytes: usize,
}

impl<T: QueueItem> Queue<T> {
    pub(crate) fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(State {
                items: VecDeque::new(),
                bytes: 0,
                dropped: 0,
                taken: 0,
                closed: false,
            }),
            max_items,
            max_bytes,
        }
    }

    /// Queue media, where the newest item always wins: evict the oldest items
    /// until `item` fits. Returns whether it was queued. An item larger than
    /// the whole byte bound is dropped at once.
    pub(crate) fn push_drop_oldest(&self, item: T) -> bool {
        let size = item.byte_len();
        let mut state = lock(&self.state);
        if state.closed {
            return false;
        }
        if size > self.max_bytes {
            state.dropped += 1;
            return false;
        }
        while state.items.len() >= self.max_items || state.bytes + size > self.max_bytes {
            let Some(evicted) = state.items.pop_front() else {
                break;
            };
            state.bytes -= evicted.byte_len();
            state.dropped += 1;
        }
        state.bytes += size;
        state.items.push_back(item);
        true
    }

    /// Queue a transport event, which is never evicted: a full queue refuses
    /// it and its caller retires the connection.
    pub(crate) fn try_push(&self, item: T) -> Push {
        let size = item.byte_len();
        let mut state = lock(&self.state);
        if state.closed {
            return Push::Closed;
        }
        if state.items.len() >= self.max_items || state.bytes + size > self.max_bytes {
            state.dropped += 1;
            return Push::Overflow;
        }
        state.bytes += size;
        state.items.push_back(item);
        Push::Accepted
    }

    /// Discard the backlog, counting it as dropped, and queue only `item`.
    pub(crate) fn replace(&self, item: T) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        state.dropped += state.items.len() as u64;
        state.items.clear();
        state.bytes = item.byte_len();
        state.items.push_back(item);
    }

    /// Remove the front item if `fits` accepts it.
    ///
    /// `fits` sees the item under the queue's lock so it can report the item's
    /// sizes. The caller copies the item after the lock is released, so a
    /// producer never waits for that copy.
    pub(crate) fn take(&self, fits: impl FnOnce(&T) -> bool) -> Taken<T> {
        let mut state = lock(&self.state);
        let Some(item) = state.items.pop_front() else {
            return if state.closed {
                Taken::Closed
            } else {
                Taken::Empty
            };
        };
        if !fits(&item) {
            state.items.push_front(item);
            return Taken::TooSmall;
        }
        state.bytes -= item.byte_len();
        state.taken += 1;
        Taken::Item(item)
    }

    /// Refuse later items and discard queued ones, counting them as dropped.
    pub(crate) fn close(&self) {
        let mut state = lock(&self.state);
        state.dropped += state.items.len() as u64;
        state.items.clear();
        state.bytes = 0;
        state.closed = true;
    }

    pub(crate) fn counts(&self) -> Counts {
        let state = lock(&self.state);
        Counts {
            dropped: state.dropped,
            taken: state.taken,
            queued: state.items.len(),
            bytes: state.bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Rng;

    /// A queue item carrying its push order, with an arbitrary size.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Item {
        sequence: u64,
        size: usize,
    }

    impl QueueItem for Item {
        fn byte_len(&self) -> usize {
            self.size
        }
    }

    fn take_any<T: QueueItem>(queue: &Queue<T>) -> Option<T> {
        match queue.take(|_| true) {
            Taken::Item(item) => Some(item),
            Taken::TooSmall => panic!("take_any accepts every item"),
            Taken::Empty | Taken::Closed => None,
        }
    }

    #[test]
    fn media_evicts_the_oldest_items_and_counts_each_one() {
        let queue = Queue::new(2, 32);
        for fill in 1..=3u8 {
            assert!(queue.push_drop_oldest(vec![fill; 8]));
        }
        assert_eq!(take_any(&queue), Some(vec![2; 8]));
        assert_eq!(take_any(&queue), Some(vec![3; 8]));
        assert_eq!(take_any(&queue), None);

        // The byte bound evicts too, and an item above it is dropped unqueued.
        assert!(queue.push_drop_oldest(vec![4; 20]));
        assert!(queue.push_drop_oldest(vec![5; 20]));
        assert!(!queue.push_drop_oldest(vec![6; 33]));
        assert_eq!(
            queue.counts(),
            Counts {
                dropped: 3,
                taken: 2,
                queued: 1,
                bytes: 20
            }
        );
    }

    #[test]
    fn events_are_refused_rather_than_evicted_when_full() {
        let queue = Queue::new(2, 32);
        assert_eq!(queue.try_push(vec![1; 8]), Push::Accepted);
        assert_eq!(queue.try_push(vec![2; 30]), Push::Overflow, "byte bound");
        assert_eq!(queue.try_push(vec![3; 8]), Push::Accepted);
        assert_eq!(queue.try_push(vec![4; 1]), Push::Overflow, "item bound");
        assert_eq!(take_any(&queue), Some(vec![1; 8]));
        assert_eq!(take_any(&queue), Some(vec![3; 8]));
        assert_eq!(queue.counts().dropped, 2);
    }

    #[test]
    fn replace_keeps_only_the_new_item_and_drops_the_backlog() {
        let queue = Queue::new(4, 64);
        assert_eq!(queue.try_push(vec![1; 8]), Push::Accepted);
        assert_eq!(queue.try_push(vec![2; 8]), Push::Accepted);
        queue.replace(vec![9; 3]);
        assert_eq!(
            queue.counts(),
            Counts {
                dropped: 2,
                taken: 0,
                queued: 1,
                bytes: 3
            }
        );
        assert_eq!(take_any(&queue), Some(vec![9; 3]));
    }

    #[test]
    fn take_keeps_an_item_the_reader_cannot_hold() {
        let queue = Queue::new(4, 64);
        assert!(queue.push_drop_oldest(vec![7; 12]));
        let mut seen = 0;
        let taken = queue.take(|item| {
            seen = item.len();
            false
        });
        assert_eq!(taken, Taken::TooSmall);
        assert_eq!(seen, 12);
        assert_eq!(queue.counts().queued, 1);
        assert_eq!(take_any(&queue), Some(vec![7; 12]));
        assert_eq!(queue.counts().taken, 1);
    }

    #[test]
    fn close_drops_what_is_queued_and_refuses_later_items() {
        let queue = Queue::new(4, 64);
        assert!(queue.push_drop_oldest(vec![1; 4]));
        assert!(queue.push_drop_oldest(vec![2; 4]));
        queue.close();
        assert_eq!(queue.take(|_| true), Taken::Closed);
        assert!(!queue.push_drop_oldest(vec![3; 4]));
        assert_eq!(queue.try_push(vec![4; 4]), Push::Closed);
        queue.replace(vec![5; 4]);
        assert_eq!(
            queue.counts(),
            Counts {
                dropped: 2,
                taken: 0,
                queued: 0,
                bytes: 0
            },
            "a closed queue counts nothing it refused"
        );
    }

    /// Drive a fresh queue with seeded random pushes, takes and a rare close,
    /// checking after every operation the properties both queue kinds share:
    /// its bounds, FIFO order, and that each item pushed while it was open is
    /// counted exactly once. `push` checks the property of one queue kind; it
    /// is told whether the queue is open.
    fn exercise(seed: u64, push: impl Fn(&Queue<Item>, Item, Bounds, bool)) {
        let mut rng = Rng::new(seed);
        for _ in 0..200 {
            let bounds = Bounds {
                items: 1 + rng.below(8),
                bytes: 1 + rng.below(64),
            };
            let queue = Queue::new(bounds.items, bounds.bytes);
            let (mut open, mut observed, mut last_taken) = (true, 0, None);
            for sequence in 0..400 {
                match rng.below(100) {
                    0..=54 => {
                        // Some items exceed the whole byte bound.
                        let size = rng.below(bounds.bytes + bounds.bytes / 4 + 1);
                        push(&queue, Item { sequence, size }, bounds, open);
                        observed += u64::from(open);
                    }
                    55..=98 => {
                        if let Some(item) = take_any(&queue) {
                            assert!(
                                last_taken.is_none_or(|last| item.sequence > last),
                                "items must leave in the order they were pushed"
                            );
                            last_taken = Some(item.sequence);
                        }
                    }
                    _ => {
                        queue.close();
                        open = false;
                    }
                }
                let counts = queue.counts();
                assert!(
                    counts.queued <= bounds.items,
                    "{counts:?} breaks {bounds:?}"
                );
                assert!(counts.bytes <= bounds.bytes, "{counts:?} breaks {bounds:?}");
                assert_eq!(
                    counts.dropped + counts.taken + counts.queued as u64,
                    observed,
                    "every item pushed while open is dropped, taken or queued, once"
                );
            }
            while take_any(&queue).is_some() {}
            assert_eq!(queue.counts().bytes, 0, "a drained queue holds no bytes");
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct Bounds {
        items: usize,
        bytes: usize,
    }

    #[test]
    fn media_accounting_holds_under_random_operations() {
        exercise(0x5eed_0001, |queue, item, bounds, open| {
            let fits = item.size <= bounds.bytes;
            assert_eq!(
                queue.push_drop_oldest(item),
                open && fits,
                "while open, the newest media is queued exactly when it fits the byte bound"
            );
        });
    }

    #[test]
    fn event_accounting_holds_under_random_operations() {
        exercise(0x5eed_0002, |queue, item, bounds, open| {
            let before = queue.counts();
            let room = before.queued < bounds.items && before.bytes + item.size <= bounds.bytes;
            let expected = match (open, room) {
                (false, _) => Push::Closed,
                (true, true) => Push::Accepted,
                (true, false) => Push::Overflow,
            };
            assert_eq!(queue.try_push(item), expected);
            assert!(
                queue.counts().queued >= before.queued,
                "events must never be evicted"
            );
        });
    }
}
