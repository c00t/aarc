pub trait Protect {
    type Guard;
    /// Get a region guard of the given reclaimer
    fn protect() -> Self::Guard;
}

pub trait ProtectPtr {
    type Guard;
    /// Get a pointer guard of the given reclaimer
    fn protect_ptr(ptr: *mut u8) -> Self::Guard;
}

pub trait Retire {
    /// Retire a given pointer in the given reclaimer
    fn retire(ptr: *mut u8, f: fn(*mut u8));
}
