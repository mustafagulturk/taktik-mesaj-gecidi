package gateway

import "syscall"

// syscallEMFILE is "too many open files", a transient accept error.
const syscallEMFILE = syscall.EMFILE
