//go:build !atomic_gcc && !atomic_solaris && !atomic_innodb

package ut

const AtomicOpsMode = "auto"
