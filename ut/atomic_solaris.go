//go:build atomic_solaris && !atomic_gcc && !atomic_innodb

package ut

const AtomicOpsMode = "solaris"
