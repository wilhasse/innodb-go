//go:build atomic_innodb && !atomic_gcc && !atomic_solaris

package ut

const AtomicOpsMode = "innodb"
