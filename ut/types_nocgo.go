//go:build !cgo

package ut

type Ulint = uintptr
type IBool = Ulint

const UnivPageSizeShift = 14
const UnivPageSize = 1 << UnivPageSizeShift
const UNIV_PAGE_SIZE = UnivPageSize
