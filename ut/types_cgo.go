//go:build cgo

package ut

/*
#include <stdint.h>

typedef uintptr_t ibgo_ulint;
typedef ibgo_ulint ibgo_ibool;
*/
import "C"

type Ulint = C.ibgo_ulint
type IBool = C.ibgo_ibool

const UnivPageSizeShift = 14
const UnivPageSize = 1 << UnivPageSizeShift
const UNIV_PAGE_SIZE = UnivPageSize
