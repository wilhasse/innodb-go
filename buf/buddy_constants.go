package buf

import (
	"strconv"

	"github.com/wilhasse/innodb-go/ut"
)

// BufBuddyLowShift mirrors BUF_BUDDY_LOW_SHIFT.
var BufBuddyLowShift = func() int {
	if strconv.IntSize <= 32 {
		return 6
	}
	return 7
}()

// BufBuddyLow mirrors BUF_BUDDY_LOW.
var BufBuddyLow = 1 << BufBuddyLowShift

// BufBuddySizes mirrors BUF_BUDDY_SIZES.
var BufBuddySizes = ut.UnivPageSizeShift - BufBuddyLowShift

// BufBuddyHigh mirrors BUF_BUDDY_HIGH.
var BufBuddyHigh = BufBuddyLow << BufBuddySizes

// BuddyStat mirrors buf_buddy_stat_t.
type BuddyStat struct {
	Used          uint64
	Relocated     uint64
	RelocatedUsec uint64
}

// BufBuddyStat mirrors buf_buddy_stat.
var BufBuddyStat []BuddyStat

// BufBuddyVarInit resets buddy allocator statistics.
func BufBuddyVarInit() {
	BufBuddyStat = make([]BuddyStat, BufBuddySizes+1)
}
