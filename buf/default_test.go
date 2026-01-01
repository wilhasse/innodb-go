package buf

import "testing"

func TestDefaultPoolsMapping(t *testing.T) {
	pools := []*Pool{
		NewPool(1, BufPoolDefaultPageSize),
		NewPool(1, BufPoolDefaultPageSize),
	}
	SetDefaultPools(pools)
	defer SetDefaultPools(nil)

	if DefaultPoolCount() != len(pools) {
		t.Fatalf("expected %d pools, got %d", len(pools), DefaultPoolCount())
	}

	space := uint32(1)
	pageNo := uint32(7)
	idx := poolIndex(space, pageNo, len(pools))
	if got := GetPool(space, pageNo); got != pools[idx] {
		t.Fatalf("expected pool %d for page mapping", idx)
	}
}

func TestDefaultPoolsIsolation(t *testing.T) {
	pools := []*Pool{
		NewPool(2, BufPoolDefaultPageSize),
		NewPool(2, BufPoolDefaultPageSize),
	}
	SetDefaultPools(pools)
	defer SetDefaultPools(nil)

	space := uint32(2)
	pageNos := make([]uint32, len(pools))
	used := make(map[uint32]struct{})
	for i := range pools {
		for pageNo := uint32(0); ; pageNo++ {
			if _, ok := used[pageNo]; ok {
				continue
			}
			if poolIndex(space, pageNo, len(pools)) == i {
				pageNos[i] = pageNo
				used[pageNo] = struct{}{}
				break
			}
		}
	}

	for i, pageNo := range pageNos {
		pool := GetPool(space, pageNo)
		if pool != pools[i] {
			t.Fatalf("expected pool %d for page %d", i, pageNo)
		}
		page, _, err := pool.Fetch(space, pageNo)
		if err != nil {
			t.Fatalf("fetch page %d: %v", pageNo, err)
		}
		pool.Release(page)
	}

	for i, pool := range pools {
		stats := pool.Stats()
		if stats.Size != 1 {
			t.Fatalf("expected pool %d size 1, got %d", i, stats.Size)
		}
	}
}
