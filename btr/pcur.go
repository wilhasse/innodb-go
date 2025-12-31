package btr

// Persistent cursor relative positions.
const (
	PcurOn                = 1
	PcurBefore            = 2
	PcurAfter             = 3
	PcurBeforeFirstInTree = 4
	PcurAfterLastInTree   = 5
)

// Persistent cursor state flags.
const (
	PcurIsPositioned  = 1997660512
	PcurWasPositioned = 1187549791
	PcurNotPositioned = 1328997689
	PcurOldStored     = 908467085
	PcurOldNotStored  = 122766467
)

// Pcur mirrors btr_pcur_t in a simplified form.
type Pcur struct {
	Cur       *Cur
	RelPos    int
	PosState  int
	OldStored int
	LatchMode LatchMode
	StoredKey []byte
}

// NewPcur allocates and initializes a persistent cursor.
func NewPcur(tree *Tree) *Pcur {
	p := &Pcur{
		Cur:       NewCur(tree),
		RelPos:    PcurOn,
		PosState:  PcurNotPositioned,
		OldStored: PcurOldNotStored,
		LatchMode: BtrNoLatches,
	}
	return p
}

// Init resets a persistent cursor.
func (p *Pcur) Init() {
	if p == nil {
		return
	}
	if p.Cur != nil {
		p.Cur.Invalidate()
	}
	p.StoredKey = nil
	p.RelPos = PcurOn
	p.PosState = PcurNotPositioned
	p.OldStored = PcurOldNotStored
	p.LatchMode = BtrNoLatches
}

// Free releases resources held by the cursor.
func (p *Pcur) Free() {
	if p == nil {
		return
	}
	p.Cur = nil
	p.StoredKey = nil
	p.RelPos = PcurOn
	p.PosState = PcurNotPositioned
	p.OldStored = PcurOldNotStored
	p.LatchMode = BtrNoLatches
}

// OpenOnUserRec positions the cursor based on a search key.
func (p *Pcur) OpenOnUserRec(key []byte, mode SearchMode) bool {
	if p == nil {
		return false
	}
	if p.Cur == nil {
		p.Cur = NewCur(nil)
	}
	if p.Cur.Tree == nil {
		return false
	}
	found := p.Cur.Search(key, mode)
	p.PosState = PcurIsPositioned
	if found {
		p.RelPos = PcurOn
		return true
	}
	if mode == SearchGE {
		p.RelPos = PcurAfterLastInTree
	} else {
		p.RelPos = PcurBeforeFirstInTree
	}
	return false
}

// OpenOnUserRecFunc is an alias for OpenOnUserRec.
func (p *Pcur) OpenOnUserRecFunc(key []byte, mode SearchMode) bool {
	return p.OpenOnUserRec(key, mode)
}

// OpenAtIndexSide positions the cursor at the left or right side of the tree.
func (p *Pcur) OpenAtIndexSide(left bool) bool {
	if p == nil || p.Cur == nil {
		return false
	}
	found := p.Cur.OpenAtIndexSide(left)
	p.PosState = PcurIsPositioned
	if found {
		p.RelPos = PcurOn
		return true
	}
	if left {
		p.RelPos = PcurBeforeFirstInTree
	} else {
		p.RelPos = PcurAfterLastInTree
	}
	return false
}

// OpenAtRandom positions the cursor at a pseudo-random record.
func (p *Pcur) OpenAtRandom() bool {
	if p == nil || p.Cur == nil {
		return false
	}
	found := p.Cur.OpenAtRandom()
	p.PosState = PcurIsPositioned
	if found {
		p.RelPos = PcurOn
		return true
	}
	p.RelPos = PcurAfterLastInTree
	return false
}

// MoveToNextPage moves the cursor to the first record on the next leaf.
func (p *Pcur) MoveToNextPage() bool {
	if p == nil || p.Cur == nil || !p.Cur.Valid() {
		return false
	}
	start := p.Cur.Cursor.node
	for p.Cur.Next() {
		if p.Cur.Cursor.node != start {
			p.RelPos = PcurOn
			p.PosState = PcurIsPositioned
			return true
		}
	}
	p.RelPos = PcurAfterLastInTree
	p.PosState = PcurWasPositioned
	return false
}

// MoveBackwardFromPage moves the cursor to the last record on the previous leaf.
func (p *Pcur) MoveBackwardFromPage() bool {
	if p == nil || p.Cur == nil || !p.Cur.Valid() {
		return false
	}
	start := p.Cur.Cursor.node
	for p.Cur.Prev() {
		if p.Cur.Cursor.node != start {
			p.RelPos = PcurOn
			p.PosState = PcurIsPositioned
			return true
		}
	}
	p.RelPos = PcurBeforeFirstInTree
	p.PosState = PcurWasPositioned
	return false
}

// StorePosition records the current cursor position for later restore.
func (p *Pcur) StorePosition() {
	if p == nil {
		return
	}
	if p.Cur == nil || !p.Cur.Valid() {
		p.StoredKey = nil
		if p.Cur != nil && p.Cur.Tree != nil && p.Cur.Tree.Size() == 0 {
			if p.RelPos != PcurAfterLastInTree {
				p.RelPos = PcurBeforeFirstInTree
			}
		}
		p.OldStored = PcurOldStored
		p.PosState = PcurWasPositioned
		return
	}
	p.StoredKey = cloneBytes(p.Cur.Cursor.node.keys[p.Cur.Cursor.index])
	p.RelPos = PcurOn
	p.OldStored = PcurOldStored
	p.PosState = PcurWasPositioned
}

// CopyStoredPosition copies stored state from another cursor.
func (p *Pcur) CopyStoredPosition(src *Pcur) {
	if p == nil || src == nil {
		return
	}
	p.StoredKey = cloneBytes(src.StoredKey)
	p.RelPos = src.RelPos
	p.PosState = src.PosState
	p.OldStored = src.OldStored
	p.LatchMode = src.LatchMode
}

// RestorePosition restores the stored position, returning true on exact match.
func (p *Pcur) RestorePosition() bool {
	if p == nil || p.Cur == nil || p.Cur.Tree == nil {
		return false
	}
	if p.OldStored != PcurOldStored {
		return false
	}
	if len(p.StoredKey) == 0 {
		p.Cur.Invalidate()
		p.PosState = PcurWasPositioned
		return false
	}

	switch p.RelPos {
	case PcurBeforeFirstInTree, PcurAfterLastInTree:
		p.Cur.Invalidate()
		p.PosState = PcurWasPositioned
		return false
	case PcurBefore:
		p.Cur.Search(p.StoredKey, SearchLE)
		if p.Cur.Valid() && p.Cur.Tree.compare(p.Cur.Cursor.node.keys[p.Cur.Cursor.index], p.StoredKey) == 0 {
			p.Cur.Prev()
		}
		p.PosState = PcurWasPositioned
		return false
	case PcurAfter:
		p.Cur.Search(p.StoredKey, SearchGE)
		if p.Cur.Valid() && p.Cur.Tree.compare(p.Cur.Cursor.node.keys[p.Cur.Cursor.index], p.StoredKey) == 0 {
			p.Cur.Next()
		}
		p.PosState = PcurWasPositioned
		return false
	default:
		p.Cur.Search(p.StoredKey, SearchLE)
		exact := p.Cur.Valid() && p.Cur.Tree.compare(p.Cur.Cursor.node.keys[p.Cur.Cursor.index], p.StoredKey) == 0
		p.PosState = PcurWasPositioned
		return exact
	}
}
