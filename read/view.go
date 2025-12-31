package read

import "sort"

// ReadView defines transaction visibility rules.
type ReadView struct {
	CreatorTrxID uint64
	TrxIDs       []uint64
	LowLimitID   uint64
	UpLimitID    uint64
}

// ViewList tracks open read views.
type ViewList struct {
	Views []*ReadView
}

// NewReadView creates a read view for the given active transactions.
func NewReadView(creator uint64, active []uint64) *ReadView {
	ids := append([]uint64(nil), active...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	maxID := creator
	for _, id := range ids {
		if id > maxID {
			maxID = id
		}
	}
	lowLimit := maxID + 1
	upLimit := lowLimit
	if len(ids) > 0 {
		upLimit = ids[len(ids)-1]
	}

	return &ReadView{
		CreatorTrxID: creator,
		TrxIDs:       ids,
		LowLimitID:   lowLimit,
		UpLimitID:    upLimit,
	}
}

// CopyWithCreator copies the view and inserts the old creator into the trx list.
func (view *ReadView) CopyWithCreator(creator uint64) *ReadView {
	if view == nil {
		return NewReadView(creator, nil)
	}
	ids := append([]uint64(nil), view.TrxIDs...)
	if view.CreatorTrxID != 0 {
		ids = insertDesc(ids, view.CreatorTrxID)
	}
	upLimit := view.UpLimitID
	if len(ids) > 0 {
		upLimit = ids[len(ids)-1]
	}
	return &ReadView{
		CreatorTrxID: creator,
		TrxIDs:       ids,
		LowLimitID:   view.LowLimitID,
		UpLimitID:    upLimit,
	}
}

// Sees reports whether a transaction id is visible in this view.
func (view *ReadView) Sees(trxID uint64) bool {
	if view == nil {
		return true
	}
	if trxID == 0 || trxID == view.CreatorTrxID {
		return true
	}
	if trxID < view.UpLimitID {
		return true
	}
	if trxID >= view.LowLimitID {
		return false
	}
	return !view.contains(trxID)
}

// Open adds a new view to the list.
func (list *ViewList) Open(creator uint64, active []uint64) *ReadView {
	if list == nil {
		return NewReadView(creator, active)
	}
	view := NewReadView(creator, active)
	list.Views = append(list.Views, view)
	return view
}

// OldestCopyOrOpenNew copies the oldest view or opens a new one.
func (list *ViewList) OldestCopyOrOpenNew(creator uint64, active []uint64) *ReadView {
	if list == nil {
		return NewReadView(creator, active)
	}
	if len(list.Views) == 0 {
		return list.Open(creator, active)
	}
	oldest := list.Views[len(list.Views)-1]
	copy := oldest.CopyWithCreator(creator)
	list.Views = append(list.Views, copy)
	return copy
}

// Close removes a view from the list.
func (list *ViewList) Close(view *ReadView) {
	if list == nil || view == nil {
		return
	}
	for i, v := range list.Views {
		if v == view {
			list.Views = append(list.Views[:i], list.Views[i+1:]...)
			return
		}
	}
}

func (view *ReadView) contains(trxID uint64) bool {
	for _, id := range view.TrxIDs {
		if id == trxID {
			return true
		}
	}
	return false
}

func insertDesc(ids []uint64, value uint64) []uint64 {
	for i, id := range ids {
		if value > id {
			out := append(ids[:i], append([]uint64{value}, ids[i:]...)...)
			return out
		}
		if value == id {
			return ids
		}
	}
	return append(ids, value)
}
