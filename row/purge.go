package row

import "github.com/wilhasse/innodb-go/data"

// PurgeItem tracks a row eligible for purge.
type PurgeItem struct {
	Tuple   *data.Tuple
	Deleted bool
}

// PurgeList stores items that may be purged.
type PurgeList struct {
	Items []PurgeItem
}

// Add appends a tuple to the purge list.
func (list *PurgeList) Add(tuple *data.Tuple) {
	if list == nil {
		return
	}
	list.Items = append(list.Items, PurgeItem{Tuple: tuple})
}

// MarkDeleted marks an item as deleted.
func (list *PurgeList) MarkDeleted(index int) {
	if list == nil || index < 0 || index >= len(list.Items) {
		return
	}
	list.Items[index].Deleted = true
}

// Run purges deleted items and returns the number purged.
func (list *PurgeList) Run() int {
	if list == nil {
		return 0
	}
	purged := 0
	kept := list.Items[:0]
	for _, item := range list.Items {
		if item.Deleted || item.Tuple == nil {
			purged++
			continue
		}
		kept = append(kept, item)
	}
	list.Items = kept
	return purged
}
