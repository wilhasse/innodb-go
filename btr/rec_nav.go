package btr

import "github.com/wilhasse/innodb-go/page"

// GetNextUserRec moves to the next user record, crossing pages as needed.
func GetNextUserRec(cur *page.Cursor) *page.Cursor {
	if cur == nil || cur.Page == nil {
		return nil
	}
	if cur.Next() {
		return cur
	}

	p := cur.Page
	for p != nil && p.NextPage != 0 {
		next := page.GetPage(p.SpaceID, p.NextPage)
		if next == nil {
			return nil
		}
		cur.Page = next
		cur.Index = -1
		if cur.First() {
			return cur
		}
		p = next
	}
	return nil
}

// GetPrevUserRec moves to the previous user record, crossing pages as needed.
func GetPrevUserRec(cur *page.Cursor) *page.Cursor {
	if cur == nil || cur.Page == nil {
		return nil
	}
	if cur.Prev() {
		return cur
	}

	p := cur.Page
	for p != nil && p.PrevPage != 0 {
		prev := page.GetPage(p.SpaceID, p.PrevPage)
		if prev == nil {
			return nil
		}
		cur.Page = prev
		cur.Index = len(prev.Records)
		if cur.Last() {
			return cur
		}
		p = prev
	}
	return nil
}
