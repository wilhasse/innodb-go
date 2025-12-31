package page

import "sync"

type pageKey struct {
	spaceID uint32
	pageNo  uint32
}

// Registry tracks in-memory pages by space and page number.
type Registry struct {
	mu    sync.RWMutex
	pages map[pageKey]*Page
}

// PageRegistry is the default in-memory page registry.
var PageRegistry = NewRegistry()

// NewRegistry allocates a new page registry.
func NewRegistry() *Registry {
	return &Registry{pages: make(map[pageKey]*Page)}
}

// Register stores a page in the registry.
func (r *Registry) Register(page *Page) {
	if r == nil || page == nil {
		return
	}
	key := pageKey{spaceID: page.SpaceID, pageNo: page.PageNo}
	r.mu.Lock()
	r.pages[key] = page
	r.mu.Unlock()
}

// Get returns a page by space and page number.
func (r *Registry) Get(spaceID, pageNo uint32) *Page {
	if r == nil {
		return nil
	}
	key := pageKey{spaceID: spaceID, pageNo: pageNo}
	r.mu.RLock()
	page := r.pages[key]
	r.mu.RUnlock()
	return page
}

// Delete removes a page from the registry.
func (r *Registry) Delete(spaceID, pageNo uint32) {
	if r == nil {
		return
	}
	key := pageKey{spaceID: spaceID, pageNo: pageNo}
	r.mu.Lock()
	delete(r.pages, key)
	r.mu.Unlock()
}

// Count returns the number of pages for a space.
func (r *Registry) Count(spaceID uint32) int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for key := range r.pages {
		if key.spaceID == spaceID {
			count++
		}
	}
	return count
}

// NewPage allocates a page, initializes it, and registers it.
func (r *Registry) NewPage(spaceID, pageNo uint32, pageType uint16) *Page {
	if r == nil {
		return nil
	}
	page := NewPage(spaceID, pageNo, pageType)
	r.Register(page)
	return page
}

// RegisterPage stores a page in the default registry.
func RegisterPage(page *Page) {
	PageRegistry.Register(page)
}

// GetPage returns a page from the default registry.
func GetPage(spaceID, pageNo uint32) *Page {
	return PageRegistry.Get(spaceID, pageNo)
}

// DeletePage removes a page from the default registry.
func DeletePage(spaceID, pageNo uint32) {
	PageRegistry.Delete(spaceID, pageNo)
}

// NewRegisteredPage creates and registers a page in the default registry.
func NewRegisteredPage(spaceID, pageNo uint32, pageType uint16) *Page {
	return PageRegistry.NewPage(spaceID, pageNo, pageType)
}
