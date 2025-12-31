package page

import "testing"

func TestRegistryLifecycle(t *testing.T) {
	reg := NewRegistry()
	if got := reg.Get(1, 2); got != nil {
		t.Fatalf("expected nil page, got %+v", got)
	}
	page := reg.NewPage(1, 2, PageTypeIndex)
	if page == nil {
		t.Fatalf("expected page")
	}
	if got := reg.Get(1, 2); got != page {
		t.Fatalf("Get returned different page")
	}
	reg.Delete(1, 2)
	if got := reg.Get(1, 2); got != nil {
		t.Fatalf("expected page deleted")
	}
}

func TestDefaultRegistryHelpers(t *testing.T) {
	page := NewRegisteredPage(3, 4, PageTypeIndex)
	if page == nil {
		t.Fatalf("expected page")
	}
	if got := GetPage(3, 4); got != page {
		t.Fatalf("GetPage returned different page")
	}
	DeletePage(3, 4)
	if got := GetPage(3, 4); got != nil {
		t.Fatalf("expected page deleted")
	}
}
