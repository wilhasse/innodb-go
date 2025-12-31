package ut

// Vector stores a growable slice of items.
type Vector struct {
	Data []any
	Used int
}

// VectorCreate allocates a vector with the given initial size.
func VectorCreate(size int) *Vector {
	if size <= 0 {
		size = 1
	}
	return &Vector{Data: make([]any, size)}
}

// VectorPush appends an element, growing storage as needed.
func VectorPush(vec *Vector, elem any) {
	if vec == nil {
		return
	}
	if vec.Used >= len(vec.Data) {
		newData := make([]any, len(vec.Data)*2)
		copy(newData, vec.Data)
		vec.Data = newData
	}
	vec.Data[vec.Used] = elem
	vec.Used++
}

// VectorLen returns the number of used elements.
func VectorLen(vec *Vector) int {
	if vec == nil {
		return 0
	}
	return vec.Used
}

// VectorGet returns the element at index.
func VectorGet(vec *Vector, index int) any {
	if vec == nil || index < 0 || index >= vec.Used {
		return nil
	}
	return vec.Data[index]
}

// VectorSlice returns the used portion of the vector.
func VectorSlice(vec *Vector) []any {
	if vec == nil {
		return nil
	}
	return vec.Data[:vec.Used]
}
