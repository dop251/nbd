package nbd

type buffer []byte

func (b *buffer) allocate(capacity int) []byte {
	if cap(*b) < capacity {
		*b = make([]byte, capacity)
	}
	return (*b)[:capacity]
}

func (b *buffer) grow(capacity int) []byte {
	if cap(*b) < capacity {
		n := make([]byte, capacity)
		copy(n, *b)
		*b = n
	}
	return (*b)[:capacity]
}
