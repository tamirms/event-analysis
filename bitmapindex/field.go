package bitmapindex

// Field identifies an indexable event field.
type Field byte

const (
	_                     = iota
	FieldContractID Field = iota
	FieldTopic0
	FieldTopic1
	FieldTopic2
	FieldTopic3
	fieldCount // internal sentinel
)

// FieldKey identifies a (field, key) pair for batch lookups.
type FieldKey struct {
	Field Field
	Key   []byte
}

// composeKey appends key and byte(f) into dst, returning the result slice.
// Callers can use a stack-allocated [256]byte to avoid heap allocation.
func composeKey(dst []byte, f Field, key []byte) []byte {
	needed := len(key) + 1
	if cap(dst) >= needed {
		dst = dst[:needed]
	} else {
		dst = make([]byte, needed)
	}
	copy(dst, key)
	dst[len(key)] = byte(f)
	return dst
}
