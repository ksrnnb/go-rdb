package file

import (
	"testing"
)

func TestEqualsBlockId(t *testing.T) {
	b1 := NewBlockID("test1", 1)
	b2 := NewBlockID("test2", 2)

	if b1.Equals(b2) {
		t.Errorf("TestEqualsBlockId:  b1.Equals(b2) should be false, but get true")
	}

	b3 := NewBlockID("test1", 1)

	if !b1.Equals(b3) {
		t.Errorf("TestEqualsBlockId:  b1.Equals(b3) should be true, but get false")
	}
}

func TestStringBlockId(t *testing.T) {
	b := NewBlockID("test", 1)

	str := b.String()

	expected := "[file test, block 1]"
	if str != expected {
		t.Errorf("TestStringBlockId: str = %s, want %s", str, expected)
	}
}
