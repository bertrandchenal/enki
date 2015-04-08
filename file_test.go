package enki

import (
	"fmt"
	"testing"
)

func TestChecksum(t *testing.T) {
	expect := "c709067ec00d61db0c75d35ace87e21d"
	f := File{Path: "32.jpg"}
	result, err := f.GetChecksum()

	if err != nil {
		t.Errorf(err.Error())
	}

	if fmt.Sprintf("%x", result) != expect {
		t.Errorf("Checksum mismatch", string(result), expect)
	}

}

func TestDistill(t *testing.T) {
	f := File{Path: "/tmp/random.data"} // TODO build file automatically
	store := DummyStore{}
	store.WeakMap = make(map[WeakHash]bool)
	store.BlockMap = make(map[StrongHash]Block)
	_,_  = f.Distill(&store) //result, err
}
