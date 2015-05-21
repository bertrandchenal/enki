package enki

import (
	"bytes"
	"fmt"
	"testing"
)


func TestScan(t *testing.T) {
	dstate := NewDirState(test_data, nil)
	// delete unstable files from dstate
	delete(dstate.FileStates, "test-data/random.data")
	delete(dstate.FileStates, "test-data/random.data.extracted")
	delete(dstate.FileStates, "test-data/bloom.gob")
	delete(dstate.FileStates, "test-data/indexes.bolt")

	res := fmt.Sprintf("%x", dstate.Checksum())
	expected := "897c6ade799edcfb9939c58ec4dbd96f"
	if res != expected {
		t.Errorf("Checksum mismatch", res, expected)
	}
}

func TestGob (t *testing.T) {
	dstate := NewDirState(test_data, nil)
	dstatecopy := &DirState{}
	dstatecopy.GobDecode(dstate.GobEncode())
	for key, fs := range dstate.FileStates {
		fscopy := dstatecopy.FileStates[key]
		if fs.Timestamp != fscopy.Timestamp {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
		if ! bytes.Equal(fs.Checksum, fscopy.Checksum) {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
	}
}
