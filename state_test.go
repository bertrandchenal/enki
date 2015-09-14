package enki

import (
	"bytes"
	"fmt"
	"testing"
)

func TestScan(t *testing.T) {
	dstate := NewDirState(test_data, nil)
	// delete unstable files from dstate
	delete(dstate.FileStates, "random.data")
	delete(dstate.FileStates, "random.data.extracted")

	res := fmt.Sprintf("%x", dstate.Checksum())
	expected := "b1c49f719c0b89e50a9a5a2fa1e3efeb"
	if res != expected {
		t.Errorf("Checksum mismatch", res, expected)
	}
}

func TestGob(t *testing.T) {
	dstate := NewDirState(test_data, nil)
	dstatecopy := &DirState{}
	dstatecopy.GobDecode(dstate.GobEncode())
	for key, fs := range dstate.FileStates {
		fscopy := dstatecopy.FileStates[key]
		if fs.Timestamp != fscopy.Timestamp {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
		if !bytes.Equal(fs.Checksum, fscopy.Checksum) {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
	}
}
