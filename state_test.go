package enki

import (
	"fmt"
	"bytes"
	"testing"
)

func TestScan(t *testing.T) {
	memoryBackend = NewMemoryBackend().(Backend)
	dstate := NewDirState(test_data, memoryBackend)
	// delete unstable files from dstate
	delete(dstate.FileStates, "random.data")
	delete(dstate.FileStates, "random.data.extracted")

	res := fmt.Sprintf("%x", dstate.Checksum())
	expected := "10b11c832b0df76a4021afa889b80a34"
	if res != expected {
		t.Errorf("Checksum mismatch", res, expected)
	}
}

func TestGob(t *testing.T) {
	memoryBackend = NewMemoryBackend().(Backend)
	dstate := NewDirState(test_data, memoryBackend)
	dstatecopy := &DirState{}
	dstatecopy.GobDecode(dstate.GobEncode())
	for key, fs := range dstate.FileStates {
		fscopy := dstatecopy.FileStates[key]
		if fs.Timestamp != fscopy.Timestamp {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
		if !bytes.Equal(fs.Sgn.CheckSum(), fscopy.Sgn.CheckSum()) {
			t.Errorf("Filestate mismatch", fs, fscopy)
		}
	}
}
