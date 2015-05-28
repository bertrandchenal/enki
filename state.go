package enki

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"
)

const MAXTIMESTAMP = 1<<63 - 1

type FileState struct {
	Timestamp int64
	Checksum []byte
}

type DirState struct {
	Timestamp int64
	FileStates map[string]FileState
	backend Backend
	dirty []string
	prevState *DirState
	root string
}

func NewDirState(path string, backend Backend) *DirState {
	fstates := make(map[string]FileState)
	prevState := LastState(backend)
	if prevState == nil {
		prevState = &DirState{
			FileStates: make(map[string]FileState),
		}
	}

	state := &DirState{
		Timestamp: time.Now().Unix(),
		FileStates: fstates,
		prevState: prevState,
		backend: backend,
		root: path,
	}

	state.scan(path)
	return state
}

func (self *DirState) append(path string, info os.FileInfo, err error) error {
	dotName := info.Name() != "." && filepath.HasPrefix(info.Name(), ".")
	if info.IsDir() {
		if dotName {
			return filepath.SkipDir
		}
		return nil
	} else if dotName {
		return nil
	}

	relpath, err := filepath.Rel(self.root, path)
	check(err)

	fstate, present := self.prevState.FileStates[relpath]
	ts := info.ModTime().Unix()

	if !present {
		// New file
		fstate.Timestamp = ts
		fstate.Checksum, err = GetChecksum(path)
		check(err)
		self.FileStates[relpath] = fstate
		self.dirty = append(self.dirty, relpath)
	} else if ts != fstate.Timestamp {
		// Existing file but new timestamp
		checksum, err := GetChecksum(path)
		check(err)
		newState := FileState{}
		newState.Timestamp = ts
		newState.Checksum = checksum
		self.FileStates[relpath] = newState
		if !bytes.Equal(checksum, fstate.Checksum) {
			self.dirty = append(self.dirty, relpath)
		}
	} else {
		// No changes
		self.FileStates[relpath] = fstate
	}

	return nil
} 

func (self *DirState) scan(path string) {
	err := filepath.Walk(path, self.append)
	check(err)
}

func (self *DirState) Checksum() []byte {
	checksum := md5.New()

	var keys []string
    for k := range self.FileStates {
        keys = append(keys, k)
    }
    sort.Strings(keys)

	for _, path := range keys {
		file_cs := self.FileStates[path]
		io.WriteString(checksum, path)
		checksum.Write(file_cs.Checksum)
	}
	return checksum.Sum(nil)
}

func (self *DirState) Snapshot() {
	if len(self.dirty) == 0 {
		log.Print("Nothing to do")
		return		
	}
	for _, relpath := range self.dirty {
		blob := &Blob{self.backend}
		state, present := self.FileStates[relpath]
		if !present {
			panic("Unexpected Error")
		}

		abspath := path.Join(self.root, relpath)
		fd, err := os.Open(abspath)
		check(err)
		defer fd.Close()
		log.Print("Add ", relpath)
		blob.Snapshot(state.Checksum, fd)
	}
	self.backend.SetState(self)
}

func (self *DirState) GobEncode() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(self)
	check(err)
	return buf.Bytes()
}

func (self *DirState) GobDecode(data []byte) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(self)
	check(err)
}
