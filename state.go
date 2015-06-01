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

const (
	MAXTIMESTAMP = 1<<63 - 1
	NEW_FILE = iota
	CHANGED_FILE = iota
)

type FileState struct {
	Timestamp int64
	Checksum []byte
}

type DirState struct {
	Timestamp int64
	FileStates map[string]FileState
	backend Backend
	dirty map[string]int
	prevState *DirState
	root string
}

func NewDirState(path string, prevState *DirState) *DirState {
	fstates := make(map[string]FileState)
	if prevState == nil {
		prevState = &DirState{
			FileStates: make(map[string]FileState),
		}
	}

	state := &DirState{
		Timestamp: time.Now().Unix(),
		FileStates: fstates,
		prevState: prevState,
		dirty: make(map[string]int),
		root: path,
	}

	err := filepath.Walk(path, state.append)
	check(err)
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
		self.dirty[relpath] = NEW_FILE
	} else if ts != fstate.Timestamp {
		// Existing file but new timestamp
		checksum, err := GetChecksum(path)
		check(err)
		newState := FileState{}
		newState.Timestamp = ts
		newState.Checksum = checksum
		self.FileStates[relpath] = newState
		if !bytes.Equal(checksum, fstate.Checksum) {
			self.dirty[relpath] = CHANGED_FILE
		}
	} else {
		// No changes
		self.FileStates[relpath] = fstate
	}

	return nil
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

func (self *DirState) Snapshot(backend Backend) {
	if len(self.dirty) == 0 {
		return		
	}
	for relpath, _ := range self.dirty {
		blob := &Blob{backend}
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
	backend.SetState(self)
}

func (self *DirState) RestorePrev(backend Backend) {
	// Remove files not in prevState
	for relpath, reason := range self.dirty {
		if reason == NEW_FILE {
			abspath := path.Join(self.root, relpath)
			err := os.Remove(abspath)
			check(err)
		}
	}

	// Restore missing & modfied files
	for relpath, state := range self.prevState.FileStates {
		_, present := self.FileStates[relpath]
		_, is_dirty := self.dirty[relpath]
		if present && !is_dirty {
			continue
		}
		blob := &Blob{backend}
		abspath := path.Join(self.root, relpath)

		var fd *os.File
		_, err := os.Stat(abspath)
		if os.IsNotExist(err) {
			fd, err = os.Create(abspath)
		} else {
			fd, err = os.Open(abspath)
		}
		check(err)
		defer fd.Close()
		log.Print("Restore ", relpath)
		blob.Restore(state.Checksum, fd)
		atime := time.Now()
		mtime := time.Unix(state.Timestamp, 0)
		os.Chtimes(abspath, atime, mtime)
	}
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

func LastState(b Backend) *DirState {
	return b.GetState(MAXTIMESTAMP)
}
