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
	DELETED_FILE = iota
)

type FileState struct {
	Timestamp int64
	Checksum []byte
	Status int
}

type DirState struct {
	Timestamp int64
	FileStates map[string]FileState
	backend Backend
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
		root: path,
	}

	err := filepath.Walk(path, state.append)
	check(err)

	state.detect_deletion()
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

	prevState, present := self.prevState.FileStates[relpath]
	ts := info.ModTime().Unix()
	newState := FileState{}
	newState.Timestamp = ts

	if !present {
		// New file
		newState.Checksum, err = GetChecksum(path)
		check(err)
		newState.Status = NEW_FILE
		self.FileStates[relpath] = newState

	} else if ts != prevState.Timestamp {
		// Existing file but new timestamp
		checksum, err := GetChecksum(path)
		check(err)
		newState.Checksum = checksum
		if !bytes.Equal(checksum, prevState.Checksum) {
			newState.Status = CHANGED_FILE
		}
		self.FileStates[relpath] = newState
	} else {
		// No changes
		newState.Checksum = prevState.Checksum
		self.FileStates[relpath] = newState
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

func (self *DirState) detect_deletion() {
	for relpath, state := range self.prevState.FileStates {
		_, present := self.FileStates[relpath]
		if present {
			continue
		}
		state.Status = DELETED_FILE
		self.FileStates[relpath] = state
	}
}

func (self *DirState) Snapshot(backend Backend) {
	snapped := false
	for relpath, state := range self.FileStates {
		if !state.Dirty() {
			continue
		}
		blob := &Blob{backend}
		abspath := path.Join(self.root, relpath)
		fd, err := os.Open(abspath)
		check(err)
		defer fd.Close()

		log.Print("Add ", relpath)
		info, err := fd.Stat()
		check(err)
		blob.Snapshot(state.Checksum, fd, info.Size())
		snapped = true
	}
	if snapped {
		backend.WriteState(self)
	}
}

func (self *DirState) RestorePrev(backend Backend) {
	var fd io.ReadWriteCloser
	var err error
	// Remove files not in prevState
	for relpath, state := range self.FileStates {
		if state.Status == NEW_FILE {
			abspath := path.Join(self.root, relpath)
			log.Print("Delete ", relpath)
			err = os.Remove(abspath)
			check(err)
		}

		if !(state.Status == CHANGED_FILE || state.Status == DELETED_FILE) {
			continue
		}

		// Restore missing & modfied files
		blob := &Blob{backend}
		abspath := path.Join(self.root, relpath)

		if state.Status == DELETED_FILE {
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
	return b.ReadState(MAXTIMESTAMP)
}

func (self *FileState) Dirty() bool {
	return self.Status == NEW_FILE || self.Status == CHANGED_FILE
}
