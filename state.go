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
	NEW_FILE     = iota
	CHANGED_FILE = iota
	DELETED_FILE = iota
)

type FileState struct {
	Timestamp int64
	SgnSum   []byte
	Status    int
	Sgn       *Signature
}

type DirState struct {
	Timestamp  int64
	FileStates map[string]FileState
	backend    Backend
	prevState  *DirState
	root       string
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
		Timestamp:  time.Now().Unix(),
		FileStates: fstates,
		prevState:  prevState,
		root:       path,
		backend:    backend,
	}

	err := filepath.Walk(path, state.append)
	check(err)

	state.detect_deletion()
	return state
}

func (self *DirState) append(pathname string, info os.FileInfo, err error) error {
	dotName := info.Name() != "." && filepath.HasPrefix(info.Name(), ".")
	if info.IsDir() {
		if dotName {
			return filepath.SkipDir
		}
		return nil
	} else if dotName {
		return nil
	}

	relpath, err := filepath.Rel(self.root, pathname)
	check(err)

	prevState, present := self.prevState.FileStates[relpath]
	ts := info.ModTime().Unix()
	newState := FileState{}
	newState.Timestamp = ts

	if !present || ts != prevState.Timestamp{
		// Changed file
		blob := &Blob{self.backend}
		abspath := path.Join(self.root, relpath)
		fd, err := os.Open(abspath)
		check(err)
		defer fd.Close()
		info, err := fd.Stat()
		check(err)
		newState.Sgn = blob.Snapshot(fd, info.Size())

		// Compute blob checksum
		sgnsum := newState.Sgn.CheckSum()
		if !present {
			newState.Status = NEW_FILE
		} else if !bytes.Equal(sgnsum, prevState.SgnSum) {
			newState.Status = CHANGED_FILE
		}
		newState.SgnSum = sgnsum
		self.FileStates[relpath] = newState

	} else {
		// No changes
		newState.SgnSum = prevState.SgnSum
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
		checksum.Write(file_cs.SgnSum)
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

func (self *DirState) Snapshot() {
	snapped := false
	for relpath, fst := range self.FileStates {
		if !fst.Dirty() {
			continue
		}
		log.Print("Add ", relpath)
		self.backend.WriteSignature(fst.SgnSum, fst.Sgn)
		snapped = true
	}
	if snapped {
		self.backend.WriteState(self)
	}
}

func (self *DirState) RestorePrev() {
	var fd io.ReadWriteCloser
	var err error
	// Remove files not in prevState
	for relpath, fst := range self.FileStates {
		if fst.Status == NEW_FILE {
			abspath := path.Join(self.root, relpath)
			log.Print("Delete ", relpath)
			err = os.Remove(abspath)
			check(err)
		}

		if !(fst.Status == CHANGED_FILE || fst.Status == DELETED_FILE) {
			continue
		}

		// Restore missing & modfied files
		blob := &Blob{self.backend}
		abspath := path.Join(self.root, relpath)

		if fst.Status == DELETED_FILE {
			dir := filepath.Dir(abspath)
			err = os.MkdirAll(dir, 0777)
			fd, err = os.Create(abspath)
		} else {
			fd, err = os.Open(abspath)
		}
		check(err)
		defer fd.Close()
		log.Print("Restore ", relpath)
		blob.Restore(fst.SgnSum, fd)
		atime := time.Now()
		mtime := time.Unix(fst.Timestamp, 0)
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
