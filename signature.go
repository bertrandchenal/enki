package enki

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"io"
)

const (
	DATA_SGM = iota
	HASH_SGM = iota
)

type Segment struct {
	Mode       int
	Weakhash   WeakHash
	Stronghash *StrongHash
	Data       []byte
}

type Signature struct {
	Segments []Segment
}

func (self *Signature) AddData(data []byte) {
	strong := StrongHash(md5.Sum(data))
	segment := Segment{
		Mode: DATA_SGM,
		Data: data,
		Stronghash: &strong,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) AddHash(weak WeakHash, strong *StrongHash) {
	segment := Segment{
		Mode:       HASH_SGM,
		Weakhash:   weak,
		Stronghash: strong,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) CheckSum() []byte {
	sgnhash := md5.New()
	for _, segment := range self.Segments {
		sgnhash.Write(segment.Stronghash[:])
	}
	return sgnhash.Sum(nil)
}

func (self *Signature) Extract(backend Backend, w io.Writer) {
	for _, segment := range self.Segments {
		if segment.Mode == DATA_SGM {
			_, err := w.Write(segment.Data)
			check(err)
		} else if segment.Mode == HASH_SGM {
			data := backend.ReadStrong(segment.Stronghash)
			if data == nil {
				panic("Hash not found in backend")
			}
			_, err := w.Write(data)
			check(err)
		}
	}
}

func (self *Signature) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	d := gob.NewDecoder(buf)
	err := d.Decode(&self.Segments)
	return err
}

func (self *Signature) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)

	// Encoding the map
	err := e.Encode(self.Segments) //FIXME fail if file is empty
	return buf.Bytes(), err
}
