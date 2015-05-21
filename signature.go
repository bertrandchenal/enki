package enki

import (
	"bytes"
	"encoding/gob"
	"io"
)

type Segment struct {
	Mode int
	Weakhash WeakHash
	Stronghash *StrongHash
	Data []byte
}

type Signature struct {
	Segments []Segment
}


func (self *Signature) AddData(data []byte) {
	segment := Segment{
		Mode: DATA_SGM,
		Data: data,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) AddHash(weak WeakHash, strong *StrongHash) {
	segment := Segment{
		Mode: HASH_SGM,
		Weakhash: weak,
		Stronghash: strong,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) Extract(backend Backend, w io.Writer) {
	for _, segment := range self.Segments {
		if segment.Mode == DATA_SGM {
			w.Write(segment.Data)
		} else if segment.Mode == HASH_SGM {
			data := backend.GetStrong(segment.Stronghash)
			if data == nil {
				panic("Hash not found in backend")
			}
			w.Write(data)
		}
	}
}

func (self *Signature) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
    d := gob.NewDecoder(buf)
    err := d.Decode(self)
	return err
}

func (self *Signature) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
    e := gob.NewEncoder(&buf)

	// Encoding the map
    err := e.Encode(self.Segments)
	return buf.Bytes(), err
}
