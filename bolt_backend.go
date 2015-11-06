package enki

import (
	"compress/gzip"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/boltdb/bolt"
	"os"
	"path"
)

// 14MB is the optimal size for 1MB of entries with false positive
// rate of 0.001 (and 10 is the optimal number of functions) 1MB
// of entries referring to block of 64KB is equivalent to 512GB

type BoltBackend struct {
	weakMap         map[WeakHash]bool
	blockFile       *os.File
	sigFile         *os.File
	db              *bolt.DB
	dotDir          *string
	signatureBucket *bolt.Bucket
	stateBucket     *bolt.Bucket
	strongBucket    *bolt.Bucket
	tx              *bolt.Tx
}

func NewBoltBackend(dotDir string) Backend {
	// Create weakMap
	weakMap := make(map[WeakHash]bool)
	mapPath := path.Join(dotDir, "weakmap.gob")
	if fd, err := os.Open(mapPath); err == nil {
		dec := gob.NewDecoder(fd)
		if err := dec.Decode(&weakMap); err != nil {
			check(err)
		}
	}

	// Create file containing blocks
	var blockFile = CreateFile(dotDir, "blocks.blob")
	var sigFile = CreateFile(dotDir, "sigs.blob")

	// Create db
	dbPath := path.Join(dotDir, "indexes.bolt")
	db, err := bolt.Open(dbPath, 0600, nil)
	check(err)

	// Create buckets
	tx, err := db.Begin(true)
	check(err)
	signatureBucket, err := tx.CreateBucketIfNotExists([]byte("signature"))
	check(err)
	stateBucket, err := tx.CreateBucketIfNotExists([]byte("state"))
	check(err)
	strongBucket, err := tx.CreateBucketIfNotExists([]byte("strong"))
	check(err)

	backend := &BoltBackend{
		weakMap,
		blockFile,
		sigFile,
		db,
		&dotDir,
		signatureBucket,
		stateBucket,
		strongBucket,
		tx,
	}
	return backend
}

func (self *BoltBackend) Close() {
	check(self.tx.Commit())
	check(self.db.Close())
	mapPath := path.Join(*self.dotDir, "weakmap.gob")
	fd, err := os.Create(mapPath)
	check(err)
	defer fd.Close()
	enc := gob.NewEncoder(fd)
	check(enc.Encode(self.weakMap))
	self.blockFile.Close()
}

func (self *BoltBackend) Abort() {
	self.tx.Rollback()
}

func (self *BoltBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	value := self.strongBucket.Get(strong[:])
	if value != nil {
		// Block already known, nothing to do
		return
	}

	WriteIndex(self.strongBucket, self.blockFile, strong[:], data)

	// Update map
	self.weakMap[weak] = true
}

func (self *BoltBackend) ReadStrong(strong *StrongHash) Block {
	value := ReadIndex(self.strongBucket, self.blockFile, strong[:])
	return value
}

func (self *BoltBackend) SearchWeak(weak WeakHash) bool {
	return self.weakMap[weak]
}

func (self *BoltBackend) ReadSignature(checksum []byte) *Signature {
	sgn := &Signature{}
	data := ReadIndex(self.signatureBucket, self.sigFile, checksum)
	if data == nil {
		return nil
	}
	err := sgn.GobDecode(data)
	check(err)
	return sgn
}

func (self *BoltBackend) WriteSignature(checksum []byte, sgn *Signature) {
	data, err := sgn.GobEncode()
	check(err)
	WriteIndex(self.signatureBucket, self.sigFile, checksum, data)
}

func (self *BoltBackend) ReadState(timestamp int64) *DirState {
	var data []byte
	var foundkey []byte
	cursor := self.stateBucket.Cursor()
	if timestamp == MAXTIMESTAMP {
		_, data = cursor.Last()
	} else {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(timestamp))
		foundkey, data = cursor.Seek(key)
		if !bytes.Equal(key, foundkey) {
			_, data = cursor.Prev()
		}
	}
	if data == nil {
		return nil
	}
	state := &DirState{}
	state.GobDecode(data)
	return state
}

func (self *BoltBackend) WriteState(state *DirState) {
	// Key is encoded with big endianess to preserve ordering
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(state.Timestamp))
	data := state.GobEncode()
	self.stateBucket.Put(key, data)
}


func CreateFile(dir string, name string) *os.File {
	var err error
	var file *os.File
	filePath := path.Join(dir, name)
	if _, err = os.Stat(filePath); err == nil {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0660)
	} else {
		file, err = os.Create(filePath)
	}
	check(err)
	return file
}

func WriteIndex(bucket *bolt.Bucket, file *os.File, key []byte, data []byte) {
	// Write the size of (gzipped) data and (gzipped) data  at the end of
	// file. Take the offset of data in the file and put it in the
	// bucket under the given key

	// Find position and update bucket
	info, err := file.Stat()
	check(err)
	position := make([]byte, 8)
	binary.LittleEndian.PutUint64(position, uint64(info.Size()))
	bucket.Put(key, position)
	file.Seek(0, 2)

	// Write data size
	data_size := make([]byte, 4)
	binary.LittleEndian.PutUint32(data_size, uint32(len(data)))
	_, err = file.Write(data_size)
	check(err)

	// Zip+Write data
	zip_writer := gzip.NewWriter(file)
	_, err = zip_writer.Write(data)
	zip_writer.Close()
	check(err)
}

func ReadIndex(bucket *bolt.Bucket, file *os.File, key []byte) []byte {
	// Seek to the position stored in strongbucket
	bpos := bucket.Get(key)
	if bpos == nil {
		return nil
	}
	position := binary.LittleEndian.Uint64(bpos)
	file.Seek(int64(position), 0)

	// The first 4 bytes encode the size of the following block
	value := make([]byte, 4)
	_, err := file.Read(value)
	check(err)
	size := binary.LittleEndian.Uint32(value)

	data := make([]byte, size)
	zip_reader, err := gzip.NewReader(file)
	check(err)
	_, err = zip_reader.Read(data)
	check(err)
	return data
}
