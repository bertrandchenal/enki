package enki

import (
	"compress/lzw"
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
	blockFile       *BlobFile
	sigFile         *BlobFile
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

	// Create blobfiles
	var blockFile = NewBlobFile(path.Join(dotDir, "blocks.blob"), strongBucket)
	var sigFile = NewBlobFile(path.Join(dotDir, "sigs.blob"), signatureBucket)

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
	self.sigFile.Close()
}

func (self *BoltBackend) Abort() {
	self.tx.Rollback()
}

func (self *BoltBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	// Store block
	self.blockFile.Write(strong[:], data)
	// Update map
	self.weakMap[weak] = true
}

func (self *BoltBackend) ReadStrong(strong *StrongHash) Block {
	return self.blockFile.Read(strong[:])
}

func (self *BoltBackend) SearchWeak(weak WeakHash) bool {
	return self.weakMap[weak]
}

func (self *BoltBackend) ReadSignature(checksum []byte) *Signature {
	sgn := &Signature{}
	data := self.sigFile.Read(checksum)
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
	self.sigFile.Write(checksum, data)
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


type BlobFile struct {
	file *os.File
	bucket *bolt.Bucket
	size uint64
}

func NewBlobFile(filePath string, bucket *bolt.Bucket) *BlobFile {
	var err error
	var size uint64
	var file *os.File
	if _, err = os.Stat(filePath); err == nil {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0660)
		info, err := file.Stat()
		check(err)
		size = uint64(info.Size())
	} else {
		file, err = os.Create(filePath)
		size = 0
	}
	check(err)
	return &BlobFile{file, bucket, size}
}

func (self *BlobFile) Write(key []byte, data []byte) {
	// Write the size of (zipped) data and (zipped) data  at the end of
	// file. Take the offset of data in the file and put it in the
	// bucket under the given key

	value := self.bucket.Get(key)
	if value != nil {
		// Key already known, nothing to do
		return
	}

	// Store future data position (current file size) in bucket
	position := make([]byte, 8)
	binary.LittleEndian.PutUint64(position, self.size)
	self.bucket.Put(key, position)

	// Jump to end of the file
	self.file.Seek(0, 2)

	// Write data size
	data_size := make([]byte, 4)
	binary.LittleEndian.PutUint32(data_size, uint32(len(data)))
	write_size, err := self.file.Write(data_size)
	if write_size != 4 {
		panic("how unexpected it is")
	}
	check(err)
	self.size += uint64(write_size)

	// Zip+Write data
	zip_writer := lzw.NewWriter(self.file, lzw.LSB, 8)
	write_size, err = zip_writer.Write(data)
	zip_writer.Close()
	check(err)
	self.size += uint64(write_size)
}

func (self *BlobFile) Read(key []byte) []byte {
	// Seek to the position stored in bucket
	bpos := self.bucket.Get(key)
	if bpos == nil {
		return nil
	}
	position := binary.LittleEndian.Uint64(bpos)
	self.file.Seek(int64(position), 0)

	// The first 4 bytes encode the size of the following block
	value := make([]byte, 4)
	_, err := self.file.Read(value)
	check(err)
	dataSize := binary.LittleEndian.Uint32(value)

	data := make([]byte, dataSize)
	zip_reader := lzw.NewReader(self.file, lzw.LSB, 8)
	_, err = zip_reader.Read(data)
	check(err)
	return data
}

func (self *BlobFile) Close() {
	self.file.Close()
}
