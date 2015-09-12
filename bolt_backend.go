package enki

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"os"
	"path"
	willfbloom "github.com/willf/bloom"
)


// 14MB is the optimal size for 1MB of entries with false positive
// rate of 0.001 (and 10 is the optimal number of functions) 1MB
// of entries referring to block of 64KB is equivalent to 512GB
const BLOOMSIZE = uint(14*2<<22)
const NBFUNC = uint(10)

type BoltBackend struct {
	bloomFilter *Bloom
	blockFile *os.File
	db *bolt.DB
	dotDir *string
	signatureBucket *bolt.Bucket
	stateBucket *bolt.Bucket
	strongBucket *bolt.Bucket
	tx *bolt.Tx
}


func NewBoltBackend(dotDir string) Backend {
	// Create bloom
	bloomFilter := NewBloom()
	bloomPath := path.Join(dotDir, "bloom.gob")
	if _, err := os.Stat(bloomPath); err == nil {
		bloomData, err := ioutil.ReadFile(bloomPath)
		check(err)
		err = bloomFilter.GobDecode(bloomData)
		check(err)
	}

	// Create file containing blocks
	var blockFile *os.File
	var err error
	blockPath := path.Join(dotDir, "blocks.blob")
	if _, err = os.Stat(blockPath); err == nil {
		blockFile, err = os.OpenFile(blockPath, os.O_RDWR|os.O_APPEND, 0660)
	} else {
		blockFile, err = os.Create(blockPath)
	}
	check(err)

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
		bloomFilter,
		blockFile,
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
	bloomPath := path.Join(*self.dotDir, "bloom.gob")
	fd, err := os.Create(bloomPath)
	check(err)
	data, err := self.bloomFilter.GobEncode()
	fd.Write(data)
	fd.Close()
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
	info, err := self.blockFile.Stat()
	check(err)
	position := make([]byte, 8)
	binary.LittleEndian.PutUint64(position, uint64(info.Size()))
	self.strongBucket.Put(strong[:], position)
	self.blockFile.Seek(0, 2)

	// Write block size
	block_size := make([]byte, 4)
	binary.LittleEndian.PutUint32(block_size, uint32(len(data)))
	_, err = self.blockFile.Write(block_size)
	check(err)
	// Write block
	_, err = self.blockFile.Write(data)
	check(err)

	// Update bloom filter
	self.bloomFilter.Add(weak)
}

func (self *BoltBackend) ReadStrong(strong *StrongHash) Block {
	// Seek to the position stored in strongbucket
	bpos := self.strongBucket.Get(strong[:])
	if bpos == nil {
		return nil
	}
	position := binary.LittleEndian.Uint64(bpos)
	self.blockFile.Seek(int64(position), 0)

	// The first 4 bytes encode the size of the following block
	value := make([]byte, 4)
	_, err := self.blockFile.Read(value)
	check(err)
	size := binary.LittleEndian.Uint32(value)

	// Read the actual data
	value = make([]byte, size)
	_, err = self.blockFile.Read(value)
	check(err)

	return value
}

func (self *BoltBackend) SearchWeak(weak WeakHash) bool {
	return self.bloomFilter.Test(weak)
}

func (self *BoltBackend) ReadSignature(checksum []byte) *Signature {
	sgn := &Signature{}
	data := self.signatureBucket.Get(checksum)
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
	self.signatureBucket.Put(checksum, data)
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


type Bloom struct {
	bf *willfbloom.BloomFilter
}

func (self *Bloom) Add(weak WeakHash) {
	weakb := make([]byte, 4)
	binary.LittleEndian.PutUint32(weakb, uint32(weak))
	self.bf.Add(weakb)
}

func (self *Bloom) Test(weak WeakHash) bool {
	weakb := make([]byte, 4)
	binary.LittleEndian.PutUint32(weakb, uint32(weak))
	return self.bf.Test(weakb)
}

func NewBloom() *Bloom {
	bloom := &Bloom{}
	bloom.bf = willfbloom.New(BLOOMSIZE, NBFUNC)
	return bloom
}

func (self *Bloom) GobDecode(data []byte) (error) {
	return self.bf.GobDecode(data)
}

func (self *Bloom) GobEncode() ([]byte, error) {
	return self.bf.GobEncode()
}
