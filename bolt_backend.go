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
}

func (self *BoltBackend) Abort() {
	self.tx.Rollback()
}

func (self *BoltBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	// FIXME data should be stored in a 'blob of blob' file, not in the index
	value := self.strongBucket.Get(strong[:])
	if value == nil {
		self.bloomFilter.Add(weak)
		self.strongBucket.Put(strong[:], data)
	}
}

func (self *BoltBackend) GetStrong(strong *StrongHash) Block {
	value := self.strongBucket.Get(strong[:])
	return value
}

func (self *BoltBackend) SearchWeak(weak WeakHash) bool {
	return self.bloomFilter.Test(weak)
}

func (self *BoltBackend) GetSignature(checksum []byte) *Signature {
	sgn := &Signature{}
	data := self.signatureBucket.Get(checksum)
	if data == nil {
		return nil
	}
	err := sgn.GobDecode(data)
	check(err)
	return sgn
}

func (self *BoltBackend) SetSignature(checksum []byte, sgn *Signature) {
	data, err := sgn.GobEncode()
	check(err)
	self.signatureBucket.Put(checksum, data)
}

func (self *BoltBackend) GetState(timestamp int64) *DirState {
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

func (self *BoltBackend) SetState(state *DirState) {
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
