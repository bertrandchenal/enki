package enki

import (
	"github.com/boltdb/bolt"
	"ioutils"
	"path"
)

type BoltBackend struct {
	bloomFilter *bloom.BloomFilter
	db bolt.DB
	signatureBucket *bolt.Bucket
	strongBucket *bolt.Bucket
	tx *bolt.Transaction
}

func NewBoltBackend(dotDir string) Backend {
	// Create bloom
	bloomPath := path.Join(dotDir, "bloom.gob")
	bloomData, err := ioutils.ReadFile(bloomPath)
	bloomFilter := BloomFromGob(bloomData)
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
	strongBucket, err := tx.CreateBucketIfNotExists([]byte("strong"))
	check(err)

	backend := &BoldBackend{
		bloomFilter,
		db,
		signatureBucket,
		strongBucket,
		tx,
	}
	return backend
}

func (self *BoltBackend) Close() {
	self.tx.Commit()
	self.db.Close()
}

func (self *BoltBackend) Abort() {
	self.tx.Rollback()
}

func (self *BoltBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	value := self.strongBucket.Get(*strong)
	if value == nil {
		self.bloomFilter.Add(weak)
		self.strongBucket.Put(*strong, data)
		println("NEW STRONG")
	} else {
		println("DUP!")
	}
}

func (self *MemoryBackend) GetStrong(strong *StrongHash) (Block, bool) {
	value := self.strongBucket.Get(*strong)
	return value, value != nil
}

func (self *MemoryBackend) SearchWeak(weak WeakHash) bool {
	return self.bloomFilter.Test(weak)
}

func (self *MemoryBackend) GetSignature(id string) (*Signature, bool) {
	sgn := self.SignatureBucket([]byte(id))
	return sgn, sgn != nil
}

func (self *MemoryBackend) SetSignature(id string, sgn *Signature) {
	self.SignatureBucket.Put([]byte(id),  sgn)
}
