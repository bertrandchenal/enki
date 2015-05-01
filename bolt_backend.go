package enki

import (
	"github.com/boltdb/bolt"
	"io/ioutil"
	"os"
	"path"
)

type BoltBackend struct {
	bloomFilter *Bloom
	db *bolt.DB
	dotDir *string
	signatureBucket *bolt.Bucket
	strongBucket *bolt.Bucket
	tx *bolt.Tx
}

func NewBoltBackend(dotDir string) Backend {
	// Create bloom
	bloomFilter := NewBloom()

	bloomPath := path.Join(dotDir, "bloom.gob")
	bloomData, err := ioutil.ReadFile(bloomPath)
	_, is_path_error := err.(*os.PathError)
	if !is_path_error {
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
	strongBucket, err := tx.CreateBucketIfNotExists([]byte("strong"))
	check(err)

	backend := &BoltBackend{
		bloomFilter,
		db,
		&dotDir,
		signatureBucket,
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
	value := self.strongBucket.Get(strong[:])
	if value == nil {
		self.bloomFilter.Add(weak)
		self.strongBucket.Put(strong[:], data)
	}
}

func (self *BoltBackend) GetStrong(strong *StrongHash) (Block, bool) {
	value := self.strongBucket.Get(strong[:])
	return value, value != nil
}

func (self *BoltBackend) SearchWeak(weak WeakHash) bool {
	return self.bloomFilter.Test(weak)
}

func (self *BoltBackend) GetSignature(id string) (*Signature, bool) {
	sgn := &Signature{}
	data := self.signatureBucket.Get([]byte(id))
	err := sgn.GobDecode(data)
	check(err)
	return sgn, true
}

func (self *BoltBackend) SetSignature(id string, sgn *Signature) {
	key := []byte(id)
	data, err := sgn.GobEncode()
	check(err)
	self.signatureBucket.Put(key, data)
}
