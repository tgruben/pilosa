// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

type BoltFile struct {
	db   *bolt.DB
	tx   *bolt.Tx
	base string
}

func NewBoltFile(path string) (*BoltFile, error) {
	// Open the Bolt database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	b := &BoltFile{
		base: path,
	}
	return b, nil
}
func (b *BoltFile) CreateFile(path string) error {
	err := os.MkdirAll(b.base, 0770)
	db, err := bolt.Open(b.base+"/"+path, 0644, nil)
	if err != nil {
		log.Fatal(err)
	}
	b.db = db
	return nil
}
func (b *BoltFile) Begin() error {
	var err error
	b.tx, err = b.db.Begin(true)
	return err
}
func (b *BoltFile) Commit() error {
	return b.tx.Commit()
}

func (b *BoltFile) Add(key, bitmap []byte) (err error) {
	bk, err := b.tx.CreateBucketIfNotExists([]byte("default"))
	if err != nil {
		fmt.Println("Add", err)
		return err
	}
	return bk.Put(key, bitmap)
}
func (b *BoltFile) Close() error {
	b.db.Close()
	return nil
}

/*
func (b *BadgeFile) Fetch(index, field, view string, shard, row uint64) *roaring.Bitmap {
	fmt.Println("FETCH", index, field, view, shard, row)
	key := MakeIndexKeyBytes(index, field, view, shard, row)
	fmt.Println("key", key)

	//r, found := m.idx.Fetch(key)
	bitmap := roaring.NewBitmap()
	//if found {
	//	fmt.Println("if", r.Offset, r.Length)
	//	m.wf.Seek(int64(r.Offset), 0)
	//	stream := make([]byte, 2)
	//	io.ReadFull(m.wf, stream)

	//fmt.Println("UMBINARY")
	//bitmap.UnmarshalBinary(stream)
	//}
	return bitmap
}
*/
