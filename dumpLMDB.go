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

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type LmdbFile struct {
	db   lmdb.DBI
	env  *lmdb.Env
	txn  *lmdb.Txn
	base string
}

func NewLmdbFile(path string) (*LmdbFile, error) {
	err := os.MkdirAll(path, 0770)
	l := &LmdbFile{
		base: path,
	}

	return l, err
}

func (b *LmdbFile) CreateFile(rpath string) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	err = env.SetMapSize(1 << 38)
	if err != nil {
		return err
	}
	path := b.base + "/" + rpath
	err = os.MkdirAll(path, 0770)
	err = env.Open(path, 0, 0644)
	if err != nil {
		return err
	}
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		return err
	}
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		fmt.Println("CREATEDBI")
		//dbi, err = txn.CreateDBI("example")
		dbi, err = txn.OpenRoot(0)
		fmt.Println("ERR", err)
		return err
		//db, err = txn.OpenRoot(0)
		//if err != nil {
		//	return err
		//	}
	})
	if err != nil {
		return err
	}
	b.db = dbi
	b.env = env
	return nil
}
func (b *LmdbFile) Begin() error {
	txn, err := b.env.BeginTxn(nil, 0)
	if err != nil {
		fmt.Println("BEGINE ERR", err)
		return err
	}
	b.txn = txn
	//	b.txn, err := env.BeginTxn(nil, 0)
	return nil
}
func (b *LmdbFile) Commit() error {
	return b.txn.Commit()
}

func (b *LmdbFile) Add(key, bitmap []byte) (err error) {
	//err = b.env.Update(func(txn *lmdb.Txn) (err error) {
	//err = b.txn.Put(b.db, key, bitmap, lmdb.NoOverwrite)
	err = b.txn.Put(b.db, key, bitmap, lmdb.Append) //assume keys in sorted order
	if err != nil {
		return err
	}
	return nil
}

func (b *LmdbFile) Close() error {
	b.env.Close()
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
