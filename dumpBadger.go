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
	"log"
	"os"

	badger "github.com/dgraph-io/badger"
)

type MigrateBadger struct {
	holder *Holder
	index  *BadgerFile
	base   string
}

type BadgerFile struct {
	db   *badger.DB
	txn  *badger.Txn
	base string
}

func NewBadgerFile(path string) (*BadgerFile, error) {
	b := &BadgerFile{}
	b.base = path
	err := os.MkdirAll(b.base, 0770)
	return b, err
}

func (b *BadgerFile) CreateFile(path string) error {
	db, err := badger.Open(badger.DefaultOptions(b.base + "/" + path))
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		return err
	}
	b.db = db
	return nil
}

func (b *BadgerFile) Begin() error {
	b.txn = b.db.NewTransaction(true)
	return nil
}
func (b *BadgerFile) Commit() error {
	b.txn.Commit()
	return nil
}

func (b *BadgerFile) Add(key, bitmap []byte) (err error) {
	return b.txn.Set(key, bitmap)
}
func (b *BadgerFile) Close() error {
	return b.db.Close()
}
