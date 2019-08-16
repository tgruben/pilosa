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
	"os"

	"github.com/tgruben/aran"
)

type AranFile struct {
	db   *aran.Db
	base string
}

func NewAranFile(path string) (*AranFile, error) {
	// Open the Bolt database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	b := &AranFile{
		base: path,
	}
	return b, nil
}

func (b *AranFile) CreateFile(path string) error {
	opts := aran.DefaultOptions()
	opts.Path = b.base + "/" + path
	fmt.Println("CreateFile", opts.Path)
	err := os.MkdirAll(opts.Path, 0770)
	if err != nil {
		fmt.Println("WHAT MKDIR", err)
		return err
	}
	d, err := aran.New(opts)
	if err != nil {
		fmt.Println("WHAT NEW", err)
		return err
	}
	b.db = d
	return nil
}

func (b *AranFile) Begin() error {
	return nil
}
func (b *AranFile) Commit() error {
	return nil
}
func (b *AranFile) Add(key, bitmap []byte) (err error) {
	//b.db.Set(key, bitmap)
	b.db.Set(key, bitmap)
	return nil
}
func (b *AranFile) Close() error {
	b.db.Close()
	return nil
}
