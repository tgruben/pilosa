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
	"bufio"
	"bytes"
	"fmt"
	"path/filepath"
	"time"
)

type IndexWriter interface {
	CreateFile(path string) error
	Add(key, value []byte) error
	Begin() error
	Commit() error
	Close() error
}

type Migrater struct {
	holder *Holder
	index  IndexWriter
}

func NewMigrater(holderPath string, iw IndexWriter) *Migrater {
	holder := NewHolder()
	holder.Path = holderPath
	holder.translateFile.Path = filepath.Join(holderPath, ".keys")
	fmt.Println("HOLDER OPEN")
	start := time.Now()
	if err := holder.Open(); err != nil {
		fmt.Println("Problems", err)
		return nil

	}
	fmt.Println("HOLDER COMPLETE", time.Now().Sub(start))
	o := &Migrater{}
	o.holder = holder
	o.index = iw
	return o
}
func (ml *Migrater) Close() {
	ml.holder.Close()
}

func (ml *Migrater) Open() (err error) {
	return nil
}

func (ml *Migrater) Migrate() {
	var b bytes.Buffer // A Buffer needs no initialization.
	writer := bufio.NewWriter(&b)
	progress := 0
	startingTime := time.Now().UTC()
	buff := make([]byte, 8)
	for _, index := range ml.holder.indexes {
		ml.index.CreateFile(index.Name())
		key := uint64(1)
		for _, field := range index.fields {
			for _, view := range field.viewMap {
				fmt.Println(index.Name(), field.Name(), view.name, len(view.fragments))
				for _, fragment := range view.fragments {
					ml.index.Begin()
					for _, row := range fragment.rows(0) {
						data := fragment.storage.OffsetRange(fragment.shard*ShardWidth, row*ShardWidth, (row+1)*ShardWidth)
						data.WriteTo(writer)
						writer.Flush()
						//key := MakeIndexKeyBytes(index.Name(), field.Name(), view.name, fragment.shard, row)
						GetBytes(key, buff)
						key++
						err := ml.index.Add(buff, b.Bytes())
						if err != nil {
							fmt.Println("Add", err)
							return

						}
						b.Reset()
						progress++
						if progress%100000 == 0 {
							ml.index.Commit()
							ml.index.Begin()
							now := time.Now()
							fmt.Println("Progress:", progress, now.Sub(startingTime))
							startingTime = now
						}
					}
					ml.index.Commit()
					fragment.Close()
				}
			}
		}
		ml.index.Close()

	}
}

func GetBytes(key uint64, fix []byte) {
	fix[7] = byte(key)
	fix[6] = byte(key >> 8)
	fix[5] = byte(key >> 16)
	fix[4] = byte(key >> 24)
	fix[3] = byte(key >> 32)
	fix[2] = byte(key >> 40)
	fix[1] = byte(key >> 48)
	fix[0] = byte(key >> 56)
}

func MakeIndexKeyBytes(index, field, view string, shard, row uint64) []byte {
	var buffer bytes.Buffer
	fix := make([]byte, 16)
	fix[15] = byte(shard)
	fix[14] = byte(shard >> 8)
	fix[13] = byte(shard >> 16)
	fix[12] = byte(shard >> 24)
	fix[11] = byte(shard >> 32)
	fix[10] = byte(shard >> 40)
	fix[9] = byte(shard >> 48)
	fix[8] = byte(shard >> 56)
	fix[7] = byte(row)
	fix[6] = byte(row >> 8)
	fix[5] = byte(row >> 16)
	fix[4] = byte(row >> 24)
	fix[3] = byte(row >> 32)
	fix[2] = byte(row >> 40)
	fix[1] = byte(row >> 48)
	fix[0] = byte(row >> 56)
	buffer.Write(fix)
	buffer.Write([]byte(view))
	buffer.Write([]byte(field))
	buffer.Write([]byte(index))
	return buffer.Bytes()
}

type NullDB struct{}

func (nb *NullDB) CreateFile(path string) error {
	return nil
}

func (nb *NullDB) Add(key, value []byte) error {
	return nil
}
func (nb *NullDB) Begin() error {
	return nil
}
func (nb *NullDB) Commit() error {
	return nil
}
func (nb *NullDB) Close() error {
	return nil
}
