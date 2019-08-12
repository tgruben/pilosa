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
	"encoding/binary"
	"os"

	"github.com/tgruben/hashdb"
)

type IndexFile struct {
	idx    *hashdb.Db
	wf     *os.File
	data   *bufio.Writer
	offset int
	base   string
}

func NewIndexFile(path string) (*IndexFile, error) {
	idxf := &IndexFile{
		base: path,
	}
	err := os.MkdirAll(idxf.base, 0770)
	return idxf, err
}

func (i *IndexFile) CreateFile(rpath string) error {
	path := i.base + "/" + rpath
	idx, err := hashdb.OpenIndex(path + ".hf")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path+".data", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(
		file,
		4096*2,
	)
	i.data = w
	i.idx = idx
	return nil
}

func (i *IndexFile) Begin() error {
	return nil
}
func (i *IndexFile) Commit() error {
	return nil
}
func (i *IndexFile) Add(bkey, bitmap []byte) error {
	key := binary.LittleEndian.Uint64(bkey)
	i.idx.Upsert(&hashdb.Entry{
		Key:    key,
		Offset: uint64(i.offset),
		Length: int32(len(bitmap)),
	})
	i.offset += len(bitmap)
	_, err := i.data.Write(bitmap)
	return err
}
func (m *IndexFile) Close() error {
	m.data.Flush()
	m.wf.Close()
	m.idx.Close()
	return nil
}

/*
func (m *IndexFile) Fetch(index, field, view string, shard, row uint64) *roaring.Bitmap {
	fmt.Println("FETCH", index, field, view, shard, row)
	key := MakeIndexKey(index, field, view, shard, row)
	fmt.Println("key", key)

	r, found := m.idx.Fetch(key)
	bitmap := roaring.NewBitmap()
	if found {
		fmt.Println("if", r.Offset, r.Length)
		m.wf.Seek(int64(r.Offset), 0)
		stream := make([]byte, 2)
		io.ReadFull(m.wf, stream)

		fmt.Println("UMBINARY")
		bitmap.UnmarshalBinary(stream)
	}
	return bitmap
}
*/
