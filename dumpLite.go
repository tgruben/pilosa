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
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type LiteFile struct {
	db   *sql.DB
	stmt *sql.Stmt
	txn  *sql.Tx
	base string
}

func NewLiteFile(path string) (*LiteFile, error) {
	err := os.MkdirAll(path, 0770)
	p := &LiteFile{base: path}
	return p, err
}
func (b *LiteFile) Init() (err error) {
	sqlStmt := `
	create table bitmap(
key INTEGER NOT NULL,
roaring blob,
PRIMARY KEY ( key)
);`
	_, err = b.db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
	return
}

func (b *LiteFile) CreateFile(path string) error {
	fmt.Println("CREATE", path)
	db, err := sql.Open("sqlite3", b.base+"/"+path)
	fmt.Println("CHECK")
	if err != nil {
		return err
	}
	b.db = db

	//if initial {
	//	fmt.Println("Initialize")
	fmt.Println("INIT")
	err = b.Init()
	if err != nil {
		return err
	}
	//}
	fmt.Println("PREPARE")
	stmt, err := db.Prepare("insert into bitmap(key, roaring) values( ?, ?)")
	if err != nil {
		return err
	}
	b.stmt = stmt
	fmt.Println("GO")
	db.Exec("PRAGMA synchronous = OFF")
	db.Exec("PRAGMA journal_mode = MEMORY")
	if err != nil {
		return err
	}

	return nil
}
func (b *LiteFile) Begin() error {
	if true {
		return nil
	}
	ctx := context.Background()
	txn, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

	//	txn, err := b.db.Begin()
	if err != nil {

		return err
	}
	b.txn = txn
	return nil
}
func (b *LiteFile) Commit() error {
	if true {
		return nil
	}
	b.txn.Commit()
	return nil
}

func (b *LiteFile) Add(key, bitmap []byte) error {
	ikey := binary.LittleEndian.Uint64(key)
	_, err := b.stmt.Exec(int64(ikey), bitmap)
	return err
}

func (b *LiteFile) Close() error {
	b.db.Close()
	return nil
}
