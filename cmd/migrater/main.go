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

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/pilosa/pilosa/syswrap"

	"github.com/pilosa/pilosa"
)

func main() {
	syswrap.SetMaxFileCount(900000)
	syswrap.SetMaxMapCount(900000)
	var b pilosa.IndexWriter
	if len(os.Args) != 4 {
		fmt.Println("usage ./migrater PILOSA_DATA BACKEND BACKEND_PATH")
		fmt.Println("BACKENDS:")
		fmt.Println("bolt")
		fmt.Println("badger")
		fmt.Println("aran")
		fmt.Println("sqlite")
		fmt.Println("lmdb")
		fmt.Println("diskh")
	}
	switch os.Args[2] {
	case "bolt":
		b, _ = pilosa.NewBoltFile(os.Args[3])
	case "badger":
		b, _ = pilosa.NewBadgerFile(os.Args[3])
	case "aran":
		b, _ = pilosa.NewAranFile(os.Args[3])
	case "sqlite":
		b, _ = pilosa.NewLiteFile(os.Args[3])
	case "lmdb":
		b, _ = pilosa.NewLmdbFile(os.Args[3])
	case "diskh":
		b, _ = pilosa.NewIndexFile(os.Args[3])
	default:
		fmt.Println("using /dev/null")
		b = &pilosa.NullDB{}
	}
	startingTime := time.Now()
	fp := pilosa.NewMigrater(os.Args[1], b)
	fmt.Println("Open")
	if err := fp.Open(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Migrate")
	fp.Migrate()
	fmt.Println("Close")
	fp.Close()
	now := time.Now()
	fmt.Println("Elapsed:", now.Sub(startingTime))

}
