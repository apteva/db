// Command aptevadbctl inspects and maintains a database directory.
//
//	aptevadbctl version
//	aptevadbctl format <dir>                          on-disk format version, without opening
//	aptevadbctl [-c collection] stats <dir>
//	aptevadbctl verify <dir>
//	aptevadbctl collections <dir>
//	aptevadbctl [-c collection] get <dir> <key>
//	aptevadbctl [-c collection] dump <dir> [prefix]   keys and values, one JSON object per line
//	aptevadbctl [-c collection] agg <dir> '<pipeline json>' [shards]
//	aptevadbctl compact <dir>
//	aptevadbctl backup <dir> <dst-dir>
//
// Without -c, get, dump and agg address the default collection.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/apteva/aptevadb"
)

func main() {
	collName := ""
	if len(os.Args) > 2 && os.Args[1] == "-c" {
		collName = os.Args[2]
		os.Args = append(os.Args[:1], os.Args[3:]...)
	}
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Printf("aptevadbctl %s (format %d)\n", aptevadb.Version, aptevadb.FormatVersion)
		return
	}
	if len(os.Args) < 3 {
		usage()
	}
	cmd, dir := os.Args[1], os.Args[2]
	if cmd == "format" {
		v, err := aptevadb.FormatVersionOf(dir)
		if err != nil {
			fail(err)
		}
		state := "readable by this build"
		if v != aptevadb.FormatVersion {
			state = fmt.Sprintf("not readable by this build, which reads format %d", aptevadb.FormatVersion)
		}
		fmt.Printf("format %d: %s\n", v, state)
		return
	}
	db, err := aptevadb.Open(dir, aptevadb.Options{})
	if err != nil {
		fail(err)
	}
	defer db.Close()
	coll, err := db.Collection(collName)
	if err != nil {
		fail(err)
	}
	enc := json.NewEncoder(os.Stdout)
	switch cmd {
	case "stats":
		enc.SetIndent("", "  ")
		enc.Encode(db.Stats())
	case "collections":
		for _, n := range db.Collections() {
			c, _ := db.Collection(n)
			fmt.Printf("%s\tindexes=%d\n", n, len(c.Indexes()))
		}
	case "verify":
		if err := db.Verify(context.Background()); err != nil {
			fail(err)
		}
		fmt.Println("ok")
	case "get":
		if len(os.Args) < 4 {
			usage()
		}
		v, ok, err := coll.Find([]byte(os.Args[3]), nil)
		if err != nil {
			fail(err)
		}
		if !ok {
			fail(fmt.Errorf("not found"))
		}
		os.Stdout.Write(render(v))
		fmt.Println()
	case "dump":
		var lo, hi []byte
		if len(os.Args) > 3 {
			lo = []byte(os.Args[3])
			hi = append(append([]byte{}, lo...), 0xff)
		}
		err := coll.Scan(lo, hi, func(k, v []byte) bool {
			fmt.Printf("{\"key\":%s,\"value\":%s}\n", jsonString(k), render(v))
			return true
		})
		if err != nil {
			fail(err)
		}
	case "agg":
		if len(os.Args) < 4 {
			usage()
		}
		shards := runtime.GOMAXPROCS(0)
		if len(os.Args) > 4 {
			shards, _ = strconv.Atoi(os.Args[4])
		}
		rows, err := coll.AggregateParallel(context.Background(), []byte(os.Args[3]), shards)
		if err != nil {
			fail(err)
		}
		for _, r := range rows {
			enc.Encode(r)
		}
	case "compact":
		before := db.Stats().HighWaterPages
		if err := db.Compact(context.Background()); err != nil {
			fail(err)
		}
		fmt.Printf("pages %d -> %d\n", before, db.Stats().HighWaterPages)
	case "backup":
		if len(os.Args) < 4 {
			usage()
		}
		if err := db.Backup(context.Background(), os.Args[3]); err != nil {
			fail(err)
		}
		fmt.Println("ok")
	default:
		usage()
	}
}

// render shows a value as JSON when it decodes as a document, else base64.
func render(v []byte) []byte {
	if m, err := aptevadb.DecodeDocument(v); err == nil {
		if out, err := json.Marshal(m); err == nil {
			return out
		}
	}
	return []byte(`"` + base64.StdEncoding.EncodeToString(v) + `"`)
}

func jsonString(b []byte) string {
	out, _ := json.Marshal(string(b))
	return string(out)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "aptevadbctl:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: aptevadbctl version | format <dir> | [-c collection] stats|verify|collections|get|dump|agg|compact|backup <dir> [args]")
	os.Exit(2)
}
