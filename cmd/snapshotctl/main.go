// Command snapshotctl inspects a snapshot-kv store directory.
//
//	snapshotctl --dir <path> stats
//
// stats prints one compact JSON line:
//
//	{"keys":N,"snapshots":M,"avgBytes":X.XX}
//
// and exits 1 without printing anything if the directory cannot be
// opened.
package main

import (
	"flag"
	"fmt"
	"os"

	snapshot "github.com/Hulalalalalalalalalalala/snapshot-kv"
)

func main() {
	dir := flag.String("dir", "", "store directory")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 || args[0] != "stats" || *dir == "" {
		fmt.Fprintln(os.Stderr, "usage: snapshotctl --dir <path> stats")
		os.Exit(2)
	}

	keys, snaps, total, err := snapshot.Stats(*dir)
	if err != nil {
		os.Exit(1)
	}

	// avgBytes rounded half away from zero to two decimals, in integer
	// math so no negative zero, NaN or Inf can ever be printed.
	var cents uint64
	if keys > 0 {
		k := uint64(keys)
		cents = (total*1000/k + 5) / 10
	}
	fmt.Printf("{\"keys\":%d,\"snapshots\":%d,\"avgBytes\":%d.%02d}\n",
		keys, snaps, cents/100, cents%100)
}
