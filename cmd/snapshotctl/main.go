// Command snapshotctl inspects snapshot-kv stores.
//
// Usage:
//
//	snapshotctl --dir <path> stats
//
// stats prints one compact JSON line, terminated by a newline:
//
//	{"keys":3,"snapshots":2,"avgBytes":4.67}
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Hulalalalalalalalalalala/snapshot-kv"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

func run(args []string, stdout io.Writer) int {
	dir, cmd, ok := parseArgs(args)
	if !ok || cmd != "stats" {
		return 1
	}

	// stats inspects an existing store: a missing path or a non-directory
	// cannot be opened, so exit 1 silently instead of creating anything.
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return 1
	}

	store, err := snapshot.Open(dir)
	if err != nil {
		return 1
	}
	defer store.Close()

	st := store.Stats()
	// Fixed key order keys, snapshots, avgBytes; no spaces after colons or
	// commas. %.2f always emits exactly two decimals and never "-0" for the
	// non-negative values Stats produces.
	_, err = fmt.Fprintf(stdout, "{\"keys\":%d,\"snapshots\":%d,\"avgBytes\":%.2f}\n",
		st.Keys, st.Snapshots, st.AvgBytes)
	if err != nil {
		return 1
	}
	return 0
}

// parseArgs accepts "--dir <path> stats" (or "--dir=<path>").
func parseArgs(args []string) (dir, cmd string, ok bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--dir" || a == "-dir":
			if i+1 >= len(args) {
				return "", "", false
			}
			dir = args[i+1]
			i++
		case len(a) > 6 && a[:6] == "--dir=":
			dir = a[6:]
		default:
			if a == "stats" && cmd == "" {
				cmd = a
			} else {
				return "", "", false
			}
		}
	}
	if dir == "" || cmd == "" {
		return "", "", false
	}
	return dir, cmd, true
}
