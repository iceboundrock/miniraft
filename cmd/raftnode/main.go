// Command raftnode runs a single Mini-Raft node. The real node process is
// implemented by a later issue; this placeholder only prints usage.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "raftnode: not implemented yet (see issue #12)")
	os.Exit(2)
}
