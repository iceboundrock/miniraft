// Command raftctl is the Mini-Raft client CLI (SET / GET / status). It is
// implemented by a later issue; this placeholder only prints usage.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "raftctl: not implemented yet (see issue #12)")
	os.Exit(2)
}
