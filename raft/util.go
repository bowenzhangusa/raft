package raft

import (
	"log"
	"time"
	"math/rand"
)

// Debugging
const Debug = 0
// Heartbeats account for a majority of logs, so we enable them separately
const DebugHeartbeats = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Returns a timeout duration a follower is allowed to wait until starting election
func getElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(300)) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
