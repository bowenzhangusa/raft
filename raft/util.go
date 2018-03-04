package raft

import (
	"log"
	"time"
	"math/rand"
)

// Debugging
const Debug = 0

var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func getElectionTimeout() time.Duration {
	return time.Duration(400+randSource.Intn(400)) * time.Millisecond
}
