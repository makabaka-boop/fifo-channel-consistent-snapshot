package main

import (
	"log"
	"os"
)

// Cluster wires N nodes (3 in this experiment) with N*(N-1) reliable FIFO
// directed channels — six for three nodes — and starts every message loop.
type Cluster struct {
	Nodes    []*Node
	channels map[[2]int]*Channel

	// Reports carries snapshot reports from nodes to the coordinator. It is
	// out-of-band: never snapshotted, never delayed by test barriers.
	Reports chan Report

	total int64
}

func newCluster(balances []int64, logger *log.Logger) *Cluster {
	n := len(balances)
	cl := &Cluster{
		channels: make(map[[2]int]*Channel),
		Reports:  make(chan Report, 16),
	}
	for i, b := range balances {
		peers := make([]int, 0, n-1)
		for j := range balances {
			if j != i {
				peers = append(peers, j)
			}
		}
		cl.Nodes = append(cl.Nodes, newNode(i, b, peers, cl.Reports, logger))
		cl.total += b
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			ch := newChannel(i, j, cl.Nodes[j].inbox)
			cl.channels[[2]int{i, j}] = ch
			cl.Nodes[i].out[j] = ch
		}
	}
	for _, nd := range cl.Nodes {
		go nd.run()
	}
	return cl
}

// Channel returns the directed channel from -> to.
func (cl *Cluster) Channel(from, to int) *Channel { return cl.channels[[2]int{from, to}] }

// Total is the initial total supply of tokens, constant for the cluster's
// lifetime (transfers only move tokens, they never create or destroy them).
func (cl *Cluster) Total() int64 { return cl.total }

// HoldAll engages the test barrier on every directed channel.
func (cl *Cluster) HoldAll() {
	for _, ch := range cl.channels {
		ch.Hold()
	}
}

// ReleaseAll lifts the test barrier on every directed channel.
func (cl *Cluster) ReleaseAll() {
	for _, ch := range cl.channels {
		ch.Release()
	}
}

// Channels lists all directed channels (diagnostics).
func (cl *Cluster) Channels() []*Channel {
	out := make([]*Channel, 0, len(cl.channels))
	for i := range cl.Nodes {
		for j := range cl.Nodes {
			if i != j {
				out = append(out, cl.channels[[2]int{i, j}])
			}
		}
	}
	return out
}

func (cl *Cluster) Shutdown() {
	for _, nd := range cl.Nodes {
		nd.shutdown()
	}
	for _, ch := range cl.channels {
		ch.close()
	}
	close(cl.Reports)
}

func defaultLogger() *log.Logger { return log.New(os.Stderr, "", log.LstdFlags) }
