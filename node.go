package main

import (
	"errors"
	"log"
)

var (
	ErrNonPositiveAmount = errors.New("amount must be a positive integer")
	ErrUnknownPeer       = errors.New("unknown peer node")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// TransferRequest asks a node to debit its balance and enqueue a transfer.
// The reply channel reports acceptance (nil) or the rejection reason.
type TransferRequest struct {
	ID     string
	To     int
	Amount int64
	Reply  chan error
}

// StartSnapshot is the coordinator's one and only intervention: it asks the
// initiator node to record its local state. Everything else propagates
// purely through markers on the existing channels.
type StartSnapshot struct{ SnapID int }

// balanceRequest is a read-only probe used by the debug endpoint. It is
// observability only and never feeds snapshot computation.
type balanceRequest struct{ Reply chan int64 }

// Report is a node's recorded local cut for one snapshot: its balance at
// record time plus, per in-channel, the transfers received after recording
// and before that channel's marker.
type Report struct {
	SnapID   int
	Node     int
	Balance  int64
	InFlight map[int][]Transfer // key: sender node id
}

// recording is the per-node Chandy–Lamport state for the active snapshot.
type recording struct {
	sid      int
	balance  int64              // local state, captured once
	inFlight map[int][]Transfer // channel state, key: sender node id
	open     map[int]bool       // in-channels still waiting for their marker
}

// Node is one logical participant. Its balance is private: it is touched
// only inside the node's own event loop, never read by other nodes and never
// exposed to the coordinator for snapshot computation.
type Node struct {
	id      int
	balance int64
	peers   []int
	out     map[int]*Channel

	inbox    chan Message
	reqs     chan TransferRequest
	ctrl     chan StartSnapshot
	bal      chan balanceRequest
	reportCh chan<- Report
	logger   *log.Logger

	rec *recording // non-nil while this node is recording a snapshot

	stop chan struct{}
	done chan struct{}
}

func newNode(id int, balance int64, peers []int, reportCh chan<- Report, logger *log.Logger) *Node {
	return &Node{
		id:       id,
		balance:  balance,
		peers:    peers,
		out:      make(map[int]*Channel),
		inbox:    make(chan Message),
		reqs:     make(chan TransferRequest, 64),
		ctrl:     make(chan StartSnapshot, 4),
		bal:      make(chan balanceRequest),
		reportCh: reportCh,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// run is the node's single message loop. Because every state change happens
// here sequentially, "debit + enqueue" and "record + emit markers" are each
// atomic with respect to the node — no locks on the balance are needed.
func (n *Node) run() {
	defer close(n.done)
	for {
		select {
		case <-n.stop:
			return
		case m := <-n.inbox:
			n.handleMessage(m)
		case r := <-n.reqs:
			n.handleTransfer(r)
		case s := <-n.ctrl:
			n.handleStart(s)
		case q := <-n.bal:
			q.Reply <- n.balance
		}
	}
}

// handleTransfer performs the send side: debit and enqueue as ONE atomic
// action of this node. If the node has already recorded its state for the
// active snapshot, its markers are already queued on every out-channel, so
// this transfer is necessarily ordered after them (post-cut).
func (n *Node) handleTransfer(r TransferRequest) {
	switch {
	case r.Amount <= 0:
		r.Reply <- ErrNonPositiveAmount
		return
	case n.out[r.To] == nil:
		r.Reply <- ErrUnknownPeer
		return
	case n.balance < r.Amount:
		r.Reply <- ErrInsufficientFunds
		return
	}
	n.balance -= r.Amount
	n.out[r.To].Enqueue(Transfer{ID: r.ID, From: n.id, To: r.To, Amount: r.Amount})
	r.Reply <- nil
}

// handleMessage performs the receive side: a transfer is credited only upon
// receipt. If the in-channel it arrived on is still recording, the transfer
// belongs to that channel's state (it was sent before the sender's cut).
func (n *Node) handleMessage(m Message) {
	switch msg := m.(type) {
	case Transfer:
		if n.rec != nil && n.rec.open[msg.From] {
			n.rec.inFlight[msg.From] = append(n.rec.inFlight[msg.From], msg)
		}
		n.balance += msg.Amount
	case Marker:
		n.handleMarker(msg)
	}
}

// handleMarker implements the marker-receiving rule.
func (n *Node) handleMarker(m Marker) {
	switch {
	case n.rec == nil:
		// First marker for this snapshot: record local state NOW, enqueue
		// markers on all out-channels before any further transfer, and treat
		// the marker's channel as recorded-empty.
		n.startRecording(m.SnapID, m.From)
	case n.rec.sid != m.SnapID:
		// Cannot happen while only one snapshot is active; logged loudly
		// instead of silently mixing records of two snapshots.
		n.logger.Printf("node %d: ignoring marker for snapshot %d while recording %d", n.id, m.SnapID, n.rec.sid)
		return
	case !n.rec.open[m.From]:
		n.logger.Printf("node %d: duplicate marker from %d for snapshot %d", n.id, m.From, m.SnapID)
		return
	default:
		delete(n.rec.open, m.From)
	}
	n.maybeReport()
}

// handleStart lets the initiator record spontaneously.
func (n *Node) handleStart(s StartSnapshot) {
	if n.rec != nil {
		n.logger.Printf("node %d: snapshot %d already recording, ignoring start for %d", n.id, n.rec.sid, s.SnapID)
		return
	}
	n.startRecording(s.SnapID, -1)
	n.maybeReport()
}

// startRecording captures the local balance and immediately enqueues a
// marker on every out-channel — all inside this single loop iteration, so no
// later transfer can overtake a marker on its channel. firstMarkerFrom is
// the in-channel the first marker arrived on (-1 for the initiator); that
// channel's state is empty by definition.
func (n *Node) startRecording(sid, firstMarkerFrom int) {
	n.rec = &recording{
		sid:      sid,
		balance:  n.balance,
		inFlight: make(map[int][]Transfer),
		open:     make(map[int]bool),
	}
	for _, p := range n.peers {
		if p != firstMarkerFrom {
			n.rec.open[p] = true
		}
	}
	for _, p := range n.peers {
		n.out[p].Enqueue(Marker{SnapID: sid, From: n.id, To: p})
	}
}

// maybeReport finishes the node's part of the snapshot once every in-channel
// has seen its marker. The report goes to the coordinator over a dedicated
// channel that is NOT part of the snapshotted network.
func (n *Node) maybeReport() {
	if n.rec == nil || len(n.rec.open) > 0 {
		return
	}
	rep := Report{SnapID: n.rec.sid, Node: n.id, Balance: n.rec.balance, InFlight: n.rec.inFlight}
	n.rec = nil
	select {
	case n.reportCh <- rep:
	case <-n.stop:
	}
}

func (n *Node) shutdown() {
	close(n.stop)
	<-n.done
}
