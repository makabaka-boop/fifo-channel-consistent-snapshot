package main

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSnapshotInProgress = errors.New("another snapshot is still in progress")
	ErrSelfTransfer       = errors.New("source and target node must differ")
	ErrUnknownNode        = errors.New("unknown node id")
	ErrNodeUnavailable    = errors.New("node did not answer in time")
)

// Coordinator owns the HTTP-facing control plane. It may NOT freeze the
// network, read node balances, or reconstruct a snapshot from current
// balances — its only levers are: hand a transfer request to the sending
// node, tell the initiator to start a snapshot, and collect the reports the
// nodes produce on their own.
type Coordinator struct {
	cluster *Cluster

	mu     sync.Mutex
	active *Snapshot
	snaps  map[int]*Snapshot
	nextID int

	txSeq int64
}

// Snapshot is the coordinator-side record of one run: the per-node reports
// received so far. It becomes complete only when every node has reported —
// until then any query must answer "running", never a fabricated result.
type Snapshot struct {
	ID          int
	StartedAt   time.Time
	CompletedAt time.Time
	reports     map[int]Report
	complete    bool
}

func newCoordinator(cl *Cluster) *Coordinator {
	c := &Coordinator{cluster: cl, snaps: make(map[int]*Snapshot)}
	go func() {
		for r := range cl.Reports {
			c.onReport(r)
		}
	}()
	return c
}

// SubmitTransfer validates and hands a transfer to the sending node's loop.
// The unique ID is assigned here so every transfer is individually traceable
// through any snapshot that catches it in flight.
func (c *Coordinator) SubmitTransfer(from, to int, amount int64) (string, error) {
	switch {
	case amount <= 0:
		return "", ErrNonPositiveAmount
	case from == to:
		return "", ErrSelfTransfer
	case from < 0 || from >= len(c.cluster.Nodes) || to < 0 || to >= len(c.cluster.Nodes):
		return "", ErrUnknownNode
	}
	id := fmt.Sprintf("tx-%06d", atomic.AddInt64(&c.txSeq, 1))
	reply := make(chan error, 1)
	req := TransferRequest{ID: id, To: to, Amount: amount, Reply: reply}
	if err := c.roundTrip(c.cluster.Nodes[from], req, reply); err != nil {
		return "", err
	}
	return id, nil
}

func (c *Coordinator) roundTrip(n *Node, req TransferRequest, reply chan error) error {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case n.reqs <- req:
	case <-timer.C:
		return ErrNodeUnavailable
	}
	select {
	case err := <-reply:
		return err
	case <-timer.C:
		return ErrNodeUnavailable
	}
}

// InitiateSnapshot starts a new Chandy–Lamport run. At most one snapshot may
// be active; once complete, a fresh one can be started and its records are
// kept strictly separate (new ID, new marker scope).
func (c *Coordinator) InitiateSnapshot() (int, error) {
	c.mu.Lock()
	if c.active != nil && !c.active.complete {
		c.mu.Unlock()
		return 0, ErrSnapshotInProgress
	}
	c.nextID++
	s := &Snapshot{ID: c.nextID, StartedAt: time.Now(), reports: make(map[int]Report)}
	c.active = s
	c.snaps[s.ID] = s
	c.mu.Unlock()

	initiator := c.cluster.Nodes[0]
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case initiator.ctrl <- StartSnapshot{SnapID: s.ID}:
		return s.ID, nil
	case <-timer.C:
		return 0, ErrNodeUnavailable
	}
}

// onReport stores a node's local cut. Reports for stale or unknown snapshot
// IDs are dropped so records of different runs can never mix.
func (c *Coordinator) onReport(r Report) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.active
	if s == nil || s.complete || r.SnapID != s.ID {
		return
	}
	s.reports[r.Node] = r
	if len(s.reports) == len(c.cluster.Nodes) {
		s.complete = true
		s.CompletedAt = time.Now()
	}
}

// InFlightTransfer is one transfer caught between the cut's two sides,
// fully traceable (unique ID, endpoints, amount).
type InFlightTransfer struct {
	TxID   string `json:"txId"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Amount int64  `json:"amount"`
}

// SnapshotView is the query result. While the run is incomplete it carries
// only progress information; balances and in-flight detail appear solely
// when every node's report is in.
type SnapshotView struct {
	ID              int                `json:"id"`
	Status          string             `json:"status"` // "running" | "complete"
	ReportsReceived int                `json:"reportsReceived"`
	ReportsExpected int                `json:"reportsExpected"`
	Balances        map[int]int64      `json:"balances,omitempty"`
	InFlight        []InFlightTransfer `json:"inFlight,omitempty"`
	SumBalances     int64              `json:"sumBalances"`
	SumInFlight     int64              `json:"sumInFlight"`
	ExpectedTotal   int64              `json:"expectedTotal"`
	Consistent      bool               `json:"consistent"`
	StartedAt       time.Time          `json:"startedAt"`
	CompletedAt     *time.Time         `json:"completedAt,omitempty"`
}

// GetSnapshot returns the current view of one snapshot, never inventing
// data: incomplete runs are reported as "running".
func (c *Coordinator) GetSnapshot(id int) (SnapshotView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.snaps[id]
	if !ok {
		return SnapshotView{}, false
	}
	return c.buildView(s), true
}

// ListSnapshots summarises all runs, oldest first.
func (c *Coordinator) ListSnapshots() []SnapshotView {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]int, 0, len(c.snaps))
	for id := range c.snaps {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	views := make([]SnapshotView, 0, len(ids))
	for _, id := range ids {
		views = append(views, c.buildView(c.snaps[id]))
	}
	return views
}

// buildView derives the consistency check purely from the recorded reports:
// sum(recorded balances) + sum(recorded in-flight) must equal the initial
// total. Current node balances are never consulted.
func (c *Coordinator) buildView(s *Snapshot) SnapshotView {
	v := SnapshotView{
		ID:              s.ID,
		Status:          "running",
		ReportsReceived: len(s.reports),
		ReportsExpected: len(c.cluster.Nodes),
		ExpectedTotal:   c.cluster.Total(),
		StartedAt:       s.StartedAt,
	}
	if !s.complete {
		return v
	}
	v.Status = "complete"
	completed := s.CompletedAt
	v.CompletedAt = &completed
	v.Balances = make(map[int]int64, len(s.reports))
	for nodeID, rep := range s.reports {
		v.Balances[nodeID] = rep.Balance
		v.SumBalances += rep.Balance
		for _, txs := range rep.InFlight {
			for _, tx := range txs {
				v.InFlight = append(v.InFlight, InFlightTransfer{
					TxID: tx.ID, From: tx.From, To: tx.To, Amount: tx.Amount,
				})
				v.SumInFlight += tx.Amount
			}
		}
	}
	sort.Slice(v.InFlight, func(i, j int) bool { return v.InFlight[i].TxID < v.InFlight[j].TxID })
	v.Consistent = v.SumBalances+v.SumInFlight == v.ExpectedTotal
	return v
}

// liveBalance is a debug probe: it asks each node for its CURRENT balance.
// Used only by the /debug/balances endpoint, never by snapshot logic.
func (c *Coordinator) liveBalance(nodeID int) (int64, error) {
	if nodeID < 0 || nodeID >= len(c.cluster.Nodes) {
		return 0, ErrUnknownNode
	}
	reply := make(chan int64, 1)
	n := c.cluster.Nodes[nodeID]
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case n.bal <- balanceRequest{Reply: reply}:
	case <-timer.C:
		return 0, ErrNodeUnavailable
	}
	select {
	case b := <-reply:
		return b, nil
	case <-timer.C:
		return 0, ErrNodeUnavailable
	}
}
