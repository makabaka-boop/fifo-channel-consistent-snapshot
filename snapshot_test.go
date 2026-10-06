package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"testing"
	"time"
)

func newTestCluster(t *testing.T, balances ...int64) (*Cluster, *Coordinator) {
	t.Helper()
	logger := log.New(io.Discard, "", 0)
	cl := newCluster(balances, logger)
	co := newCoordinator(cl)
	t.Cleanup(cl.Shutdown)
	return cl, co
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func completeView(t *testing.T, co *Coordinator, sid int) SnapshotView {
	t.Helper()
	var v SnapshotView
	waitFor(t, fmt.Sprintf("snapshot %d to complete", sid), func() bool {
		var ok bool
		v, ok = co.GetSnapshot(sid)
		return ok && v.Status == "complete"
	})
	return v
}

func assertConsistent(t *testing.T, v SnapshotView, wantTotal int64) {
	t.Helper()
	if !v.Consistent {
		t.Fatalf("snapshot %d inconsistent: balances=%v sum=%d inFlight=%d total=%d want %d",
			v.ID, v.Balances, v.SumBalances, v.SumInFlight, v.SumBalances+v.SumInFlight, wantTotal)
	}
	seen := map[string]bool{}
	for _, tx := range v.InFlight {
		if tx.TxID == "" {
			t.Fatal("in-flight transfer without traceable id")
		}
		if seen[tx.TxID] {
			t.Fatalf("in-flight transfer %s recorded twice", tx.TxID)
		}
		seen[tx.TxID] = true
	}
}

// Scenario 1: a transfer is debited at the sender but, held back by the test
// barrier, never received before the cut. The snapshot must surface it as
// in-flight (traceable by id), and while the barrier holds, the coordinator
// must report "running" — never a fabricated complete result.
func TestSnapshotCapturesDebitedButNotReceived(t *testing.T) {
	cl, co := newTestCluster(t, 100, 100, 100)

	ch01 := cl.Channel(0, 1)
	ch01.Hold()

	txID, err := co.SubmitTransfer(0, 1, 30)
	if err != nil {
		t.Fatalf("submit transfer: %v", err)
	}
	// Debit happened (reply came after enqueue), delivery is held back.
	if got := ch01.Len(); got != 1 {
		t.Fatalf("expected 1 queued transfer on 0->1, got %d", got)
	}

	sid, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatalf("initiate snapshot: %v", err)
	}
	// Initiator's marker must queue BEHIND the in-flight transfer on 0->1.
	waitFor(t, "marker queued behind transfer on 0->1", func() bool { return ch01.Len() == 2 })

	// Markers flow on the five free channels; node 1's cut waits for 0->1.
	waitFor(t, "two of three reports", func() bool {
		v, _ := co.GetSnapshot(sid)
		return v.ReportsReceived == 2
	})
	v, _ := co.GetSnapshot(sid)
	if v.Status != "running" {
		t.Fatalf("barrier held: expected status running, got %q (%+v)", v.Status, v)
	}
	if v.Consistent || v.Balances != nil {
		t.Fatalf("incomplete snapshot must not present a result: %+v", v)
	}

	ch01.Release()
	v = completeView(t, co, sid)

	assertConsistent(t, v, 300)
	wantBal := map[int]int64{0: 70, 1: 100, 2: 100}
	if !reflect.DeepEqual(v.Balances, wantBal) {
		t.Fatalf("balances = %v, want %v", v.Balances, wantBal)
	}
	wantFlight := []InFlightTransfer{{TxID: txID, From: 0, To: 1, Amount: 30}}
	if !reflect.DeepEqual(v.InFlight, wantFlight) {
		t.Fatalf("inFlight = %+v, want %+v", v.InFlight, wantFlight)
	}
	if v.SumBalances != 270 || v.SumInFlight != 30 {
		t.Fatalf("sums = %d + %d, want 270 + 30", v.SumBalances, v.SumInFlight)
	}
}

// Scenario 2: markers interleave with transfers on every channel. All six
// channels are held; the test releases them one by one in a fixed order,
// producing a known cut that must be reproduced exactly.
func TestMarkerInterleavingWithControlledDelivery(t *testing.T) {
	cl, co := newTestCluster(t, 100, 100, 100)
	cl.HoldAll()

	txA, err := co.SubmitTransfer(0, 1, 10) // will be in flight on 0->1
	if err != nil {
		t.Fatal(err)
	}
	txB, err := co.SubmitTransfer(1, 2, 20) // will be in flight on 1->2
	if err != nil {
		t.Fatal(err)
	}
	txC, err := co.SubmitTransfer(2, 0, 5) // will be in flight on 2->0
	if err != nil {
		t.Fatal(err)
	}

	sid, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Initiator (node 0) records 90 and enqueues markers on both out-channels
	// before anything else can leave; on 0->1 the marker sits behind txA.
	waitFor(t, "initiator markers queued", func() bool {
		return cl.Channel(0, 1).Len() == 2 && cl.Channel(0, 2).Len() == 1
	})
	// Nodes 1 and 2 have not recorded yet: no markers on their out-channels.
	if cl.Channel(1, 2).Len() != 1 || cl.Channel(2, 0).Len() != 1 {
		t.Fatal("no node besides the initiator may have emitted markers yet")
	}

	// Controlled delivery order, one channel at a time.
	cl.Channel(0, 2).Release() // node 2 records (95), queues markers behind txC on 2->0, and on 2->1
	waitFor(t, "node 2 markers queued", func() bool {
		return cl.Channel(2, 0).Len() == 2 && cl.Channel(2, 1).Len() == 1
	})
	cl.Channel(2, 1).Release() // node 1 records (80), queues markers behind txB on 1->2, and on 1->0
	waitFor(t, "node 1 markers queued", func() bool {
		return cl.Channel(1, 2).Len() == 2 && cl.Channel(1, 0).Len() == 1
	})
	cl.Channel(0, 1).Release() // txA lands in node 1's channel-state log, then the marker closes 0->1
	cl.Channel(1, 2).Release() // txB lands in node 2's channel-state log, then the marker closes 1->2
	cl.Channel(2, 0).Release() // txC lands in node 0's channel-state log, then the marker closes 2->0
	cl.Channel(1, 0).Release() // empty channel state

	v := completeView(t, co, sid)
	assertConsistent(t, v, 300)

	wantBal := map[int]int64{0: 90, 1: 80, 2: 95}
	if !reflect.DeepEqual(v.Balances, wantBal) {
		t.Fatalf("balances = %v, want %v", v.Balances, wantBal)
	}
	got := map[string]InFlightTransfer{}
	for _, tx := range v.InFlight {
		got[tx.TxID] = tx
	}
	want := map[string]InFlightTransfer{
		txA: {TxID: txA, From: 0, To: 1, Amount: 10},
		txB: {TxID: txB, From: 1, To: 2, Amount: 20},
		txC: {TxID: txC, From: 2, To: 0, Amount: 5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inFlight = %v, want %v", got, want)
	}
	if v.SumBalances != 265 || v.SumInFlight != 35 {
		t.Fatalf("sums = %d + %d, want 265 + 35", v.SumBalances, v.SumInFlight)
	}
}

// Scenario 3: two snapshots back to back. Each must form its own consistent
// cut; records must not mix, and the first snapshot's stored result must be
// untouched by the second run.
func TestTwoConsecutiveSnapshotsDoNotMix(t *testing.T) {
	cl, co := newTestCluster(t, 100, 100, 100)

	// --- Snapshot 1: transfer D in flight on 1->0. ---
	cl.Channel(1, 0).Hold()
	txD, err := co.SubmitTransfer(1, 0, 15)
	if err != nil {
		t.Fatal(err)
	}
	sid1, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "snapshot 1 marker queued behind D", func() bool { return cl.Channel(1, 0).Len() == 2 })
	if v, _ := co.GetSnapshot(sid1); v.Status != "running" {
		t.Fatalf("snapshot 1 should be running while 1->0 is held, got %q", v.Status)
	}
	cl.Channel(1, 0).Release()
	v1 := completeView(t, co, sid1)
	assertConsistent(t, v1, 300)
	if want := (map[int]int64{0: 100, 1: 85, 2: 100}); !reflect.DeepEqual(v1.Balances, want) {
		t.Fatalf("snapshot 1 balances = %v, want %v", v1.Balances, want)
	}
	if len(v1.InFlight) != 1 || v1.InFlight[0].TxID != txD {
		t.Fatalf("snapshot 1 inFlight = %+v, want only %s", v1.InFlight, txD)
	}

	// --- Snapshot 2: only allowed after 1 completed; transfer E in flight on 2->1. ---
	cl.Channel(2, 1).Hold()
	txE, err := co.SubmitTransfer(2, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	sid2, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatalf("second snapshot must be allowed after the first completed: %v", err)
	}
	if sid2 == sid1 {
		t.Fatal("each snapshot needs its own id")
	}
	waitFor(t, "snapshot 2 marker queued behind E", func() bool { return cl.Channel(2, 1).Len() == 2 })
	cl.Channel(2, 1).Release()
	v2 := completeView(t, co, sid2)
	assertConsistent(t, v2, 300)
	// Node 0 has credited D by now (115); node 2 debited E (93).
	if want := (map[int]int64{0: 115, 1: 85, 2: 93}); !reflect.DeepEqual(v2.Balances, want) {
		t.Fatalf("snapshot 2 balances = %v, want %v", v2.Balances, want)
	}
	if len(v2.InFlight) != 1 || v2.InFlight[0].TxID != txE {
		t.Fatalf("snapshot 2 inFlight = %+v, want only %s (no record from snapshot 1)", v2.InFlight, txE)
	}

	// Snapshot 1's record is immutable and uncontaminated.
	v1again, ok := co.GetSnapshot(sid1)
	if !ok || !reflect.DeepEqual(v1again, v1) {
		t.Fatalf("snapshot 1 record changed after snapshot 2:\nbefore: %+v\nafter:  %+v", v1, v1again)
	}
}

// Only one snapshot may be active at a time; after completion a new one is
// allowed and gets a fresh id.
func TestOnlyOneActiveSnapshot(t *testing.T) {
	cl, co := newTestCluster(t, 100, 100, 100)
	cl.HoldAll() // keep markers from completing the first snapshot

	sid1, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co.InitiateSnapshot(); !errors.Is(err, ErrSnapshotInProgress) {
		t.Fatalf("second initiate while running: got %v, want ErrSnapshotInProgress", err)
	}
	cl.ReleaseAll()
	completeView(t, co, sid1)

	sid2, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatalf("initiate after completion must succeed: %v", err)
	}
	if sid2 == sid1 {
		t.Fatal("snapshot ids must not be reused")
	}
	completeView(t, co, sid2)
}

// Transfer validation: positive integers only, distinct endpoints, known
// nodes, sufficient funds; every accepted transfer gets a unique id.
func TestTransferValidation(t *testing.T) {
	_, co := newTestCluster(t, 50, 50, 50)

	cases := []struct {
		name     string
		from, to int
		amount   int64
		wantErr  error
	}{
		{"zero amount", 0, 1, 0, ErrNonPositiveAmount},
		{"negative amount", 0, 1, -5, ErrNonPositiveAmount},
		{"self transfer", 1, 1, 5, ErrSelfTransfer},
		{"unknown sender", 9, 1, 5, ErrUnknownNode},
		{"unknown receiver", 0, 9, 5, ErrUnknownNode},
		{"insufficient funds", 0, 1, 1000, ErrInsufficientFunds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := co.SubmitTransfer(tc.from, tc.to, tc.amount); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}

	id1, err := co.SubmitTransfer(0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := co.SubmitTransfer(0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatalf("transfer ids must be unique, got %q twice", id1)
	}
}

// Snapshots must be consistent even while transfers keep flowing — the
// exchange is never paused. Run with -race.
func TestSnapshotUnderConcurrentTraffic(t *testing.T) {
	_, co := newTestCluster(t, 1000, 1000, 1000)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			from, to := i%3, (i+1)%3
			_, _ = co.SubmitTransfer(from, to, 1) // may briefly hit insufficient funds; fine
			i++
		}
	}()

	sid, err := co.InitiateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	v := completeView(t, co, sid)

	// Repeated snapshots under load: each run gets a fresh id and its own
	// consistent cut; earlier records stay intact.
	for i := 0; i < 5; i++ {
		sid, err = co.InitiateSnapshot()
		if err != nil {
			t.Fatalf("snapshot %d under traffic: %v", i+2, err)
		}
		v = completeView(t, co, sid)
		assertConsistent(t, v, 3000)
	}
	close(stop)
	<-done

	assertConsistent(t, v, 3000)
	if v.SumBalances+v.SumInFlight != 3000 {
		t.Fatalf("cut total = %d, want 3000", v.SumBalances+v.SumInFlight)
	}
	t.Logf("cut under traffic: balances=%v inFlight=%d transfers", v.Balances, len(v.InFlight))
}
