package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T, balances ...int64) (*Cluster, *Coordinator, *httptest.Server) {
	t.Helper()
	cl := newCluster(balances, log.New(io.Discard, "", 0))
	co := newCoordinator(cl)
	srv := httptest.NewServer(newServer(co))
	t.Cleanup(func() {
		srv.Close()
		cl.Shutdown()
	})
	return cl, co, srv
}

func post(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestHTTPAPI(t *testing.T) {
	cl, _, srv := newTestServer(t, 100, 100, 100)

	// --- transfer validation over HTTP ---
	bad := []struct {
		body map[string]any
		want int
	}{
		{map[string]any{"from": 0, "to": 1, "amount": 0}, http.StatusBadRequest},
		{map[string]any{"from": 0, "to": 1, "amount": -7}, http.StatusBadRequest},
		{map[string]any{"from": 2, "to": 2, "amount": 5}, http.StatusBadRequest},
		{map[string]any{"from": 0, "to": 5, "amount": 5}, http.StatusBadRequest},
		{map[string]any{"from": 0, "to": 1, "amount": 99999}, http.StatusConflict},
	}
	for i, tc := range bad {
		if code, _ := post(t, srv.URL+"/transfers", tc.body); code != tc.want {
			t.Fatalf("bad transfer %d: got HTTP %d, want %d", i, code, tc.want)
		}
	}

	code, resp := post(t, srv.URL+"/transfers", map[string]any{"from": 0, "to": 1, "amount": 25})
	if code != http.StatusAccepted || resp["txId"] == "" {
		t.Fatalf("valid transfer: got %d %v", code, resp)
	}

	// --- one active snapshot at a time; incomplete while barrier holds ---
	cl.HoldAll()
	code, resp = post(t, srv.URL+"/snapshots", nil)
	if code != http.StatusAccepted {
		t.Fatalf("initiate: got %d %v", code, resp)
	}
	sid := int(resp["snapshotId"].(float64))

	if code, _ = post(t, srv.URL+"/snapshots", nil); code != http.StatusConflict {
		t.Fatalf("second initiate while running: got HTTP %d, want 409", code)
	}

	code, resp = get(t, fmt.Sprintf("%s/snapshots/%d", srv.URL, sid))
	if code != http.StatusOK || resp["status"] != "running" {
		t.Fatalf("held channels must yield running, got %d %v", code, resp)
	}
	if _, hasBalances := resp["balances"]; hasBalances {
		t.Fatalf("running snapshot must not carry a balance result: %v", resp)
	}

	if code, _ = get(t, srv.URL+"/snapshots/999"); code != http.StatusNotFound {
		t.Fatalf("unknown snapshot: got HTTP %d, want 404", code)
	}

	// --- release the barrier: snapshot completes and is consistent ---
	cl.ReleaseAll()
	var view map[string]any
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, view = get(t, fmt.Sprintf("%s/snapshots/%d", srv.URL, sid))
		if view["status"] == "complete" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if view["status"] != "complete" {
		t.Fatalf("snapshot did not complete after release: %v", view)
	}
	if view["consistent"] != true {
		t.Fatalf("snapshot inconsistent: %v", view)
	}
	if int(view["sumBalances"].(float64)+view["sumInFlight"].(float64)) != 300 {
		t.Fatalf("cut total != 300: %v", view)
	}

	// --- after completion a new snapshot may be taken ---
	code, resp = post(t, srv.URL+"/snapshots", nil)
	if code != http.StatusAccepted {
		t.Fatalf("initiate after completion: got %d %v", code, resp)
	}
	if int(resp["snapshotId"].(float64)) == sid {
		t.Fatal("snapshot id reused")
	}

	// --- debug endpoints ---
	code, resp = get(t, srv.URL+"/debug/balances")
	if code != http.StatusOK || resp["balances"] == nil {
		t.Fatalf("debug balances: got %d %v", code, resp)
	}
	code, resp = get(t, srv.URL+"/debug/channels")
	if code != http.StatusOK || len(resp["channels"].([]any)) != 6 {
		t.Fatalf("debug channels: got %d %v", code, resp)
	}
}

func TestHTTPBarrierEndpoints(t *testing.T) {
	cl, _, srv := newTestServer(t, 100, 100, 100)

	code, _ := post(t, srv.URL+"/debug/channels/0/1/hold", nil)
	if code != http.StatusOK || !cl.Channel(0, 1).Held() {
		t.Fatalf("hold: got HTTP %d, held=%v", code, cl.Channel(0, 1).Held())
	}
	code, _ = post(t, srv.URL+"/debug/channels/0/1/release", nil)
	if code != http.StatusOK || cl.Channel(0, 1).Held() {
		t.Fatalf("release: got HTTP %d, held=%v", code, cl.Channel(0, 1).Held())
	}
	if code, _ := post(t, srv.URL+"/debug/channels/0/0/hold", nil); code != http.StatusNotFound {
		t.Fatalf("self channel: got HTTP %d, want 404", code)
	}
	if code, _ := post(t, srv.URL+"/debug/channels/1/9/hold", nil); code != http.StatusNotFound {
		t.Fatalf("unknown channel: got HTTP %d, want 404", code)
	}
}
