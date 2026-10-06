package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// Server exposes the control plane over HTTP. It holds no node state itself;
// everything goes through the coordinator.
type Server struct {
	coord *Coordinator
	mux   *http.ServeMux
}

func newServer(coord *Coordinator) *Server {
	s := &Server{coord: coord, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /", s.handleIndex)
	s.mux.HandleFunc("POST /transfers", s.handleTransfer)
	s.mux.HandleFunc("POST /snapshots", s.handleInitiateSnapshot)
	s.mux.HandleFunc("GET /snapshots", s.handleListSnapshots)
	s.mux.HandleFunc("GET /snapshots/{id}", s.handleGetSnapshot)
	s.mux.HandleFunc("GET /debug/balances", s.handleDebugBalances)
	s.mux.HandleFunc("GET /debug/channels", s.handleDebugChannels)
	s.mux.HandleFunc("POST /debug/channels/{from}/{to}/hold", s.handleBarrier(true))
	s.mux.HandleFunc("POST /debug/channels/{from}/{to}/release", s.handleBarrier(false))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "chandy-lamport snapshot demo (3 nodes, 6 directed FIFO channels)",
		"endpoints": map[string]string{
			"POST /transfers":                          `{"from":0,"to":1,"amount":25} — positive integer transfer, returns unique txId`,
			"POST /snapshots":                          "initiate a snapshot (409 while one is active)",
			"GET  /snapshots":                          "list all snapshots",
			"GET  /snapshots/{id}":                     "status running|complete; complete views include balances, in-flight transfers and the consistency check",
			"GET  /debug/balances":                     "live balances (observability only, never used for snapshots)",
			"GET  /debug/channels":                     "queue depth and barrier state per directed channel",
			"POST /debug/channels/{from}/{to}/hold":    "engage test barrier on one channel",
			"POST /debug/channels/{from}/{to}/release": "lift test barrier",
		},
	})
}

type transferBody struct {
	From   int   `json:"from"`
	To     int   `json:"to"`
	Amount int64 `json:"amount"`
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	var body transferBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	id, err := s.coord.SubmitTransfer(body.From, body.To, body.Amount)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]any{
			"txId": id, "from": body.From, "to": body.To, "amount": body.Amount, "status": "queued",
		})
	case errors.Is(err, ErrInsufficientFunds):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrNodeUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func (s *Server) handleInitiateSnapshot(w http.ResponseWriter, _ *http.Request) {
	id, err := s.coord.InitiateSnapshot()
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]any{"snapshotId": id, "status": "running"})
	case errors.Is(err, ErrSnapshotInProgress):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, err.Error())
	}
}

func (s *Server) handleListSnapshots(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": s.coord.ListSnapshots()})
}

func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "snapshot id must be an integer")
		return
	}
	view, ok := s.coord.GetSnapshot(id)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown snapshot id")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// handleDebugBalances reports LIVE balances for observability. Snapshot
// results are never derived from these values.
func (s *Server) handleDebugBalances(w http.ResponseWriter, _ *http.Request) {
	balances := make(map[int]int64)
	var sum int64
	for i := range s.coord.cluster.Nodes {
		b, err := s.coord.liveBalance(i)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		balances[i] = b
		sum += b
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"balances": balances,
		"note":     "live balances, observability only — snapshot results never use current balances",
	})
}

func (s *Server) handleDebugChannels(w http.ResponseWriter, _ *http.Request) {
	type ch struct {
		From   int  `json:"from"`
		To     int  `json:"to"`
		Queued int  `json:"queued"`
		Held   bool `json:"held"`
	}
	list := []ch{}
	for _, c := range s.coord.cluster.Channels() {
		list = append(list, ch{From: c.From, To: c.To, Queued: c.Len(), Held: c.Held()})
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": list})
}

func (s *Server) handleBarrier(hold bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		from, err1 := strconv.Atoi(r.PathValue("from"))
		to, err2 := strconv.Atoi(r.PathValue("to"))
		if err1 != nil || err2 != nil {
			writeError(w, http.StatusBadRequest, "from/to must be integers")
			return
		}
		c := s.coord.cluster.Channel(from, to)
		if c == nil {
			writeError(w, http.StatusNotFound, "no such directed channel")
			return
		}
		if hold {
			c.Hold()
		} else {
			c.Release()
		}
		writeJSON(w, http.StatusOK, map[string]any{"from": from, "to": to, "held": hold})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
