package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Three logical nodes, six directed FIFO channels, one control plane.
// Tokens are demo chips exchanged continuously; the snapshot must observe
// them (including in-flight ones) without ever pausing the exchange.
func main() {
	logger := defaultLogger()

	port := envInt("PORT", 8080)
	initial := int64(envInt("INITIAL_BALANCE", 1000))
	trafficMs := envInt("TRAFFIC_INTERVAL_MS", 100)

	cluster := newCluster([]int64{initial, initial, initial}, logger)
	coord := newCoordinator(cluster)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if trafficMs > 0 {
		go runTraffic(ctx, coord, time.Duration(trafficMs)*time.Millisecond, logger)
	} else {
		logger.Print("background traffic disabled (TRAFFIC_INTERVAL_MS=0)")
	}

	srv := &http.Server{Addr: ":" + strconv.Itoa(port), Handler: newServer(coord)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Printf("listening on :%d — 3 nodes, 6 directed FIFO channels, initial total %d", port, cluster.Total())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("server: %v", err)
	}
	cluster.Shutdown()
}

// runTraffic keeps the three nodes exchanging small random transfers so the
// system is always mid-flight when a snapshot is taken. Transfers that would
// overdraw are skipped — balances never go negative.
func runTraffic(ctx context.Context, coord *Coordinator, interval time.Duration, logger *log.Logger) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tick := time.NewTicker(interval)
	defer tick.Stop()
	logger.Printf("background traffic every %s", interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			from, to := rng.Intn(3), rng.Intn(3)
			if from == to {
				continue
			}
			amount := int64(1 + rng.Intn(10))
			if _, err := coord.SubmitTransfer(from, to, amount); err != nil &&
				!errors.Is(err, ErrInsufficientFunds) {
				logger.Printf("traffic transfer failed: %v", err)
			}
		}
	}
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
