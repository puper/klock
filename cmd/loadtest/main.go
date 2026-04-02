package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puper/klock/client"
)

func main() {
	addr := flag.String("addr", "grpc://127.0.0.1:8080", "klock grpc address")
	token := flag.String("token", "", "auth token")
	concurrency := flag.Int("concurrency", 32, "number of workers")
	duration := flag.Duration("duration", 15*time.Second, "test duration")
	lockTimeout := flag.Duration("lock-timeout", 2*time.Second, "per lock timeout")
	p1 := flag.String("p1", "tenant-load", "lock p1 key")
	keyspace := flag.Int("keyspace", 512, "distinct p2 keys")
	flag.Parse()

	if *concurrency <= 0 || *duration <= 0 || *keyspace <= 0 {
		log.Fatal("concurrency/duration/keyspace must be > 0")
	}

	var totalOK atomic.Int64
	var totalErr atomic.Int64
	var firstErr atomic.Value
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, 1024)

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	for i := 0; i < *concurrency; i++ {
		workerID := i
		go func() {
			defer wg.Done()
			c := client.NewWithConfig(*addr, client.Config{
				SessionLease:      8 * time.Second,
				HeartbeatInterval: 1 * time.Second,
				HeartbeatTimeout:  2 * time.Second,
				LocalTTL:          3 * time.Second,
				ServerLeaseBuffer: 4 * time.Second,
				AuthToken:         *token,
			})
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = c.Close(ctx)
			}()

			seq := 0
			for time.Now().Before(deadline) {
				seq++
				p2 := fmt.Sprintf("res-%d", (workerID*1000000+seq)%*keyspace)
				start := time.Now()

				ctx, cancel := context.WithTimeout(context.Background(), *lockTimeout)
				h, err := c.Lock(ctx, *p1, p2, client.LockOption{Timeout: *lockTimeout})
				cancel()
				if err != nil {
					totalErr.Add(1)
					if firstErr.Load() == nil {
						firstErr.Store(err.Error())
					}
					continue
				}

				uCtx, uCancel := context.WithTimeout(context.Background(), *lockTimeout)
				err = h.Unlock(uCtx)
				uCancel()
				if err != nil {
					totalErr.Add(1)
					if firstErr.Load() == nil {
						firstErr.Store(err.Error())
					}
					continue
				}

				lat := time.Since(start)
				latMu.Lock()
				latencies = append(latencies, lat)
				latMu.Unlock()
				totalOK.Add(1)
			}
		}()
	}

	wg.Wait()
	ok := totalOK.Load()
	errCnt := totalErr.Load()
	total := ok + errCnt

	latMu.Lock()
	sorted := append([]time.Duration(nil), latencies...)
	latMu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var p50, p95, p99, avg time.Duration
	if len(sorted) > 0 {
		p50 = sorted[len(sorted)*50/100]
		p95 = sorted[len(sorted)*95/100]
		p99 = sorted[len(sorted)*99/100]
		var sum time.Duration
		for _, d := range sorted {
			sum += d
		}
		avg = sum / time.Duration(len(sorted))
	}

	rps := float64(ok) / duration.Seconds()
	errRate := 0.0
	if total > 0 {
		errRate = float64(errCnt) / float64(total) * 100
	}

	fmt.Printf("loadtest finished\n")
	fmt.Printf("  addr:          %s\n", *addr)
	fmt.Printf("  duration:      %s\n", duration.String())
	fmt.Printf("  concurrency:   %d\n", *concurrency)
	fmt.Printf("  keyspace:      %d\n", *keyspace)
	fmt.Printf("  success:       %d\n", ok)
	fmt.Printf("  errors:        %d\n", errCnt)
	if v := firstErr.Load(); v != nil {
		fmt.Printf("  first_error:   %s\n", v.(string))
	}
	fmt.Printf("  error_rate:    %.2f%%\n", errRate)
	fmt.Printf("  throughput:    %.1f ops/s\n", rps)
	fmt.Printf("  latency_avg:   %s\n", avg)
	fmt.Printf("  latency_p50:   %s\n", p50)
	fmt.Printf("  latency_p95:   %s\n", p95)
	fmt.Printf("  latency_p99:   %s\n", p99)
}
