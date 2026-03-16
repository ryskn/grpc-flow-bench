package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/ryskn/grpc-flow-bench/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	target       = flag.String("target", "localhost:50051", "server address")
	flows        = flag.Int("flows", 100, "number of concurrent flows (connections)")
	duration     = flag.Duration("duration", 10*time.Second, "benchmark duration")
	payloadSize  = flag.Int("payload", 64, "payload size in bytes")
	mode         = flag.String("mode", "unary", "mode: unary or stream")
	setupTimeout = flag.Duration("setup-timeout", 30*time.Second, "timeout for stream setup phase")
)

type stats struct {
	latencies []int64
	mu        sync.Mutex
}

func (s *stats) add(ns int64) {
	s.mu.Lock()
	s.latencies = append(s.latencies, ns)
	s.mu.Unlock()
}

func (s *stats) report(totalRPCs int64, elapsed time.Duration) {
	s.mu.Lock()
	lats := s.latencies
	s.mu.Unlock()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	n := len(lats)
	if n == 0 {
		fmt.Println("no data")
		return
	}

	sum := int64(0)
	for _, l := range lats {
		sum += l
	}

	p := func(pct float64) float64 {
		idx := int(math.Ceil(pct/100*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		return float64(lats[idx]) / 1e6
	}

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Flows (connections): %d\n", *flows)
	fmt.Printf("Mode:                %s\n", *mode)
	fmt.Printf("Duration:            %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total RPCs:          %d\n", totalRPCs)
	fmt.Printf("RPS:                 %.0f\n", float64(totalRPCs)/elapsed.Seconds())
	fmt.Printf("Latency avg:         %.3f ms\n", float64(sum)/float64(n)/1e6)
	fmt.Printf("Latency p50:         %.3f ms\n", p(50))
	fmt.Printf("Latency p95:         %.3f ms\n", p(95))
	fmt.Printf("Latency p99:         %.3f ms\n", p(99))
	fmt.Printf("Latency max:         %.3f ms\n", float64(lats[n-1])/1e6)
}

func runUnary(conn *pb.BenchClient, payload []byte, ctx context.Context, counter *int64, s *stats) {
	seq := int64(0)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		start := time.Now()
		_, err := (*conn).Ping(ctx, &pb.PingRequest{Seq: seq, Payload: payload})
		if err != nil {
			continue
		}
		s.add(time.Since(start).Nanoseconds())
		atomic.AddInt64(counter, 1)
		seq++
	}
}

func runStream(stream pb.Bench_StreamPingClient, payload []byte, benchCtx context.Context, counter *int64, s *stats) {
	seq := int64(0)
	for {
		select {
		case <-benchCtx.Done():
			stream.CloseSend()
			return
		default:
		}
		start := time.Now()
		if err := stream.Send(&pb.PingRequest{Seq: seq, Payload: payload}); err != nil {
			return
		}
		if _, err := stream.Recv(); err != nil {
			return
		}
		s.add(time.Since(start).Nanoseconds())
		atomic.AddInt64(counter, 1)
		seq++
	}
}

func main() {
	flag.Parse()

	payload := make([]byte, *payloadSize)

	fmt.Printf("Connecting %d flows to %s ...\n", *flows, *target)

	conns := make([]*grpc.ClientConn, *flows)
	clients := make([]pb.BenchClient, *flows)
	for i := 0; i < *flows; i++ {
		conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect flow %d: %v", i, err)
		}
		conns[i] = conn
		clients[i] = pb.NewBenchClient(conn)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	var counter int64
	s := &stats{}
	var wg sync.WaitGroup

	if *mode == "stream" {
		// Phase 1: establish all streams with a dedicated setup timeout
		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()

		streams := make([]pb.Bench_StreamPingClient, *flows)
		var setupWg sync.WaitGroup
		var setupErrors int64

		fmt.Printf("Setting up %d streams (timeout: %s)...\n", *flows, *setupTimeout)
		for i := 0; i < *flows; i++ {
			setupWg.Add(1)
			go func(i int, c pb.BenchClient) {
				defer setupWg.Done()
				stream, err := c.StreamPing(streamCtx)
				if err != nil {
					log.Printf("stream open error [%d]: %v", i, err)
					atomic.AddInt64(&setupErrors, 1)
					return
				}
				streams[i] = stream
			}(i, clients[i])
		}

		setupDone := make(chan struct{})
		go func() { setupWg.Wait(); close(setupDone) }()
		select {
		case <-setupDone:
		case <-time.After(*setupTimeout):
			fmt.Printf("Warning: setup timed out after %s\n", *setupTimeout)
		}

		if setupErrors > 0 {
			fmt.Printf("Warning: %d/%d streams failed to open\n", setupErrors, *flows)
		}

		// Phase 2: run benchmark on established streams
		benchCtx, benchCancel := context.WithTimeout(context.Background(), *duration)
		defer benchCancel()

		start := time.Now()
		for i := 0; i < *flows; i++ {
			if streams[i] == nil {
				continue
			}
			wg.Add(1)
			go func(stream pb.Bench_StreamPingClient) {
				defer wg.Done()
				runStream(stream, payload, benchCtx, &counter, s)
			}(streams[i])
		}
		wg.Wait()
		elapsed := time.Since(start)
		s.report(atomic.LoadInt64(&counter), elapsed)
	} else {
		benchCtx, benchCancel := context.WithTimeout(context.Background(), *duration)
		defer benchCancel()

		start := time.Now()
		for i := 0; i < *flows; i++ {
			wg.Add(1)
			go func(c pb.BenchClient) {
				defer wg.Done()
				runUnary(&c, payload, benchCtx, &counter, s)
			}(clients[i])
		}
		wg.Wait()
		elapsed := time.Since(start)
		s.report(atomic.LoadInt64(&counter), elapsed)
	}
}
