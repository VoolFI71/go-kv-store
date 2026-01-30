//go:build benchmark
// +build benchmark

package main

import (
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type BenchmarkResults struct {
	TotalOps     int64
	Duration     time.Duration
	OpsPerSecond float64
	AvgLatency   time.Duration
	MinLatency   time.Duration
	MaxLatency   time.Duration
	P50Latency   time.Duration
	P95Latency   time.Duration
	P99Latency   time.Duration
	Errors       int64
}

func runBenchmarkPipeline(name string, numOps int, numClients int, pipelineSize int,
	setOp func(*BenchmarkClient, int), getOp func(*BenchmarkClient, int)) BenchmarkResults {
	fmt.Printf("\n=== %s (Pipeline, batch=%d) ===\n", name, pipelineSize)
	fmt.Printf("Операций: %d, Клиентов: %d\n", numOps, numClients)

	var totalOps int64
	var totalErrors int64
	var totalLatency int64
	var minLatency int64 = 1e18
	var maxLatency int64
	latencies := make([]int64, 0, numOps/pipelineSize+1)

	startTime := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex

	opsPerClient := numOps / numClients
	if opsPerClient == 0 {
		opsPerClient = 1
	}

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			client, err := NewBenchmarkClient("localhost:6379")
			if err != nil {
				fmt.Printf("Ошибка подключения клиента %d: %v\n", clientID, err)
				atomic.AddInt64(&totalErrors, int64(opsPerClient))
				return
			}
			defer client.Close()

			remainingOps := opsPerClient
			opIdx := clientID * opsPerClient

			for remainingOps > 0 {
				batchSize := pipelineSize
				if batchSize > remainingOps {
					batchSize = remainingOps
				}

				batchStart := time.Now()
				for j := 0; j < batchSize; j++ {
					if setOp != nil {
						setOp(client, opIdx+j)
					} else if getOp != nil {
						getOp(client, opIdx+j)
					}
				}

				err := client.Flush()
				if err != nil {
					atomic.AddInt64(&totalErrors, int64(batchSize))
					opIdx += batchSize
					remainingOps -= batchSize
					continue
				}

				err = client.ReadPipelineResponses(batchSize)
				batchLatency := time.Since(batchStart).Nanoseconds()

				mu.Lock()
				if batchLatency < minLatency {
					minLatency = batchLatency
				}
				if batchLatency > maxLatency {
					maxLatency = batchLatency
				}
				totalLatency += batchLatency
				latencies = append(latencies, batchLatency)
				mu.Unlock()

				if err != nil {
					atomic.AddInt64(&totalErrors, int64(batchSize))
				} else {
					atomic.AddInt64(&totalOps, int64(batchSize))
				}

				opIdx += batchSize
				remainingOps -= batchSize
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	avgLatency := time.Duration(0)
	if totalOps > 0 {
		avgLatency = time.Duration(totalLatency / int64(len(latencies)))
	}

	opsPerSecond := float64(totalOps) / duration.Seconds()

	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		sorted := make([]int64, len(latencies))
		copy(sorted, latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		if len(sorted) > 0 {
			p50 = time.Duration(sorted[len(sorted)*50/100])
			if len(sorted) > 1 {
				p95 = time.Duration(sorted[len(sorted)*95/100])
				p99 = time.Duration(sorted[len(sorted)*99/100])
			}
		}
	}

	return BenchmarkResults{
		TotalOps:     totalOps,
		Duration:     duration,
		OpsPerSecond: opsPerSecond,
		AvgLatency:   avgLatency,
		MinLatency:   time.Duration(minLatency),
		MaxLatency:   time.Duration(maxLatency),
		P50Latency:   p50,
		P95Latency:   p95,
		P99Latency:   p99,
		Errors:       totalErrors,
	}
}

func runBenchmark(name string, numOps int, numClients int, operation func(*BenchmarkClient, int) error) BenchmarkResults {
	fmt.Printf("\n=== %s ===\n", name)
	fmt.Printf("Операций: %d, Клиентов: %d\n", numOps, numClients)

	var totalOps int64
	var totalErrors int64
	var totalLatency int64
	var minLatency int64 = 1e18
	var maxLatency int64
	latencies := make([]int64, 0, numOps)

	startTime := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex

	opsPerClient := numOps / numClients
	if opsPerClient == 0 {
		opsPerClient = 1
	}

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			client, err := NewBenchmarkClient("localhost:6379")
			if err != nil {
				fmt.Printf("Ошибка подключения клиента %d: %v\n", clientID, err)
				atomic.AddInt64(&totalErrors, int64(opsPerClient))
				return
			}
			defer client.Close()

			for j := 0; j < opsPerClient; j++ {
				opStart := time.Now()
				err := operation(client, clientID*opsPerClient+j)
				latency := time.Since(opStart).Nanoseconds()

				mu.Lock()
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}
				totalLatency += latency
				latencies = append(latencies, latency)
				mu.Unlock()

				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	avgLatency := time.Duration(0)
	if totalOps > 0 {
		avgLatency = time.Duration(totalLatency / totalOps)
	}

	opsPerSecond := float64(totalOps) / duration.Seconds()

	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		sorted := make([]int64, len(latencies))
		copy(sorted, latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		if len(sorted) > 0 {
			p50 = time.Duration(sorted[len(sorted)*50/100])
			if len(sorted) > 1 {
				p95 = time.Duration(sorted[len(sorted)*95/100])
				p99 = time.Duration(sorted[len(sorted)*99/100])
			}
		}
	}

	return BenchmarkResults{
		TotalOps:     totalOps,
		Duration:     duration,
		OpsPerSecond: opsPerSecond,
		AvgLatency:   avgLatency,
		MinLatency:   time.Duration(minLatency),
		MaxLatency:   time.Duration(maxLatency),
		P50Latency:   p50,
		P95Latency:   p95,
		P99Latency:   p99,
		Errors:       totalErrors,
	}
}

func printResults(results BenchmarkResults) {
	fmt.Printf("Результаты:\n")
	fmt.Printf("  ✓ Всего операций: %d\n", results.TotalOps)
	fmt.Printf("  ✗ Ошибок: %d\n", results.Errors)
	fmt.Printf("  ⏱  Время выполнения: %v\n", results.Duration)
	fmt.Printf("  🚀 Пропускная способность: %.2f ops/sec (%.2f K ops/sec)\n",
		results.OpsPerSecond, results.OpsPerSecond/1000)
	fmt.Printf("\n  Латентность:\n")
	fmt.Printf("    Средняя (avg):  %10v\n", results.AvgLatency)
	fmt.Printf("    Медиана (p50):   %10v\n", results.P50Latency)
	fmt.Printf("    95-й перцентиль: %10v\n", results.P95Latency)
	fmt.Printf("    99-й перцентиль: %10v\n", results.P99Latency)
	fmt.Printf("    Минимальная:     %10v\n", results.MinLatency)
	fmt.Printf("    Максимальная:   %10v\n", results.MaxLatency)
}

func main() {
	pipelineOnly := flag.Bool("pipeline-only", false, "run only pipeline benchmark(s) (useful for profiling)")
	pipelineKind := flag.String("pipeline-kind", "both", "pipeline kind: get|set|both")
	pipelineOps := flag.Int("pipeline-ops", 1000000, "total ops for pipeline benchmark(s)")
	pipelineClients := flag.Int("pipeline-clients", 10, "number of clients for pipeline benchmark(s)")
	pipelineBatch := flag.Int("pipeline-batch", 100, "pipeline batch size")
	startDelaySeconds := flag.Int("start-delay", 0, "sleep N seconds before starting (helps to attach pprof)")
	flag.Parse()

	if *pipelineOnly {
		fmt.Println("=== KV Store Benchmark (pipeline only) ===")
		fmt.Println("Убедитесь, что сервер запущен на localhost:6379")
		if *startDelaySeconds > 0 {
			fmt.Printf("Старт через %d секунд...\n", *startDelaySeconds)
			time.Sleep(time.Duration(*startDelaySeconds) * time.Second)
		}
	} else {
		fmt.Println("=== KV Store Benchmark ===")
		fmt.Println("Убедитесь, что сервер запущен на localhost:6379")
		fmt.Println("Нажмите Enter для начала...")
		fmt.Scanln()
	}

	setOp := func(client *BenchmarkClient, idx int) error {
		key := fmt.Sprintf("bench_key_%d", idx)
		value := fmt.Sprintf("bench_value_%d", idx)
		return client.Set(key, value)
	}

	getOp := func(client *BenchmarkClient, idx int) error {
		key := fmt.Sprintf("bench_key_%d", idx%10000)
		_, err := client.Get(key)
		return err
	}

	setOpPipeline := func(client *BenchmarkClient, idx int) {
		key := fmt.Sprintf("bench_key_%d", idx)
		value := fmt.Sprintf("bench_value_%d", idx)
		client.SetPipeline(key, value)
	}

	getOpPipeline := func(client *BenchmarkClient, idx int) {
		key := fmt.Sprintf("bench_key_%d", idx%10000)
		client.GetPipeline(key)
	}

	needPrepForGet := !*pipelineOnly || *pipelineKind == "get" || *pipelineKind == "both"
	if needPrepForGet {
		fmt.Println("\nПодготовка данных для GET тестов...")
		prepClient, _ := NewBenchmarkClient("localhost:6379")
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("bench_key_%d", i)
			value := fmt.Sprintf("bench_value_%d", i)
			prepClient.Set(key, value)
		}
		prepClient.Close()
	}

	if *pipelineOnly {
		switch *pipelineKind {
		case "set":
			results := runBenchmarkPipeline("SET Pipeline", *pipelineOps, *pipelineClients, *pipelineBatch, setOpPipeline, nil)
			printResults(results)
		case "get":
			results := runBenchmarkPipeline("GET Pipeline", *pipelineOps, *pipelineClients, *pipelineBatch, nil, getOpPipeline)
			printResults(results)
		default: // "both"
			resultsSet := runBenchmarkPipeline("SET Pipeline", *pipelineOps, *pipelineClients, *pipelineBatch, setOpPipeline, nil)
			printResults(resultsSet)
			resultsGet := runBenchmarkPipeline("GET Pipeline", *pipelineOps, *pipelineClients, *pipelineBatch, nil, getOpPipeline)
			printResults(resultsGet)
		}
		fmt.Println("\n=== Benchmark завершен ===")
		return
	}

	results1 := runBenchmark("SET (1 клиент, 100000 операций)", 100000, 1, setOp)
	printResults(results1)

	results2 := runBenchmark("SET (10 клиентов, 100000 операций)", 100000, 10, setOp)
	printResults(results2)

	results3 := runBenchmark("GET (1 клиент, 1000000 операций)", 1000000, 1, getOp)
	printResults(results3)

	results4 := runBenchmark("GET (10 клиентов, 1000000 операций)", 1000000, 10, getOp)
	printResults(results4)

	results5 := runBenchmarkPipeline("SET Pipeline", 1000000, 10, 100, setOpPipeline, nil)
	printResults(results5)

	results6 := runBenchmarkPipeline("GET Pipeline", 1000000, 10, 100, nil, getOpPipeline)
	printResults(results6)

	fmt.Printf("\n=== Смешанная нагрузка (50%% SET, 50%% GET) ===\n")
	fmt.Printf("Операций: 200000, Клиентов: 10\n")

	var mixedOps int64
	var mixedErrors int64
	var mixedLatency int64
	var mixedMinLatency int64 = 1e18
	var mixedMaxLatency int64
	mixedLatencies := make([]int64, 0, 200000)
	mixedStart := time.Now()
	var mixedWg sync.WaitGroup
	var mixedMu sync.Mutex

	const mixedTotalOps = 200000
	const mixedClients = 10
	opsPerClient := mixedTotalOps / mixedClients
	extraOps := mixedTotalOps % mixedClients

	for i := 0; i < mixedClients; i++ {
		mixedWg.Add(1)
		go func(clientID int) {
			defer mixedWg.Done()

			client, err := NewBenchmarkClient("localhost:6379")
			if err != nil {
				return
			}
			defer client.Close()

			myOps := opsPerClient
			startIdx := clientID*opsPerClient + min(clientID, extraOps)
			if clientID < extraOps {
				myOps++
			}

			for j := 0; j < myOps; j++ {
				idx := startIdx + j
				opStart := time.Now()
				var err error

				if idx%2 == 0 {
					key := fmt.Sprintf("mixed_key_%d", idx)
					value := fmt.Sprintf("mixed_value_%d", idx)
					err = client.Set(key, value)
				} else {
					key := fmt.Sprintf("bench_key_%d", idx%10000)
					_, err = client.Get(key)
				}

				latency := time.Since(opStart).Nanoseconds()

				mixedMu.Lock()
				if latency < mixedMinLatency {
					mixedMinLatency = latency
				}
				if latency > mixedMaxLatency {
					mixedMaxLatency = latency
				}
				mixedLatency += latency
				mixedLatencies = append(mixedLatencies, latency)
				mixedMu.Unlock()

				if err != nil {
					atomic.AddInt64(&mixedErrors, 1)
				} else {
					atomic.AddInt64(&mixedOps, 1)
				}
			}
		}(i)
	}

	mixedWg.Wait()
	mixedDuration := time.Since(mixedStart)
	mixedOpsPerSecond := float64(mixedOps) / mixedDuration.Seconds()
	mixedAvgLatency := time.Duration(0)
	if mixedOps > 0 {
		mixedAvgLatency = time.Duration(mixedLatency / mixedOps)
	}

	var mixedP50, mixedP95, mixedP99 time.Duration
	if len(mixedLatencies) > 0 {
		sort.Slice(mixedLatencies, func(i, j int) bool { return mixedLatencies[i] < mixedLatencies[j] })
		if len(mixedLatencies) > 0 {
			mixedP50 = time.Duration(mixedLatencies[len(mixedLatencies)*50/100])
			if len(mixedLatencies) > 1 {
				mixedP95 = time.Duration(mixedLatencies[len(mixedLatencies)*95/100])
				mixedP99 = time.Duration(mixedLatencies[len(mixedLatencies)*99/100])
			}
		}
	}

	fmt.Printf("Результаты:\n")
	fmt.Printf("  ✓ Всего операций: %d\n", mixedOps)
	fmt.Printf("  ✗ Ошибок: %d\n", mixedErrors)
	fmt.Printf("  ⏱  Время выполнения: %v\n", mixedDuration)
	fmt.Printf("  🚀 Пропускная способность: %.2f ops/sec (%.2f K ops/sec)\n",
		mixedOpsPerSecond, mixedOpsPerSecond/1000)
	fmt.Printf("\n  Латентность:\n")
	fmt.Printf("    Средняя (avg):  %10v\n", mixedAvgLatency)
	fmt.Printf("    Медиана (p50):   %10v\n", mixedP50)
	fmt.Printf("    95-й перцентиль: %10v\n", mixedP95)
	fmt.Printf("    99-й перцентиль: %10v\n", mixedP99)
	fmt.Printf("    Минимальная:     %10v\n", time.Duration(mixedMinLatency))
	fmt.Printf("    Максимальная:   %10v\n", time.Duration(mixedMaxLatency))

	fmt.Println("\n=== Benchmark завершен ===")
}
