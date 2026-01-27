package main

import (
	"fmt"
	"sync/atomic"
	"testing"
)

const (
	benchmarkServerAddr = "localhost:6379"
)

func BenchmarkSET(b *testing.B) {
	client, err := NewBenchmarkClient(benchmarkServerAddr)
	if err != nil {
		b.Fatalf("Не удалось подключиться к серверу: %v", err)
	}
	defer client.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := client.Set(key, value); err != nil {
			b.Fatalf("Ошибка SET: %v", err)
		}
	}
}

func BenchmarkGET(b *testing.B) {
	client, err := NewBenchmarkClient(benchmarkServerAddr)
	if err != nil {
		b.Fatalf("Не удалось подключиться к серверу: %v", err)
	}
	defer client.Close()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		client.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		_, err := client.Get(key)
		if err != nil {
			b.Fatalf("Ошибка GET: %v", err)
		}
	}
}

func BenchmarkSETParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		client, err := NewBenchmarkClient(benchmarkServerAddr)
		if err != nil {
			b.Fatalf("Не удалось подключиться к серверу: %v", err)
		}
		defer client.Close()

		counter := int64(0)
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("pkey%d", idx)
			value := fmt.Sprintf("pvalue%d", idx)
			if err := client.Set(key, value); err != nil {
				b.Fatalf("Ошибка SET: %v", err)
			}
		}
	})
}

func BenchmarkGETParallel(b *testing.B) {
	prepClient, err := NewBenchmarkClient(benchmarkServerAddr)
	if err != nil {
		b.Fatalf("Не удалось подключиться к серверу: %v", err)
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("pkey%d", i)
		value := fmt.Sprintf("pvalue%d", i)
		prepClient.Set(key, value)
	}
	prepClient.Close()

	b.RunParallel(func(pb *testing.PB) {
		client, err := NewBenchmarkClient(benchmarkServerAddr)
		if err != nil {
			b.Fatalf("Не удалось подключиться к серверу: %v", err)
		}
		defer client.Close()

		counter := int64(0)
		for pb.Next() {
			idx := atomic.AddInt64(&counter, 1) % 10000
			key := fmt.Sprintf("pkey%d", idx)
			_, err := client.Get(key)
			if err != nil {
				b.Fatalf("Ошибка GET: %v", err)
			}
		}
	})
}
