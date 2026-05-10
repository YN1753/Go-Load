package engine

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

type Requester struct {
	URL           string `json:"url"`
	Concurrency   int    `json:"concurrency"`
	TotalRequests int    `json:"total_requests"`
	SuccessCount  int64  `json:"success_count"`
	FailureCount  int64  `json:"failure_count"`
}

func (r *Requester) Run() {
	fmt.Println("hello world")
	start := time.Now()
	jobs := make(chan struct{}, r.TotalRequests)
	var wg sync.WaitGroup
	for i := 0; i < r.Concurrency; i++ {
		wg.Add(1)
		go r.Work(jobs, &wg)

	}
	for i := 0; i < r.TotalRequests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	r.ListeningOut(start)
	r.Collect()

	wg.Wait()
	r.PrintReport(start)

}
func (r *Requester) Work(jobs <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	client := &http.Client{Timeout: 5 * time.Second}
	for range jobs {
		resp, err := client.Get(r.URL)
		if err != nil {
			atomic.AddInt64(&r.FailureCount, 1)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			atomic.AddInt64(&r.SuccessCount, 1)
		} else {
			atomic.AddInt64(&r.FailureCount, 1)
		}
	}
}
func (r *Requester) Collect() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		success := atomic.LoadInt64(&r.SuccessCount)
		failure := atomic.LoadInt64(&r.FailureCount)
		total := success + failure
		fmt.Printf("\r进度: %d/%d | 成功: %d | 失败: %d",
			total, r.TotalRequests, success, failure)
		if total >= int64(r.TotalRequests) {
			return
		}
	}
}
func (r *Requester) PrintReport(start time.Time) {
	dur := time.Since(start)
	fmt.Println("QPS:=", float64(r.FailureCount+r.SuccessCount)/dur.Seconds())
	fmt.Println(r)
}

func (r *Requester) ListeningOut(start time.Time) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt) // 监听 Ctrl+C

	go func() {
		<-sigChan
		fmt.Println("\n[!] 收到中断信号，正在结算已完成的请求...")
		r.PrintReport(start)
		os.Exit(0)
	}()
}
