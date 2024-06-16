package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServers = make([]string, 3)
)

var (
	SumOfBytes = make(map[string]int64)
	mu         sync.Mutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, refuse := context.WithTimeout(context.Background(), timeout)
	defer refuse()
	req, err := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, refuse := context.WithTimeout(r.Context(), timeout)
	defer refuse()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		defer resp.Body.Close()
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Printf("fwd %d %s", resp.StatusCode, resp.Request.URL)
		body := resp.Body
		defer body.Close()
		buf := make([]byte, 4096)
		count, err := io.CopyBuffer(rw, body, buf)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		log.Printf("Sent %d bytes in response to %s", count, r.RemoteAddr)
		mu.Lock()
		SumOfBytes[dst] += count
		mu.Unlock()
		rw.WriteHeader(resp.StatusCode)
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	healthChecker := &HealthChecker{}
	healthChecker.health = health
	healthChecker.serversPool = serversPool
	healthChecker.healthyServers = healthyServers
	healthChecker.checkInterval = 10 * time.Second

	balancer := &Balancer{}
	balancer.healthChecker = healthChecker
	balancer.forward = forward

	balancer.Start()
}

type Balancer struct {
	healthChecker *HealthChecker
	forward       func(string, http.ResponseWriter, *http.Request) error
}

func (b *Balancer) getServerIndexWithLowestLoad(serverLoad map[string]int64, serversPool []string) int {
	mu.Lock()
	defer mu.Unlock()

	minLoad := int64(^uint64(0) >> 1)
	var minLoadServer int

	for i, server := range serversPool {
		load := serverLoad[server]
		if load < minLoad {
			minLoad = load
			minLoadServer = i
		}
	}
	return minLoadServer
}
func (b *Balancer) Start() {
	flag.Parse()

	b.healthChecker.StartHealthCheck()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		index := b.getServerIndexWithLowestLoad(SumOfBytes, b.healthChecker.GetHealthyServers())
		log.Println(SumOfBytes)
		_ = b.forward(b.healthChecker.GetHealthyServers()[index], rw, r)
	}))
	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

type HealthChecker struct {
	health         func(string) bool
	serversPool    []string
	healthyServers []string
	checkInterval  time.Duration
	healthyMu      sync.Mutex
}

func (hc *HealthChecker) StartHealthCheck() {
	for i, server := range hc.serversPool {
		server := server
		i := i
		go func() {
			for range time.Tick(hc.checkInterval) {
				isHealthy := hc.health(server)
				if !isHealthy {
					hc.serversPool[i] = ""
				} else {
					hc.serversPool[i] = server
				}

				hc.healthyServers = make([]string, 0)

				for _, value := range hc.serversPool {
					if value != "" {
						hc.healthyServers = append(hc.healthyServers, value)
					}
				}
				log.Println(server, isHealthy)
			}
		}()
	}
}

func (hc *HealthChecker) GetHealthyServers() []string {
	hc.healthyMu.Lock()
	defer hc.healthyMu.Unlock()
	return hc.healthyServers
}
