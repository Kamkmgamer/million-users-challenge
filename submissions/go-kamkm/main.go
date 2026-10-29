package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = "80"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET is required")
	}

	// Read batch concurrency bounds active sessions. DB_POOL is the shared
	// ceiling for reads, the reserved writer, and profile lookups.
	poolSize := 128
	if value := os.Getenv("DB_POOL"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 512 {
			log.Fatalf("invalid DB_POOL=%q (must be 1..512)", value)
		}
		poolSize = parsed
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("invalid DATABASE_URL: %v", err)
	}
	poolConfig.MaxConns = int32(poolSize)
	poolConfig.ConnConfig.RuntimeParams["jit"] = "off"
	poolConfig.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	// Warm only a small set of connections so the first requests do not pay all
	// setup latency while avoiding a large startup spike on the database host.
	poolConfig.MinConns = int32(minInt(poolSize, 32))
	poolConfig.MaxConnIdleTime = 10 * time.Minute
	poolConfig.MaxConnLifetime = 30 * time.Minute

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("open postgres pool: %v", err)
	}
	defer db.Close()

	application := &application{db: db, reads: newReadBatcher(db), writes: newWriteBatcher(db), secret: []byte(jwtSecret), started: time.Now()}
	host := valueOr("HOST", defaultHost)
	port := valueOr("PORT", defaultPort)
	ports := []string{port}
	if extra := os.Getenv("EXTRA_PORT"); extra != "" && extra != port {
		ports = append(ports, extra)
	}

	servers := make([]*fasthttp.Server, len(ports))
	listeners := make([]net.Listener, len(ports))
	for i, p := range ports {
		address := net.JoinHostPort(host, p)
		listener, listenErr := (&net.ListenConfig{KeepAlive: -1}).Listen(ctx, "tcp", address)
		if listenErr != nil {
			log.Fatalf("listen %s: %v", address, listenErr)
		}
		listeners[i] = listener
		servers[i] = &fasthttp.Server{
			Handler:            fasthttpadaptor.NewFastHTTPHandler(application),
			Concurrency:        2000000,
			ReduceMemoryUsage:  true,
			IdleTimeout:        180 * time.Second,
			ReadTimeout:        15 * time.Second,
			ReadBufferSize:     16 << 10,
			MaxRequestBodySize: maxBodyBytes,
			CloseOnShutdown:    true,
			ErrorHandler: func(ctx *fasthttp.RequestCtx, err error) {
				ctx.SetContentType("application/json")
				ctx.SetStatusCode(400)
				ctx.SetBodyString(`{"error":"malformed JSON body"}`)
			},
		}
	}

	log.Printf("go-kamkm listening on %v (postgres pool %d)", ports, poolSize)
	var wait sync.WaitGroup
	serveErrors := make(chan error, len(servers))
	for i := range servers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			if serveErr := servers[index].Serve(listeners[index]); serveErr != nil {
				serveErrors <- serveErr
			}
		}(i)
	}

	select {
	case <-ctx.Done():
	case serveErr := <-serveErrors:
		log.Printf("HTTP server stopped: %v", serveErr)
		stop()
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		_ = server.ShutdownWithContext(shutdownContext)
	}
	wait.Wait()
	application.writes.close()
}

func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
