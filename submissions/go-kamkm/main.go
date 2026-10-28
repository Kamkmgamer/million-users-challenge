package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = "80"
	maxDBConns  = 256
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

	poolSize := 8 * runtime.GOMAXPROCS(0)
	if poolSize < 16 {
		poolSize = 16
	}
	if poolSize > maxDBConns {
		poolSize = maxDBConns
	}
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
	// Warm only a small set of connections so the first requests do not pay all
	// setup latency while avoiding a large startup spike on the database host.
	poolConfig.MinConns = int32(minInt(poolSize, 16))
	poolConfig.MaxConnIdleTime = 10 * time.Minute
	poolConfig.MaxConnLifetime = 30 * time.Minute

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("open postgres pool: %v", err)
	}
	defer db.Close()

	application := &application{db: db, secret: []byte(jwtSecret), started: time.Now()}
	host := valueOr("HOST", defaultHost)
	port := valueOr("PORT", defaultPort)
	ports := []string{port}
	if extra := os.Getenv("EXTRA_PORT"); extra != "" && extra != port {
		ports = append(ports, extra)
	}

	servers := make([]*http.Server, len(ports))
	listeners := make([]net.Listener, len(ports))
	for i, p := range ports {
		address := net.JoinHostPort(host, p)
		listener, listenErr := (&net.ListenConfig{KeepAlive: -1}).Listen(ctx, "tcp", address)
		if listenErr != nil {
			log.Fatalf("listen %s: %v", address, listenErr)
		}
		listeners[i] = listener
		servers[i] = &http.Server{
			Handler:           application,
			IdleTimeout:       180 * time.Second,
			ReadHeaderTimeout: 15 * time.Second,
			MaxHeaderBytes:    16 << 10,
			ErrorLog:          log.New(os.Stderr, "http: ", log.LstdFlags),
		}
	}

	log.Printf("go-luna listening on %v (postgres pool %d)", ports, poolSize)
	var wait sync.WaitGroup
	serveErrors := make(chan error, len(servers))
	for i := range servers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			if serveErr := servers[index].Serve(listeners[index]); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
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
		_ = server.Shutdown(shutdownContext)
	}
	wait.Wait()
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
