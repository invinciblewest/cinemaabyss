package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	port                   string
	monolithUrl            *url.URL
	moviesServiceUrl       *url.URL
	eventsServiceUrl       *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
}

type upstream struct {
	name  string
	url   *url.URL
	proxy *httputil.ReverseProxy
}

func main() {
	config := getConfig()
	addr := ":" + config.port

	monolithUpstream := upstream{
		name:  "monolith",
		url:   config.monolithUrl,
		proxy: buildReverseProxy(config.monolithUrl),
	}
	moviesUpstream := upstream{
		name:  "movies",
		url:   config.moviesServiceUrl,
		proxy: buildReverseProxy(config.moviesServiceUrl),
	}
	eventsUpstream := upstream{
		name:  "events",
		url:   config.eventsServiceUrl,
		proxy: buildReverseProxy(config.eventsServiceUrl),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]bool{"status": true})
		if err != nil {
			log.Println("health check error:", err)
			return
		}
		log.Println("health check ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		var target upstream

		switch {
		case strings.Contains(r.URL.Path, "/api/events"):
			target = eventsUpstream
		case strings.Contains(r.URL.Path, "/api/movies/health"):
			target = moviesUpstream
		case strings.Contains(r.URL.Path, "/api/movies"):
			if config.gradualMigration {
				if config.moviesMigrationPercent <= 0 {
					target = monolithUpstream
				} else if config.moviesMigrationPercent >= 100 {
					target = moviesUpstream
				}

				if int(math.Floor(rand.Float64()*100)) < config.moviesMigrationPercent {
					target = moviesUpstream
				} else {
					target = monolithUpstream
				}
			} else {
				target = monolithUpstream
			}
		default:
			target = monolithUpstream
		}

		rec := &statusRecorder{ResponseWriter: w, status: 200}
		target.proxy.ServeHTTP(rec, r)

		log.Printf("method=%s path=%s upstream=%s status=%d dur=%s", r.Method, r.URL.Path, target.name, rec.status, time.Since(start))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("proxy listening on %s (monolith=%s, events=%s, movies=%s, MOVIES_MIGRATION_PERCENT=%d)",
			addr, config.monolithUrl, config.eventsServiceUrl, config.moviesServiceUrl, config.moviesMigrationPercent)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	log.Println("proxy stopped")
}

func getConfig() *Config {
	moviesMigrationPercent, err := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))
	if err != nil {
		moviesMigrationPercent = 0
	}
	return &Config{
		port:                   getEnv("PORT", "8000"),
		monolithUrl:            mustParseURL(getEnv("MONOLITH_URL", "http://monolith:8080")),
		moviesServiceUrl:       mustParseURL(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")),
		eventsServiceUrl:       mustParseURL(getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")),
		gradualMigration:       getEnv("GRADUAL_MIGRATION", "true") == "true",
		moviesMigrationPercent: moviesMigrationPercent,
	}
}

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		log.Fatalf("invalid URL %q: %v", s, err)
	}
	return u
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func buildReverseProxy(target *url.URL) *httputil.ReverseProxy {
	reverseProxy := httputil.NewSingleHostReverseProxy(target)

	reverseProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error upstream=%s path=%s err=%v", target.Host, r.URL.Path, err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}

	reverseProxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
	}

	return reverseProxy
}
