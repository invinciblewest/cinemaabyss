package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	port         string
	kafkaBroker  string
	movieTopic   string
	paymentTopic string
	userTopic    string
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type App struct {
	writer *kafka.Writer
	config *Config
}

type writeResult struct {
	partition int
	offset    int64
	err       error
}

func NewApp(cfg *Config) *App {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.kafkaBroker),
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	writer.Completion = func(messages []kafka.Message, err error) {
		for _, m := range messages {
			if ch, ok := m.WriterData.(chan writeResult); ok {
				if err != nil {
					ch <- writeResult{err: err}
				} else {
					ch <- writeResult{partition: m.Partition, offset: m.Offset}
				}
				close(ch)
			}
		}
	}
	return &App{
		writer: writer,
		config: cfg,
	}
}

func (app *App) wrapPOST(h func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h(w, r)
	}
}

func (app *App) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (app *App) handleMovie(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(r)
	if err != nil || !validateRequired(raw, "movie_id", "title", "action") {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	app.publish(w, "movie", app.config.movieTopic, raw)
}

func (app *App) handleUser(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(r)
	if err != nil || !validateRequired(raw, "user_id", "action", "timestamp") {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	app.publish(w, "user", app.config.userTopic, raw)
}

func (app *App) handlePayment(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(r)
	if err != nil || !validateRequired(raw, "payment_id", "user_id", "amount", "status", "timestamp") {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	app.publish(w, "payment", app.config.paymentTopic, raw)
}

func (app *App) publish(w http.ResponseWriter, kind, topic string, payload json.RawMessage) {
	id := kind + "-" + uuid.NewString()
	ev := Event{
		ID:        id,
		Type:      kind,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	body, _ := json.Marshal(ev)

	resCh := make(chan writeResult, 1)

	msg := kafka.Message{
		Topic:      topic,
		Key:        []byte(kind),
		Value:      body,
		Time:       time.Now(),
		WriterData: resCh,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := app.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("kafka write error: %v", err)
		writeError(w, http.StatusInternalServerError, "kafka write error")
		return
	}

	res := <-resCh
	if res.err != nil {
		log.Printf("kafka completion error: %v", res.err)
		writeError(w, http.StatusInternalServerError, "kafka write error")
		return
	}

	resp := EventResponse{
		Status:    "success",
		Partition: res.partition,
		Offset:    res.offset,
		Event:     ev,
	}
	writeJSON(w, http.StatusCreated, resp)
	log.Printf("event %s has been published", id)
}

func main() {
	config := getConfig()
	addr := ":" + config.port

	app := NewApp(config)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", app.Health)
	mux.HandleFunc("/api/events/movie", app.wrapPOST(app.handleMovie))
	mux.HandleFunc("/api/events/user", app.wrapPOST(app.handleUser))
	mux.HandleFunc("/api/events/payment", app.wrapPOST(app.handlePayment))

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("events service listening on %s", addr)
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
	log.Println("events service stopped")
}

func getConfig() *Config {
	return &Config{
		port:         getEnv("PORT", "8082"),
		kafkaBroker:  getEnv("KAFKA_BROKERS", "localhost:9092"),
		movieTopic:   getEnv("MOVIE_TOPIC", "movie-events"),
		paymentTopic: getEnv("PAYMENT_TOPIC", "payment-events"),
		userTopic:    getEnv("USER_TOPIC", "user-events"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func validateRequired(raw []byte, fields ...string) bool {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	for _, f := range fields {
		v, ok := m[f]
		if !ok {
			return false
		}
		switch vv := v.(type) {
		case string:
			if strings.TrimSpace(vv) == "" {
				return false
			}
		case float64:
		}
	}
	return true
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return ioReadAll(r.Body)
}

func ioReadAll(r io.Reader) ([]byte, error) {
	const m = 10 << 20
	var buf []byte
	tmp := make([]byte, 32*1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if len(buf) > m {
				return nil, errors.New("body too large")
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return buf, nil
}
