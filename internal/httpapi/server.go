package httpapi

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Surya-Sastry/tab/internal/auth"
	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/observability"
	"github.com/Surya-Sastry/tab/internal/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type Server struct {
	Store          *postgres.Store
	Activity       *mongo.Collection
	ReplayWriter   *kafka.Writer
	JWTSigningKey  string
	AdminReplayKey string
	Logger         *slog.Logger
	Metrics        *observability.Metrics
	Hub            *Hub
	loginLimiter   *rateLimiter
	dummyHash      string
}

type contextKey string

const userKey contextKey = "userID"

func New(store *postgres.Store, jwtKey, adminKey string, logger *slog.Logger, metrics *observability.Metrics) *Server {
	random := make([]byte, 24)
	_, _ = rand.Read(random)
	dummyHash, _ := auth.HashPassword(base64.RawURLEncoding.EncodeToString(random))
	return &Server{
		Store: store, JWTSigningKey: jwtKey, AdminReplayKey: adminKey,
		Logger: logger, Metrics: metrics, Hub: NewHub(), loginLimiter: newRateLimiter(10, time.Minute),
		dummyHash: dummyHash,
	}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(s.requestContext, s.accessLog, s.recoverer)
	r.Get("/health/live", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "live"}) })
	r.Get("/health/ready", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Post("/api/v1/auth/register", s.register)
	r.Post("/api/v1/auth/login", s.login)
	r.Group(func(protected chi.Router) {
		protected.Use(s.authenticate)
		protected.Get("/api/v1/auth/me", s.me)
		protected.Post("/api/v1/groups", s.createGroup)
		protected.Get("/api/v1/groups", s.listGroups)
		protected.Get("/api/v1/groups/{groupID}", s.getGroup)
		protected.Get("/api/v1/groups/{groupID}/members", s.listMembers)
		protected.Post("/api/v1/groups/{groupID}/members", s.addMember)
		protected.Delete("/api/v1/groups/{groupID}/members/{memberID}", s.removeMember)
	})
	return r
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name, Email, Password string
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	hash, err := auth.HashPassword(request.Password)
	if err != nil {
		writeError(w, err)
		return
	}
	user, err := s.Store.CreateUser(r.Context(), request.Name, request.Email, hash)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, user)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if !s.loginLimiter.Allow(host) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "try again later"})
		return
	}
	var request struct{ Email, Password string }
	if !decodeJSON(w, r, &request) {
		return
	}
	user, hash, err := s.Store.UserByEmail(r.Context(), request.Email)
	valid := false
	if err != nil {
		_ = auth.VerifyPassword(s.dummyHash, request.Password)
	} else {
		valid = auth.VerifyPassword(hash, request.Password)
	}
	if !valid {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	token, err := auth.Issue(s.JWTSigningKey, user.ID, 15*time.Minute)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"accessToken": token, "expiresIn": 900})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	user, err := s.Store.UserByID(r.Context(), userID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, user)
}

func (s *Server) createGroup(w http.ResponseWriter, r *http.Request) {
	var request struct{ Name, Currency string }
	if !decodeJSON(w, r, &request) {
		return
	}
	group, err := s.Store.CreateGroup(r.Context(), userID(r), request.Name, request.Currency)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, group)
}

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.Store.ListGroups(r.Context(), userID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, groups)
}

func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "groupID")
	groups, err := s.Store.ListGroups(r.Context(), userID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	for _, group := range groups {
		if group.ID == id {
			writeJSON(w, 200, group)
			return
		}
	}
	writeError(w, domain.ErrNotFound)
}

func (s *Server) addMember(w http.ResponseWriter, r *http.Request) {
	var request struct {
		UserID string `json:"userId"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if err := s.Store.AddMember(r.Context(), chi.URLParam(r, "groupID"), userID(r), request.UserID); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listMembers(w http.ResponseWriter, r *http.Request) {
	members, err := s.Store.ListGroupMembers(r.Context(), chi.URLParam(r, "groupID"), userID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, members)
}

func (s *Server) removeMember(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.RemoveMember(r.Context(), chi.URLParam(r, "groupID"), userID(r), chi.URLParam(r, "memberID")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		id, err := auth.Parse(s.JWTSigningKey, value)
		if err != nil {
			writeError(w, domain.ErrUnauthenticated)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, id)))
	})
}

func (s *Server) requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Correlation-ID"))
		if _, err := uuid.Parse(id); err != nil {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey("correlationID"), id)))
	})
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(recorder, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unknown"
		}
		s.Metrics.ObserveHTTP(r.Method, route, recorder.status, time.Since(start))
		s.Logger.Info("http request", "method", r.Method, "route", route,
			"status", recorder.status, "durationMs", time.Since(start).Milliseconds(),
			"correlationId", correlationID(r))
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.Logger.Error("http panic", "correlationId", correlationID(r), "error", fmt.Sprint(recovered))
				writeJSON(w, 500, map[string]string{"error": "internal error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if contentType := r.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON body"})
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "internal error"
	switch {
	case errors.Is(err, domain.ErrInvalid):
		status, message = 400, err.Error()
	case errors.Is(err, domain.ErrUnauthenticated):
		status, message = 401, "authentication required"
	case errors.Is(err, domain.ErrForbidden):
		status, message = 403, "forbidden"
	case errors.Is(err, domain.ErrNotFound):
		status, message = 404, "not found"
	case errors.Is(err, domain.ErrConflict):
		status, message = 409, err.Error()
	}
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func userID(r *http.Request) string {
	value, _ := r.Context().Value(userKey).(string)
	return value
}

func correlationID(r *http.Request) string {
	value, _ := r.Context().Value(contextKey("correlationID")).(string)
	return value
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid Idempotency-Key is required"})
		return "", false
	}
	return key, true
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijacking not supported")
	}
	return hijacker.Hijack()
}

type rateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	entries map[string]*rateEntry
}

type rateEntry struct {
	start time.Time
	count int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, entries: make(map[string]*rateEntry)}
}

func (l *rateLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	entry := l.entries[key]
	if entry == nil || now.Sub(entry.start) >= l.window {
		l.entries[key] = &rateEntry{start: now, count: 1}
		return true
	}
	entry.count++
	return entry.count <= l.limit
}
