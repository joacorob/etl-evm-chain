package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/sirupsen/logrus"
)

// Server encapsulates the HTTP server, router and job registry.
type Server struct {
	mux *http.ServeMux
	mu  sync.RWMutex
	jobs map[string]*jobEntry
}

type jobEntry struct {
	status *JobStatus
	cancel context.CancelFunc // allows cancellation via DELETE /jobs/{id}
}

// NewServer builds a server with basic logging and panic recovery middlewares.
func NewServer() *Server {
	mux := http.NewServeMux()
	s := &Server{
		mux:  mux,
		jobs: make(map[string]*jobEntry),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/jobs", s.handleJobs)              // POST /jobs
	s.mux.HandleFunc("/jobs/", s.handleJobByID)          // GET/DELETE /jobs/{id}
}

// Run starts the HTTP server on the provided port.
func (s *Server) Run(port string) error {
	addr := fmt.Sprintf(":%s", port)
	handler := s.recoveryMiddleware(s.loggingMiddleware(s.mux))
	logrus.Infof("HTTP server running on %s", addr)
	return http.ListenAndServe(addr, handler)
}

// Simple request logger middleware.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// recoveryMiddleware catches panics and returns 500.
func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logrus.Errorf("panic recovered: %v", rec)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
} 