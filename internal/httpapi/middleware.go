package httpapi

import (
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

func recoverMiddleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Error("panic recovered", "value", v)
				writeError(w, 500, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type visitor struct {
	count int
	reset time.Time
}
type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]visitor
	limit    int
	window   time.Duration
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{visitors: make(map[string]visitor), limit: limit, window: window}
}
func (l *rateLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		now := time.Now()
		l.mu.Lock()
		v := l.visitors[host]
		if now.After(v.reset) {
			v = visitor{reset: now.Add(l.window)}
		}
		v.count++
		l.visitors[host] = v
		allowed := v.count <= l.limit
		l.mu.Unlock()
		if !allowed {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
