package dashcam

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// jobStore tracks in-flight clip downloads.
//
// In-memory on purpose: a download is a short, interactive, retryable action,
// not durable state. If the service restarts mid-pull the job is lost and FMS
// simply starts another — cheaper than a table and a cleanup job for something
// that lives for seconds. Completed jobs are swept after a short grace so the
// map cannot grow without bound.
type jobStore struct {
	mu   sync.Mutex
	jobs map[string]*job
}

type job struct {
	ID     string
	Status string // downloading | uploading | done | failed
	S3Key  string
	Err    string
	done   time.Time // when it reached a terminal state, for sweeping
}

func newJobStore() *jobStore {
	s := &jobStore{jobs: map[string]*job{}}
	go s.sweep()
	return s
}

func (s *jobStore) create() *job {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	j := &job{ID: hex.EncodeToString(b), Status: "downloading"}
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	return j
}

func (s *jobStore) set(id, status, s3key, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return
	}
	j.Status, j.S3Key, j.Err = status, s3key, errMsg
	if status == "done" || status == "failed" {
		j.done = time.Now()
	}
}

func (s *jobStore) get(id string) (job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return job{}, false
	}
	return *j, true
}

// sweep drops terminal jobs 30 minutes after they finished — long enough for
// FMS to have polled the result, short enough to bound the map.
func (s *jobStore) sweep() {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-30 * time.Minute)
		s.mu.Lock()
		for id, j := range s.jobs {
			if !j.done.IsZero() && j.done.Before(cutoff) {
				delete(s.jobs, id)
			}
		}
		s.mu.Unlock()
	}
}
