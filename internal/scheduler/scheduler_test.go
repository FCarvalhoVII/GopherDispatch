package scheduler

import (
	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"testing"
)

func TestWeightedCycle(t *testing.T) {
	s, err := New(Weights{High: 5, Medium: 3, Low: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := []job.Priority{job.PriorityHigh, job.PriorityHigh, job.PriorityHigh, job.PriorityHigh, job.PriorityHigh, job.PriorityMedium, job.PriorityMedium, job.PriorityMedium, job.PriorityLow}
	for i, p := range want {
		if got := s.Next(); got != p {
			t.Fatalf("step %d: got %s want %s", i, got, p)
		}
	}
}
func TestInvalidWeights(t *testing.T) {
	if _, err := New(Weights{High: 1, Medium: 0, Low: 1}); err == nil {
		t.Fatal("zero weight accepted")
	}
}
func BenchmarkNext(b *testing.B) {
	s, _ := New(Weights{High: 5, Medium: 3, Low: 1})
	for i := 0; i < b.N; i++ {
		s.Next()
	}
}
