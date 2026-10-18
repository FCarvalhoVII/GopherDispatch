package queue

import (
	"testing"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
)

func TestStreamForPriority(t *testing.T) {
	tests := []struct {
		priority job.Priority
		want     string
	}{
		{job.PriorityHigh, StreamHigh}, {job.PriorityMedium, StreamMedium}, {job.PriorityLow, StreamLow},
	}
	for _, test := range tests {
		got, err := StreamFor(test.priority)
		if err != nil || got != test.want {
			t.Fatalf("StreamFor(%s) = %q, %v", test.priority, got, err)
		}
	}
}

func TestStreamForRejectsInvalidPriority(t *testing.T) {
	if _, err := StreamFor("unknown"); err == nil {
		t.Fatal("invalid priority accepted")
	}
}
