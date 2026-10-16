package job

import (
	"encoding/json"
	"testing"
)

func TestPriorityValid(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		priority Priority
		want     bool
	}{
		{PriorityHigh, true}, {PriorityMedium, true}, {PriorityLow, true}, {Priority("urgent"), false},
	} {
		if got := tc.priority.Valid(); got != tc.want {
			t.Errorf("Priority(%q).Valid() = %v, want %v", tc.priority, got, tc.want)
		}
	}
}

func TestCreateInputValidate(t *testing.T) {
	t.Parallel()
	valid := CreateInput{Type: "send-email", Payload: json.RawMessage(`{"to":"a@example.com"}`), Priority: PriorityHigh, MaxAttempts: 3}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	invalid := valid
	invalid.Priority = "urgent"
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid priority accepted")
	}
}
