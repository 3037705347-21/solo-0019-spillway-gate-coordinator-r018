package domain

import (
	"strings"
	"testing"
	"time"
)

func testApprovedCommand(t *testing.T, now time.Time) GateCommand {
	t.Helper()
	command, err := NewGateCommand("cmd-1", "gate-1", 2.0, 0.5, 4.0, "balance", "dispatcher", now)
	if err != nil {
		t.Fatalf("NewGateCommand: %v", err)
	}
	decision := InterlockDecision{
		Verdict:    InterlockClear,
		Verifier:   "safety",
		Note:       "clear",
		ReviewedAt: now,
	}
	if err := command.Review(decision, "issued-token", now); err != nil {
		t.Fatalf("Review: %v", err)
	}
	return command
}

func TestFailedBeginDoesNotConsumeToken(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	for _, tc := range []struct {
		name     string
		token    string
		operator string
	}{
		{name: "blank operator", token: "issued-token", operator: "   "},
		{name: "wrong token", token: "other-token", operator: "chen"},
		{name: "blank token", token: "", operator: "chen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := testApprovedCommand(t, now)
			err := command.BeginExecution(tc.token, tc.operator, "req-1", now)
			if err == nil {
				t.Fatal("expected rejection, got nil")
			}
			if command.Status != CommandApproved {
				t.Fatalf("status changed after rejection: %s", command.Status)
			}
			if command.ExecutionToken != "issued-token" {
				t.Fatalf("token consumed after rejection: %q", command.ExecutionToken)
			}
			if command.ExecutionReceipt != nil {
				t.Fatal("receipt written after rejected execution")
			}
			if command.Version != 2 {
				t.Fatalf("version advanced after rejection: %d", command.Version)
			}
		})
	}
}

func TestTokenCannotRestartFinishedCommandButSameRequestConfirms(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	command := testApprovedCommand(t, now)

	if err := command.BeginExecution("issued-token", "chen", "req-1", now); err != nil {
		t.Fatalf("BeginExecution: %v", err)
	}
	if command.ExecutionToken != "" {
		t.Fatalf("live token must be erased at the boundary, got %q", command.ExecutionToken)
	}
	gate := Gate{ID: "gate-1", Status: GateMoving, Aperture: 0.5, MaxAperture: 4, ActiveCommandID: "cmd-1"}
	if err := gate.Settle(2.0, false, now); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if err := command.Complete(gate, "req-1", now); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Starting another action with the spent token is a terminal denial, not a
	// re-execution.
	err := command.BeginExecution("issued-token", "chen", "req-2", now)
	if err == nil {
		t.Fatal("spent token started a second execution")
	}
	if !strings.Contains(err.Error(), "command_already_completed") {
		t.Fatalf("expected command_already_completed, got %v", err)
	}

	// The same request retries confirm the original terminal state without any
	// state change.
	snapshot := command
	receipt, ok := command.TerminalReceipt("issued-token", "req-1")
	if !ok {
		t.Fatal("same-request retry could not confirm the terminal state")
	}
	if receipt.Status != CommandCompleted {
		t.Fatalf("receipt status = %s", receipt.Status)
	}
	if receipt.Gate.Aperture != 2.0 || receipt.Gate.Status != GateIdle {
		t.Fatalf("receipt gate snapshot diverged: %+v", receipt.Gate)
	}
	if command.Status != snapshot.Status || command.Version != snapshot.Version {
		t.Fatal("confirmation mutated the command")
	}

	for _, tc := range []struct{ token, requestKey string }{
		{"issued-token", "req-other"},
		{"wrong-token", "req-1"},
		{"", "req-1"},
	} {
		if _, ok := command.TerminalReceipt(tc.token, tc.requestKey); ok {
			t.Fatalf("confirmation accepted token=%q key=%q", tc.token, tc.requestKey)
		}
	}
}
