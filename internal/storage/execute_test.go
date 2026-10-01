package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

func setupApprovedCommand(t *testing.T, repo *MemoryRepository, gateID string, target float64) string {
	t.Helper()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()

	gate, err := domain.NewGate(gateID, "测试闸", 4.0, 0.5, now)
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	if err := repo.CreateGate(ctx, gate); err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	command, err := domain.NewGateCommand("cmd-"+gateID, gateID, target, 0.5, 4.0, "balance", "dispatcher", now)
	if err != nil {
		t.Fatalf("NewGateCommand: %v", err)
	}
	if err := repo.CreateCommand(ctx, command); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	decision, err := domain.NewInterlockDecision("clear", "safety", "clear", now)
	if err != nil {
		t.Fatalf("NewInterlockDecision: %v", err)
	}
	token := "issued-token"
	if _, _, err := repo.ReviewCommand(ctx, command.ID, decision, token, now); err != nil {
		t.Fatalf("ReviewCommand: %v", err)
	}
	return command.ID
}

func TestRejectedExecutionLeavesTokenAndReservationIntact(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()
	commandID := setupApprovedCommand(t, repo, "gate-a", 2.0)

	if _, _, _, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "  ", false, "req-1", now); err == nil {
		t.Fatal("blank operator must be rejected")
	}
	if _, _, _, err := repo.ExecuteCommand(ctx, commandID, "wrong-token", "chen", false, "req-1", now); err == nil {
		t.Fatal("wrong token must be rejected")
	}

	// Nothing took effect: the command stays approved with its token, the gate
	// stays reserved, and the same request can still execute.
	command, err := repo.Command(ctx, commandID)
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if command.Status != domain.CommandApproved {
		t.Fatalf("command status after rejections: %s", command.Status)
	}
	if command.ExecutionToken != "issued-token" {
		t.Fatalf("token consumed after rejections: %q", command.ExecutionToken)
	}
	gate, err := repo.Gate(ctx, "gate-a")
	if err != nil {
		t.Fatalf("Gate: %v", err)
	}
	if gate.Status != domain.GateReserved || gate.ActiveCommandID != commandID {
		t.Fatalf("reservation changed after rejections: %+v", gate)
	}

	executed, settled, replayed, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, "req-1", now)
	if err != nil {
		t.Fatalf("valid execution after prior rejections: %v", err)
	}
	if replayed {
		t.Fatal("first successful execution must not be a replay")
	}
	if executed.Status != domain.CommandCompleted || settled.Status != domain.GateIdle {
		t.Fatalf("unexpected outcome: command=%s gate=%s", executed.Status, settled.Status)
	}
}

func TestSameRequestRetriesConfirmOriginalResult(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()
	commandID := setupApprovedCommand(t, repo, "gate-b", 2.0)

	command, gate, _, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, "req-1", now)
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}
	finishedVersion := command.Version
	finishedGateVersion := gate.Version

	for i := 0; i < 3; i++ {
		retryCommand, retryGate, replay, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, "req-1", now.Add(time.Duration(i+1)*time.Second))
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if !replay {
			t.Fatalf("retry %d must be flagged as an idempotent replay", i)
		}
		if retryCommand.Status != domain.CommandCompleted || retryCommand.Version != finishedVersion {
			t.Fatalf("retry %d diverged: status=%s version=%d", i, retryCommand.Status, retryCommand.Version)
		}
		if retryGate.Version != finishedGateVersion || retryGate.Aperture != 2.0 {
			t.Fatalf("retry %d gate diverged: %+v", i, retryGate)
		}
	}

	// A new request identity reusing the spent token cannot start anything.
	if _, _, _, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, "req-2", now); err == nil {
		t.Fatal("spent token under a new request identity was accepted")
	}
}

func TestConcurrentRetriesCrossBoundaryExactlyOnce(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()
	commandID := setupApprovedCommand(t, repo, "gate-c", 2.0)

	const n = 32
	type outcome struct {
		replayed bool
		err      error
	}
	outcomes := make([]outcome, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, replayed, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, "req-1", now)
			outcomes[i] = outcome{replayed, err}
		}(i)
	}
	close(start)
	wg.Wait()

	executions := 0
	for _, outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("concurrent retry failed: %v", outcome.err)
		}
		if !outcome.replayed {
			executions++
		}
	}
	if executions != 1 {
		t.Fatalf("expected exactly one boundary crossing, got %d", executions)
	}

	gate, err := repo.Gate(ctx, "gate-c")
	if err != nil {
		t.Fatalf("Gate: %v", err)
	}
	if gate.Status != domain.GateIdle || gate.ActiveCommandID != "" || gate.Aperture != 2.0 {
		t.Fatalf("gate after concurrent retries: %+v", gate)
	}
}
func TestCommandsOnDifferentGatesDoNotPreemptEachOther(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()
	commandA := setupApprovedCommand(t, repo, "gate-d", 2.0)
	commandB := setupApprovedCommand(t, repo, "gate-e", 3.0)

	type result struct {
		replayed bool
		err      error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	run := func(commandID, requestKey string) {
		defer wg.Done()
		<-start
		_, _, replayed, err := repo.ExecuteCommand(ctx, commandID, "issued-token", "chen", false, requestKey, now)
		results <- result{replayed, err}
	}
	wg.Add(2)
	go run(commandA, "req-a")
	go run(commandB, "req-b")
	close(start)
	wg.Wait()
	close(results)

	for r := range results {
		if r.err != nil {
			t.Fatalf("cross-gate execute failed: %v", r.err)
		}
		if r.replayed {
			t.Fatal("a first execution on its own gate must not be reported as a replay")
		}
	}

	gateA, err := repo.Gate(ctx, "gate-d")
	if err != nil {
		t.Fatalf("Gate A: %v", err)
	}
	gateB, err := repo.Gate(ctx, "gate-e")
	if err != nil {
		t.Fatalf("Gate B: %v", err)
	}
	if gateA.Status != domain.GateIdle || gateA.Aperture != 2.0 || gateA.ActiveCommandID != "" {
		t.Fatalf("gate A diverged: %+v", gateA)
	}
	if gateB.Status != domain.GateIdle || gateB.Aperture != 3.0 || gateB.ActiveCommandID != "" {
		t.Fatalf("gate B diverged: %+v", gateB)
	}
}

func TestConcurrentClearReviewsHaveSingleWinner(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()

	gate, err := domain.NewGate("gate-f", "竞争闸", 4.0, 0.5, now)
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	if err := repo.CreateGate(ctx, gate); err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	createPending := func(id string, target float64) string {
		command, err := domain.NewGateCommand(id, "gate-f", target, 0.5, 4.0, "balance", "dispatcher", now)
		if err != nil {
			t.Fatalf("NewGateCommand: %v", err)
		}
		if err := repo.CreateCommand(ctx, command); err != nil {
			t.Fatalf("CreateCommand: %v", err)
		}
		return id
	}
	first := createPending("cmd-first", 2.0)
	second := createPending("cmd-second", 3.0)
	decision, err := domain.NewInterlockDecision("clear", "safety", "clear", now)
	if err != nil {
		t.Fatalf("NewInterlockDecision: %v", err)
	}

	approve := func(id string, approved chan<- bool) {
		_, _, reviewErr := repo.ReviewCommand(ctx, id, decision, "token-"+id, now)
		approved <- reviewErr == nil
	}
	results := make(chan bool, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; approve(first, results) }()
	go func() { defer wg.Done(); <-start; approve(second, results) }()
	close(start)
	wg.Wait()
	close(results)

	winners := 0
	for won := range results {
		if won {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one winning clear review, got %d", winners)
	}

	storedGate, err := repo.Gate(ctx, "gate-f")
	if err != nil {
		t.Fatalf("Gate: %v", err)
	}
	if storedGate.Status != domain.GateReserved {
		t.Fatalf("gate status: %s", storedGate.Status)
	}
	if storedGate.ActiveCommandID != first && storedGate.ActiveCommandID != second {
		t.Fatalf("gate references unknown command: %s", storedGate.ActiveCommandID)
	}
	winnerID := storedGate.ActiveCommandID
	loserID := second
	if winnerID == second {
		loserID = first
	}
	winner, err := repo.Command(ctx, winnerID)
	if err != nil {
		t.Fatalf("winner Command: %v", err)
	}
	loser, err := repo.Command(ctx, loserID)
	if err != nil {
		t.Fatalf("loser Command: %v", err)
	}
	if winner.Status != domain.CommandApproved || winner.ExecutionToken == "" {
		t.Fatalf("winner was not approved with a token: %+v", winner)
	}
	if loser.Status != domain.CommandPending || loser.ExecutionToken != "" {
		t.Fatalf("loser must stay pending without a token: status=%s token=%q", loser.Status, loser.ExecutionToken)
	}
}
