package main

import (
	"fmt"
	"net/http"
	"sync"
)

type executeOutcome struct {
	status int
	body   map[string]any
	err    error
}

func runExecuteGateCommand(client *apiClient) error {
	if err := checkSuccessfulExecution(client); err != nil {
		return err
	}
	if err := checkIneffectiveFailureKeepsToken(client); err != nil {
		return err
	}
	if err := checkFaultedExecutionReplay(client); err != nil {
		return err
	}

	fmt.Println("execute-gate-command: single execution, idempotent retries, and non-consuming failures verified")
	return nil
}

func executeCall(client *apiClient, commandID, token, operator string, fault bool) executeOutcome {
	result, err := client.post("/commands/"+commandID+"/execute", map[string]any{
		"operator":       operator,
		"token":          token,
		"simulate_fault": fault,
	})
	if err != nil {
		return executeOutcome{err: err}
	}
	return executeOutcome{status: result.Status, body: result.Body}
}

func checkSuccessfulExecution(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-west", "西侧泄洪闸", 4, 0.25); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-west", 2.0, "平衡下游水位", "dispatcher-zhao")
	if err != nil {
		return err
	}
	commandID, err := stringField(command, "id")
	if err != nil {
		return err
	}

	approved, err := fixtureClearReview(client, commandID, "safety-he")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	// Near-simultaneous identical retries of the same execution request:
	// exactly one may cross the execution boundary. Every response must be the
	// same terminal snapshot (200), proving the losers confirmed the original
	// result instead of moving the gate a second time.
	const burstSize = 6
	outcomes := make([]executeOutcome, burstSize)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range outcomes {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			outcomes[index] = executeCall(client, commandID, token, "operator-chen", false)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, outcome := range outcomes {
		if outcome.err != nil {
			return fmt.Errorf("concurrent retry %d: %w", i, outcome.err)
		}
		if outcome.status != http.StatusOK {
			return fmt.Errorf("concurrent retry %d: expected HTTP 200 replay, got %d with body %s",
				i, outcome.status, prettyBody(outcome.body))
		}
		if err := requireTerminalSnapshot(outcome.body, "completed", 2.0, "idle"); err != nil {
			return fmt.Errorf("concurrent retry %d: %w", i, err)
		}
	}
	if err := requireIdenticalSnapshots(outcomes); err != nil {
		return err
	}

	// A single boundary crossing increments the command exactly twice
	// (approved -> executing -> completed) and the gate exactly three times
	// (reserved -> moving -> idle). Double execution would show larger versions.
	reference := outcomes[0].body
	if err := requireVersion(reference, "command", 4); err != nil {
		return err
	}
	if err := requireVersion(reference, "gate", 4); err != nil {
		return err
	}

	// A later retry after a lost response still confirms the original terminal
	// state and performs no further movement.
	retry := executeCall(client, commandID, token, "operator-chen", false)
	if retry.err != nil {
		return retry.err
	}
	if retry.status != http.StatusOK {
		return fmt.Errorf("retry after lost response: expected HTTP 200, got %d with body %s",
			retry.status, prettyBody(retry.body))
	}
	if err := requireTerminalSnapshot(retry.body, "completed", 2.0, "idle"); err != nil {
		return fmt.Errorf("retry after lost response: %w", err)
	}
	if err := requireVersion(retry.body, "command", 4); err != nil {
		return err
	}
	if err := requireVersion(retry.body, "gate", 4); err != nil {
		return err
	}

	// A different token on a finished command can never start another movement.
	foreign := executeCall(client, commandID, "some-other-token", "operator-chen", false)
	if foreign.err != nil {
		return foreign.err
	}
	if err := requireStatus(apiResult{Status: foreign.status, Body: foreign.body}, http.StatusConflict,
		"reject foreign token after completion"); err != nil {
		return err
	}
	if err := requireVersion(foreign.body, "command", 4); err == nil {
		return fmt.Errorf("foreign token response unexpectedly carried a terminal command snapshot: %s",
			prettyBody(foreign.body))
	}

	gateResult, err := client.get("/gates/gate-west")
	if err != nil {
		return err
	}
	if err := requireStatus(gateResult, http.StatusOK, "read gate after retries"); err != nil {
		return err
	}
	gate, err := objectField(gateResult.Body, "gate")
	if err != nil {
		return err
	}
	status, err := stringField(gate, "status")
	if err != nil {
		return err
	}
	aperture, err := floatField(gate, "aperture")
	if err != nil {
		return err
	}
	version, err := floatField(gate, "version")
	if err != nil {
		return err
	}
	if status != "idle" || aperture != 2.0 || version != 4 {
		return fmt.Errorf("gate changed across replays: status=%q aperture=%v version=%v", status, aperture, version)
	}
	if active, exists := gate["active_command_id"]; exists && active != "" {
		return fmt.Errorf("gate still references active command after completion: %v", active)
	}
	return nil
}

func checkIneffectiveFailureKeepsToken(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-north", "北侧泄洪闸", 5, 1.0); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-north", 3.0, "失败不消耗令牌", "dispatcher-qin")
	if err != nil {
		return err
	}
	commandID, err := stringField(command, "id")
	if err != nil {
		return err
	}
	approved, err := fixtureClearReview(client, commandID, "safety-he")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	// A wrong token fails before the execution boundary and must not consume the
	// real token or move the gate.
	wrongToken := executeCall(client, commandID, "not-the-issued-token", "operator-chen", false)
	if wrongToken.err != nil {
		return wrongToken.err
	}
	if err := requireStatus(
		apiResult{Status: wrongToken.status, Body: wrongToken.body},
		http.StatusConflict, "reject wrong token before execution",
	); err != nil {
		return err
	}
	if err := requireCode(wrongToken.body, "invalid_execution_token"); err != nil {
		return err
	}
	if err := requireApprovedState(client, commandID, "gate-north"); err != nil {
		return fmt.Errorf("wrong token altered state: %w", err)
	}

	// A validation failure (empty operator) likewise fails before any write.
	emptyOperator := executeCall(client, commandID, token, "", false)
	if emptyOperator.err != nil {
		return emptyOperator.err
	}
	if err := requireStatus(
		apiResult{Status: emptyOperator.status, Body: emptyOperator.body},
		http.StatusBadRequest, "reject empty operator before execution",
	); err != nil {
		return err
	}
	if err := requireCode(emptyOperator.body, "invalid_operator"); err != nil {
		return err
	}
	if err := requireApprovedState(client, commandID, "gate-north"); err != nil {
		return fmt.Errorf("invalid operator altered state: %w", err)
	}

	// The original token must still drive the one genuine execution.
	success := executeCall(client, commandID, token, "operator-chen", false)
	if success.err != nil {
		return success.err
	}
	if err := requireStatus(
		apiResult{Status: success.status, Body: success.body},
		http.StatusOK, "execute after rejected attempts",
	); err != nil {
		return err
	}
	if err := requireTerminalSnapshot(success.body, "completed", 3.0, "idle"); err != nil {
		return err
	}
	return requireVersion(success.body, "command", 4)
}

func checkFaultedExecutionReplay(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-east", "东侧故障闸", 6, 0.5); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-east", 4.0, "故障开度调整", "dispatcher-zhao")
	if err != nil {
		return err
	}
	commandID, err := stringField(command, "id")
	if err != nil {
		return err
	}
	approved, err := fixtureClearReview(client, commandID, "safety-he")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	faulted := executeCall(client, commandID, token, "operator-chen", true)
	if faulted.err != nil {
		return faulted.err
	}
	if err := requireStatus(
		apiResult{Status: faulted.status, Body: faulted.body},
		http.StatusOK, "execute simulated fault",
	); err != nil {
		return err
	}
	if err := requireTerminalSnapshot(faulted.body, "aborted", 4.0, "faulted"); err != nil {
		return err
	}
	if err := requireVersion(faulted.body, "command", 4); err != nil {
		return err
	}
	if err := requireVersion(faulted.body, "gate", 4); err != nil {
		return err
	}

	// Retrying the aborted execution confirms the original terminal state; the
	// faulted gate must not be touched again.
	replay := executeCall(client, commandID, token, "operator-chen", true)
	if replay.err != nil {
		return replay.err
	}
	if replay.status != http.StatusOK {
		return fmt.Errorf("fault retry: expected HTTP 200 replay, got %d with body %s",
			replay.status, prettyBody(replay.body))
	}
	if err := requireTerminalSnapshot(replay.body, "aborted", 4.0, "faulted"); err != nil {
		return fmt.Errorf("fault replay snapshot: %w", err)
	}
	if err := requireVersion(replay.body, "command", 4); err != nil {
		return err
	}
	return requireVersion(replay.body, "gate", 4)
}

func requireTerminalSnapshot(body map[string]any, commandStatus string, aperture float64, gateStatus string) error {
	commandView, err := objectField(body, "command")
	if err != nil {
		return err
	}
	status, err := stringField(commandView, "status")
	if err != nil {
		return err
	}
	if status != commandStatus {
		return fmt.Errorf("command status: expected %q, got %q", commandStatus, status)
	}

	gateView, err := objectField(body, "gate")
	if err != nil {
		return err
	}
	status, err = stringField(gateView, "status")
	if err != nil {
		return err
	}
	if status != gateStatus {
		return fmt.Errorf("gate status: expected %q, got %q", gateStatus, status)
	}
	gotAperture, err := floatField(gateView, "aperture")
	if err != nil {
		return err
	}
	if gotAperture != aperture {
		return fmt.Errorf("gate aperture: expected %v, got %v", aperture, gotAperture)
	}
	if active, exists := gateView["active_command_id"]; exists && active != "" {
		return fmt.Errorf("settled gate still references active command %v", active)
	}
	return nil
}

func requireIdenticalSnapshots(outcomes []executeOutcome) error {
	reference := outcomes[0].body
	refCommand, err := objectField(reference, "command")
	if err != nil {
		return err
	}
	refGate, err := objectField(reference, "gate")
	if err != nil {
		return err
	}
	refCommandVersion, err := floatField(refCommand, "version")
	if err != nil {
		return err
	}
	refGateVersion, err := floatField(refGate, "version")
	if err != nil {
		return err
	}
	refUpdatedAt, err := stringField(refCommand, "updated_at")
	if err != nil {
		return err
	}
	refGateUpdatedAt, err := stringField(refGate, "updated_at")
	if err != nil {
		return err
	}

	for i := 1; i < len(outcomes); i++ {
		commandView, err := objectField(outcomes[i].body, "command")
		if err != nil {
			return err
		}
		gateView, err := objectField(outcomes[i].body, "gate")
		if err != nil {
			return err
		}
		commandVersion, err := floatField(commandView, "version")
		if err != nil {
			return err
		}
		gateVersion, err := floatField(gateView, "version")
		if err != nil {
			return err
		}
		if commandVersion != refCommandVersion || gateVersion != refGateVersion {
			return fmt.Errorf("retry %d changed versions: command %v->%v gate %v->%v",
				i, refCommandVersion, commandVersion, refGateVersion, gateVersion)
		}
		updatedAt, err := stringField(commandView, "updated_at")
		if err != nil {
			return err
		}
		gateUpdatedAt, err := stringField(gateView, "updated_at")
		if err != nil {
			return err
		}
		if updatedAt != refUpdatedAt || gateUpdatedAt != refGateUpdatedAt {
			return fmt.Errorf("retry %d returned a different terminal snapshot", i)
		}
	}
	return nil
}

func requireVersion(body map[string]any, field string, expected float64) error {
	view, err := objectField(body, field)
	if err != nil {
		return err
	}
	version, err := floatField(view, "version")
	if err != nil {
		return err
	}
	if version != expected {
		return fmt.Errorf("%s version: expected %v, got %v", field, expected, version)
	}
	return nil
}

func requireCode(body map[string]any, expected string) error {
	code, err := stringField(body, "code")
	if err != nil {
		return err
	}
	if code != expected {
		return fmt.Errorf("error code: expected %q, got %q", expected, code)
	}
	return nil
}

func requireApprovedState(client *apiClient, commandID, gateID string) error {
	commandResult, err := client.get("/commands/" + commandID)
	if err != nil {
		return err
	}
	if err := requireStatus(commandResult, http.StatusOK, "read command after failed attempt"); err != nil {
		return err
	}
	commandView, err := objectField(commandResult.Body, "command")
	if err != nil {
		return err
	}
	status, err := stringField(commandView, "status")
	if err != nil {
		return err
	}
	version, err := floatField(commandView, "version")
	if err != nil {
		return err
	}
	if status != "approved" || version != 2 {
		return fmt.Errorf("command after failed attempt: status=%q version=%v, want approved/2", status, version)
	}

	gateResult, err := client.get("/gates/" + gateID)
	if err != nil {
		return err
	}
	if err := requireStatus(gateResult, http.StatusOK, "read gate after failed attempt"); err != nil {
		return err
	}
	gateView, err := objectField(gateResult.Body, "gate")
	if err != nil {
		return err
	}
	gateStatus, err := stringField(gateView, "status")
	if err != nil {
		return err
	}
	activeCommandID, err := stringField(gateView, "active_command_id")
	if err != nil {
		return err
	}
	if gateStatus != "reserved" || activeCommandID != commandID {
		return fmt.Errorf("gate after failed attempt: status=%q active_command_id=%q", gateStatus, activeCommandID)
	}
	return nil
}
