package main

import (
	"fmt"
	"net/http"
	"sync"
)

func executePayload(token, operator, requestID string, simulateFault bool) map[string]any {
	return map[string]any{
		"operator":       operator,
		"token":          token,
		"request_id":     requestID,
		"simulate_fault": simulateFault,
	}
}

func runExecuteGateCommand(client *apiClient) error {
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

	const requestID = "execution-request-0001"

	// A request that never took effect must not consume the token: the operator
	// field is rejected, then the identical request identity and token succeed.
	invalidOperator, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "   ", requestID, false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(invalidOperator, http.StatusBadRequest, "reject execution without operator"); err != nil {
		return err
	}

	executed, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", requestID, false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(executed, http.StatusOK, "execute approved command"); err != nil {
		return err
	}
	if replayed, _ := executed.Body["idempotent_replay"].(bool); replayed {
		return fmt.Errorf("first execution must not be flagged as an idempotent replay")
	}
	executedCommand, err := objectField(executed.Body, "command")
	if err != nil {
		return err
	}
	completedStatus, err := stringField(executedCommand, "status")
	if err != nil {
		return err
	}
	if completedStatus != "completed" {
		return fmt.Errorf("execution status: expected completed, got %q", completedStatus)
	}

	executedGate, err := objectField(executed.Body, "gate")
	if err != nil {
		return err
	}
	gateStatus, err := stringField(executedGate, "status")
	if err != nil {
		return err
	}
	aperture, err := floatField(executedGate, "aperture")
	if err != nil {
		return err
	}
	if gateStatus != "idle" || aperture != 2.0 {
		return fmt.Errorf("settled gate mismatch: status=%q aperture=%v", gateStatus, aperture)
	}

	originalCommandVersion, ok := executedCommand["version"].(float64)
	if !ok {
		return fmt.Errorf("first execution response is missing numeric command version")
	}
	originalGateVersion, ok := executedGate["version"].(float64)
	if !ok {
		return fmt.Errorf("first execution response is missing numeric gate version")
	}

	// A retry of the same request after a lost response confirms the original
	// terminal state; it must not start a second movement.
	retry, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", requestID, false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(retry, http.StatusOK, "retry confirms the original terminal state"); err != nil {
		return err
	}
	if replayed, _ := retry.Body["idempotent_replay"].(bool); !replayed {
		return fmt.Errorf("retry of the same request must be flagged as an idempotent replay")
	}
	retryCommand, err := objectField(retry.Body, "command")
	if err != nil {
		return err
	}
	if status, _ := stringField(retryCommand, "status"); status != "completed" {
		return fmt.Errorf("retry status: expected completed, got %q", status)
	}
	if version, _ := retryCommand["version"].(float64); version != originalCommandVersion {
		return fmt.Errorf("retry must not advance the command version: original %v, retry %v", originalCommandVersion, version)
	}
	retryGate, err := objectField(retry.Body, "gate")
	if err != nil {
		return err
	}
	if retryAperture, _ := floatField(retryGate, "aperture"); retryAperture != aperture {
		return fmt.Errorf("retry gate snapshot diverged from original outcome: %v", retryAperture)
	}
	if retryVersion, _ := retryGate["version"].(float64); retryVersion != originalGateVersion {
		return fmt.Errorf("retry must not advance the gate version: original %v, retry %v", originalGateVersion, retryVersion)
	}

	// Reusing the spent token under a new request identity tries to start a new
	// action and must be refused: finished commands cannot be re-executed.
	reused, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", "execution-request-0002", false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(reused, http.StatusConflict, "reject spent execution token as a new action"); err != nil {
		return err
	}
	if code, _ := stringField(reused.Body, "code"); code != "command_already_completed" {
		return fmt.Errorf("spent token reuse: expected command_already_completed, got %q", code)
	}

	// A retry cannot be claimed by a forged request identity or a wrong token.
	forged, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", "forged-request", false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(forged, http.StatusConflict, "reject forged request identity"); err != nil {
		return err
	}
	wrongToken, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload("not-the-issued-token", "operator-chen", requestID, false),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(wrongToken, http.StatusConflict, "reject retry with a mismatched token"); err != nil {
		return err
	}

	// Retries of a faulted execution confirm the aborted terminal state, with
	// the gate latched faulted and the final aperture retained.
	if err := verifyFaultedExecutionReplay(client); err != nil {
		return err
	}

	// Two near-simultaneous retries of one approved command: exactly one
	// crosses the execution boundary; the other confirms the stored result.
	if err := verifyConcurrentRetries(client); err != nil {
		return err
	}

	fmt.Println("execute-gate-command: single-use token, idempotent confirmation and concurrency verified")
	return nil
}

func verifyFaultedExecutionReplay(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-fault", "故障演练闸", 4, 1.0); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-fault", 3.0, "故障演练", "dispatcher-zhao")
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

	faulted, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", "fault-request-1", true),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(faulted, http.StatusOK, "execute with simulated fault"); err != nil {
		return err
	}
	faultCommand, err := objectField(faulted.Body, "command")
	if err != nil {
		return err
	}
	if status, _ := stringField(faultCommand, "status"); status != "aborted" {
		return fmt.Errorf("fault execution: expected aborted, got %q", status)
	}
	faultGate, err := objectField(faulted.Body, "gate")
	if err != nil {
		return err
	}
	if status, _ := stringField(faultGate, "status"); status != "faulted" {
		return fmt.Errorf("fault execution: expected faulted gate, got %q", status)
	}
	if finalAperture, _ := floatField(faultGate, "aperture"); finalAperture != 3.0 {
		return fmt.Errorf("fault execution: expected retained aperture 3.0, got %v", finalAperture)
	}

	retry, err := client.post(
		"/commands/"+commandID+"/execute",
		executePayload(token, "operator-chen", "fault-request-1", true),
	)
	if err != nil {
		return err
	}
	if err := requireStatus(retry, http.StatusOK, "fault retry confirms original aborted state"); err != nil {
		return err
	}
	if replayed, _ := retry.Body["idempotent_replay"].(bool); !replayed {
		return fmt.Errorf("fault retry must be flagged as an idempotent replay")
	}
	retryGate, err := objectField(retry.Body, "gate")
	if err != nil {
		return err
	}
	if status, _ := stringField(retryGate, "status"); status != "faulted" {
		return fmt.Errorf("fault retry gate: expected faulted, got %q", status)
	}
	if finalAperture, _ := floatField(retryGate, "aperture"); finalAperture != 3.0 {
		return fmt.Errorf("fault retry aperture: expected 3.0, got %v", finalAperture)
	}
	return nil
}

func verifyConcurrentRetries(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-race", "并发演练闸", 4, 0.5); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-race", 1.5, "并发演练", "dispatcher-zhao")
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

	const parallel = 16
	results := make([]apiResult, parallel)
	var wg sync.WaitGroup
	wg.Add(parallel)
	start := make(chan struct{})
	for i := 0; i < parallel; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			result, callErr := client.post(
				"/commands/"+commandID+"/execute",
				executePayload(token, "operator-chen", "race-request-1", false),
			)
			if callErr != nil {
				results[index] = apiResult{Status: 0, Body: map[string]any{"error": callErr.Error()}}
				return
			}
			results[index] = result
		}(i)
	}
	close(start)
	wg.Wait()

	executions := 0
	replays := 0
	for _, result := range results {
		if result.Status != http.StatusOK {
			return fmt.Errorf("concurrent retries: expected every call to return 200, got %d body %s", result.Status, prettyBody(result.Body))
		}
		if replayed, _ := result.Body["idempotent_replay"].(bool); replayed {
			replays++
		} else {
			executions++
		}
	}
	if executions != 1 {
		return fmt.Errorf("expected exactly one boundary crossing, got %d", executions)
	}
	if replays != parallel-1 {
		return fmt.Errorf("expected %d idempotent confirmations, got %d", parallel-1, replays)
	}
	return nil
}
