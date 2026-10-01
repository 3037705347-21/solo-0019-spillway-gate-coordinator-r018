package coordination

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"example.com/spillway-gate-coordinator/internal/contracts"
	"example.com/spillway-gate-coordinator/internal/domain"
)

func (s *Service) RequestCommand(
	ctx context.Context,
	request contracts.RequestCommandRequest,
) (domain.GateCommand, error) {
	gate, err := s.repository.Gate(ctx, request.GateID)
	if err != nil {
		return domain.GateCommand{}, err
	}

	commandID, err := s.newID("cmd")
	if err != nil {
		return domain.GateCommand{}, domain.Internal("command_id_generation_failed", "command id could not be generated")
	}

	command, err := domain.NewGateCommand(
		commandID,
		gate.ID,
		request.TargetAperture,
		gate.Aperture,
		gate.MaxAperture,
		request.Reason,
		request.RequestedBy,
		s.now(),
	)
	if err != nil {
		return domain.GateCommand{}, err
	}
	if err := s.repository.CreateCommand(ctx, command); err != nil {
		return domain.GateCommand{}, err
	}
	return command, nil
}

func (s *Service) Command(ctx context.Context, id string) (domain.GateCommand, error) {
	return s.repository.Command(ctx, id)
}

func (s *Service) ReviewInterlock(
	ctx context.Context,
	commandID string,
	request contracts.ReviewInterlockRequest,
) (domain.GateCommand, domain.Gate, error) {
	decision, err := domain.NewInterlockDecision(
		request.Verdict,
		request.Verifier,
		request.Note,
		s.now(),
	)
	if err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	token := ""
	if decision.IsClear() {
		token, err = s.newToken()
		if err != nil {
			return domain.GateCommand{}, domain.Gate{}, domain.Internal(
				"execution_token_generation_failed",
				"execution token could not be generated",
			)
		}
	}

	return s.repository.ReviewCommand(ctx, commandID, decision, token, s.now())
}

func (s *Service) ExecuteCommand(
	ctx context.Context,
	commandID string,
	request contracts.ExecuteCommandRequest,
) (domain.GateCommand, domain.Gate, bool, error) {
	requestKey, err := executionRequestKey(commandID, request.RequestID)
	if err != nil {
		return domain.GateCommand{}, domain.Gate{}, false, err
	}
	command, gate, replayed, err := s.repository.ExecuteCommand(
		ctx,
		commandID,
		strings.TrimSpace(request.Token),
		request.Operator,
		request.SimulateFault,
		requestKey,
		s.now(),
	)
	return command, gate, replayed, err
}

// executionRequestKey identifies one operator action (an execution request and
// its retries after a lost response). It is deliberately independent of the
// execution token: the token authorizes a single boundary crossing, while the
// request identity confirms whether a later call is the same action.
func executionRequestKey(commandID, requestID string) (string, error) {
	commandID = strings.TrimSpace(commandID)
	requestID = strings.TrimSpace(requestID)
	if commandID == "" {
		return "", domain.Invalid("invalid_command_reference", "command id is required")
	}
	if requestID == "" {
		return "", domain.Invalid("invalid_execution_request", "request_id is required for execution and its retries")
	}

	digest := sha256.Sum256([]byte(commandID + "\x00" + requestID))
	return base64.RawURLEncoding.EncodeToString(digest[:18]), nil
}
