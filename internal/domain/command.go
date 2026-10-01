package domain

import (
	"strings"
	"time"
)

type CommandStatus string

const (
	CommandPending   CommandStatus = "pending"
	CommandApproved  CommandStatus = "approved"
	CommandRejected  CommandStatus = "rejected"
	CommandExecuting CommandStatus = "executing"
	CommandCompleted CommandStatus = "completed"
	CommandAborted   CommandStatus = "aborted"
)

type ExecutionReceipt struct {
	Token          string
	RequestKey     string
	Status         CommandStatus
	ExecutedBy     string
	TerminalReason string
	CommandVersion int64
	UpdatedAt      time.Time
	Gate           Gate
}

type GateCommand struct {
	ID               string
	GateID           string
	TargetAperture   float64
	StartingAperture float64
	Reason           string
	RequestedBy      string
	Status           CommandStatus
	Interlock        *InterlockDecision
	ExecutionToken   string
	ExecutionReceipt *ExecutionReceipt
	ExecutedBy       string
	TerminalReason   string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Version          int64
}

func NewGateCommand(
	commandID string,
	gateID string,
	targetAperture float64,
	startingAperture float64,
	maxAperture float64,
	reason string,
	requestedBy string,
	now time.Time,
) (GateCommand, error) {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return GateCommand{}, Invalid("invalid_command_id", "command id is required")
	}

	gateID = strings.TrimSpace(gateID)
	if gateID == "" {
		return GateCommand{}, Invalid("invalid_gate_reference", "gate id is required")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		return GateCommand{}, Invalid("invalid_command_reason", "reason is required")
	}

	requestedBy = strings.TrimSpace(requestedBy)
	if requestedBy == "" {
		return GateCommand{}, Invalid("invalid_requested_by", "requested_by is required")
	}

	if startingAperture < 0 || startingAperture > maxAperture {
		return GateCommand{}, Invalid("invalid_starting_aperture", "starting aperture is outside the gate limit")
	}
	if targetAperture < 0 || targetAperture > maxAperture {
		return GateCommand{}, Invalid("invalid_target_aperture", "target aperture is outside the gate limit")
	}
	if targetAperture == startingAperture {
		return GateCommand{}, Invalid("no_aperture_change", "target aperture must differ from the current aperture")
	}

	return GateCommand{
		ID:               commandID,
		GateID:           gateID,
		TargetAperture:   targetAperture,
		StartingAperture: startingAperture,
		Reason:           reason,
		RequestedBy:      requestedBy,
		Status:           CommandPending,
		CreatedAt:        now.UTC(),
		UpdatedAt:        now.UTC(),
		Version:          1,
	}, nil
}

func (c GateCommand) IsTerminal() bool {
	return c.Status == CommandRejected ||
		c.Status == CommandCompleted ||
		c.Status == CommandAborted
}

func (c *GateCommand) Review(decision InterlockDecision, token string, now time.Time) error {
	if c.Status != CommandPending {
		return Conflict("command_not_pending", "only pending commands can be reviewed")
	}
	if decision.IsClear() && strings.TrimSpace(token) == "" {
		return Invalid("missing_execution_token", "clear decisions require an execution token")
	}

	decisionCopy := decision
	c.Interlock = &decisionCopy
	c.UpdatedAt = now.UTC()
	c.Version++

	if decision.IsClear() {
		c.ExecutionToken = strings.TrimSpace(token)
		c.Status = CommandApproved
		return nil
	}

	c.ExecutionToken = ""
	c.Status = CommandRejected
	return nil
}

// ExecuteDenial explains why a command in its current state cannot start an
// execution. It never changes state.
func (c GateCommand) ExecuteDenial() error {
	switch c.Status {
	case CommandCompleted:
		return Conflict("command_already_completed", "command already completed; its token cannot start another execution")
	case CommandAborted:
		return Conflict("command_already_aborted", "command already aborted; its token cannot start another execution")
	case CommandRejected:
		return Conflict("command_rejected", "rejected commands cannot execute")
	case CommandExecuting:
		return Conflict("execution_in_progress", "execution is already in progress")
	default:
		return Conflict("command_not_executable", "only approved commands can execute")
	}
}

// BeginExecution crosses the single execution boundary approved->executing.
// All preconditions are validated against the current state before anything is
// mutated, so a rejected attempt leaves the command and its token untouched:
// a request that never took effect must not consume the token.
func (c *GateCommand) BeginExecution(token, operator, requestKey string, now time.Time) error {
	token = strings.TrimSpace(token)
	operator = strings.TrimSpace(operator)
	requestKey = strings.TrimSpace(requestKey)

	if c.Status != CommandApproved {
		return c.ExecuteDenial()
	}
	if token == "" || token != c.ExecutionToken {
		return Conflict("invalid_execution_token", "execution token is invalid")
	}
	if operator == "" {
		return Invalid("invalid_operator", "operator is required")
	}
	if requestKey == "" {
		return Invalid("invalid_execution_request", "execution request identity is required")
	}

	c.ExecutedBy = operator
	c.Status = CommandExecuting
	// The live token authorizes exactly one boundary crossing; erase it the
	// moment the boundary is crossed so it can never start another movement.
	originalToken := c.ExecutionToken
	c.ExecutionToken = ""
	c.ExecutionReceipt = &ExecutionReceipt{
		Token:      originalToken,
		RequestKey: requestKey,
		Status:     CommandExecuting,
		ExecutedBy: operator,
	}
	c.UpdatedAt = now.UTC()
	c.Version++
	return nil
}

func (c *GateCommand) requireExecuting(requestKey string) error {
	if c.Status != CommandExecuting {
		return Conflict("command_not_executing", "command is not executing")
	}
	if c.ExecutionReceipt == nil || c.ExecutionReceipt.RequestKey != strings.TrimSpace(requestKey) {
		return Conflict("execution_request_mismatch", "execution request does not match the running command")
	}
	return nil
}

func (c *GateCommand) Complete(gate Gate, requestKey string, now time.Time) error {
	if err := c.requireExecuting(requestKey); err != nil {
		return err
	}

	c.Status = CommandCompleted
	c.UpdatedAt = now.UTC()
	c.Version++
	c.ExecutionReceipt = &ExecutionReceipt{
		Token:          c.ExecutionReceipt.Token,
		RequestKey:     requestKey,
		Status:         CommandCompleted,
		ExecutedBy:     c.ExecutedBy,
		CommandVersion: c.Version,
		UpdatedAt:      c.UpdatedAt,
		Gate:           gate,
	}
	return nil
}

func (c *GateCommand) Abort(gate Gate, requestKey, reason string, now time.Time) error {
	if err := c.requireExecuting(requestKey); err != nil {
		return err
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "gate_execution_failed"
	}
	c.TerminalReason = reason
	c.Status = CommandAborted
	c.UpdatedAt = now.UTC()
	c.Version++
	c.ExecutionReceipt = &ExecutionReceipt{
		Token:          c.ExecutionReceipt.Token,
		RequestKey:     requestKey,
		Status:         CommandAborted,
		ExecutedBy:     c.ExecutedBy,
		TerminalReason: c.TerminalReason,
		CommandVersion: c.Version,
		UpdatedAt:      c.UpdatedAt,
		Gate:           gate,
	}
	return nil
}

// TerminalReceipt confirms the original outcome of a finished execution. It is
// strictly read-only: the state machine is not rolled back and no movement is
// started. Confirming a result is a separate concern from starting one.
//
// The presented token must be the token that originally crossed the execution
// boundary, and the request key must identify the same execution request.
func (c GateCommand) TerminalReceipt(token, requestKey string) (ExecutionReceipt, bool) {
	token = strings.TrimSpace(token)
	requestKey = strings.TrimSpace(requestKey)
	if c.ExecutionReceipt == nil {
		return ExecutionReceipt{}, false
	}
	if !c.IsTerminal() {
		return ExecutionReceipt{}, false
	}
	if token == "" || token != c.ExecutionReceipt.Token {
		return ExecutionReceipt{}, false
	}
	if requestKey == "" || requestKey != c.ExecutionReceipt.RequestKey {
		return ExecutionReceipt{}, false
	}
	return *c.ExecutionReceipt, true
}
