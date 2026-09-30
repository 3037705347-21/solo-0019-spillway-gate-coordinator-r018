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

	decisionCopy := decision
	c.Interlock = &decisionCopy
	c.UpdatedAt = now.UTC()
	c.Version++

	if decision.IsClear() {
		token = strings.TrimSpace(token)
		if token == "" {
			return Invalid("missing_execution_token", "clear decisions require an execution token")
		}
		c.ExecutionToken = token
		c.Status = CommandApproved
		return nil
	}

	c.ExecutionToken = ""
	c.Status = CommandRejected
	return nil
}

func (c *GateCommand) BeginExecution(token, operator, requestKey string, now time.Time) error {
	if c.Status != CommandApproved {
		return Conflict("command_not_executable", "only approved commands can execute")
	}
	if strings.TrimSpace(token) == "" || token != c.ExecutionToken {
		return Conflict("invalid_execution_token", "execution token is invalid")
	}
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return Invalid("invalid_execution_request", "execution request identity is required")
	}

	c.ExecutedBy = strings.TrimSpace(operator)
	c.Status = CommandExecuting
	c.ExecutionToken = ""
	c.ExecutionReceipt = &ExecutionReceipt{
		Token:      token,
		RequestKey: requestKey,
		Status:     CommandExecuting,
		ExecutedBy: c.ExecutedBy,
	}
	c.UpdatedAt = now.UTC()
	c.Version++
	if c.ExecutedBy == "" {
		return Invalid("invalid_operator", "operator is required")
	}
	return nil
}

func (c *GateCommand) Complete(gate Gate, requestKey string, now time.Time) error {
	if c.Status != CommandExecuting {
		return Conflict("command_not_executing", "command is not executing")
	}

	c.ExecutionToken = ""
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
	if c.Status != CommandExecuting {
		return Conflict("command_not_executing", "command is not executing")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "gate_execution_failed"
	}
	c.ExecutionToken = ""
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

func (c GateCommand) ReplayExecution(requestKey string) (GateCommand, Gate, bool) {
	if c.ExecutionReceipt == nil || c.ExecutionReceipt.RequestKey != requestKey {
		return GateCommand{}, Gate{}, false
	}
	if !c.IsTerminal() {
		return GateCommand{}, Gate{}, false
	}

	command := c
	command.ExecutionToken = ""
	command.UpdatedAt = c.ExecutionReceipt.UpdatedAt
	command.Version = c.ExecutionReceipt.CommandVersion
	return command, c.ExecutionReceipt.Gate, true
}

func (c *GateCommand) RebuildExecutionRequest(token, requestKey string) bool {
	if c.ExecutionReceipt == nil || c.ExecutionReceipt.RequestKey != requestKey {
		return false
	}
	if !c.IsTerminal() {
		return false
	}

	c.Status = CommandApproved
	c.ExecutionToken = token
	c.ExecutionReceipt = nil
	return true
}
