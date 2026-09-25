package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

type SagaTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *SagaTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *SagaTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *SagaTestSuite) TestSagaSuccessWritesAuditTrail() {
	var infra *workflows.InfrastructureActivities
	var audit *workflows.AuditActivities

	s.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(audit.RecordAuditTrail, mock.Anything, mock.Anything).Return(nil).Times(3)

	prop := contracts.AgentProposal{
		ID:        "wf-ok-1",
		Type:      contracts.CmdModifyRouting,
		TargetID:  "edge-a",
		RiskScore: 0.2,
	}
	s.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, prop)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *SagaTestSuite) TestSagaFailureRunsCompensation() {
	var infra *workflows.InfrastructureActivities
	var audit *workflows.AuditActivities

	s.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).
		Return(contracts.NonRetryableInfraError)
	s.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(audit.RecordAuditTrail, mock.Anything, mock.Anything).Return(nil)

	prop := contracts.AgentProposal{
		ID:        "wf-fail-1",
		Type:      contracts.CmdUpdateCert,
		TargetID:  contracts.FailingNodeID,
		RiskScore: 0.2,
	}
	s.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, prop)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func TestSagaTestSuite(t *testing.T) {
	suite.Run(t, new(SagaTestSuite))
}
