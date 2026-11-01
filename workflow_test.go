package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	acts := &Activities{}
	env.RegisterActivityWithOptions(acts.PickWorker, activityName("PickWorker"))
	env.RegisterActivityWithOptions(acts.Step, activityName("Step"))
	return env
}

func TestOrderCompletesOnOneWorker(t *testing.T) {
	env := newEnv(t)
	env.OnActivity("PickWorker", mock.Anything).Return(Assignment{Queue: "worker-a", Identity: "a"}, nil).Once()
	env.OnActivity("Step", mock.Anything, mock.Anything).Return(nil).Times(4)

	env.ExecuteWorkflow(OrderWorkflow, "order-1")

	var state OrderState
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatal(err)
	}
	if !state.Done || state.Handoffs != 0 {
		t.Fatalf("got done=%v handoffs=%d, want done with no handoffs", state.Done, state.Handoffs)
	}
	for _, s := range state.Steps {
		if s.Worker != "a" || s.Status != "done" {
			t.Errorf("step %s: worker %q status %q", s.Name, s.Worker, s.Status)
		}
	}
}

func TestOrderMovesToAnotherWorkerWhenOneDies(t *testing.T) {
	env := newEnv(t)
	env.OnActivity("PickWorker", mock.Anything).Return(Assignment{Queue: "worker-a", Identity: "a"}, nil).Once()
	env.OnActivity("PickWorker", mock.Anything).Return(Assignment{Queue: "worker-b", Identity: "b"}, nil).Once()

	charges := 0
	env.OnActivity("Step", mock.Anything, mock.Anything).Return(func(_ context.Context, in StepInput) error {
		if in.Step == "charge" {
			charges++
			if charges == 1 {
				if !in.StartPlain {
					t.Error("the first step should start the in-memory order")
				}
				return temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil)
			}
			if in.StartPlain {
				t.Error("the retried step must not start a second in-memory order")
			}
		}
		return nil
	})

	env.ExecuteWorkflow(OrderWorkflow, "order-1")

	var state OrderState
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatal(err)
	}
	if state.Handoffs != 1 || charges != 2 {
		t.Fatalf("got handoffs=%d charges=%d, want 1 and 2", state.Handoffs, charges)
	}
	for _, s := range state.Steps {
		if s.Worker != "b" || s.Status != "done" {
			t.Errorf("step %s: worker %q status %q, want b done", s.Name, s.Worker, s.Status)
		}
	}
}

func TestOrderRecordsThePlainCopy(t *testing.T) {
	env := newEnv(t)
	env.OnActivity("PickWorker", mock.Anything).Return(Assignment{Queue: "worker-a", Identity: "a"}, nil)
	env.OnActivity("Step", mock.Anything, mock.Anything).Return(nil)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("plain", PlainUpdate{Worker: "a", Step: "charge", Status: "done"})
	}, time.Second)

	env.ExecuteWorkflow(OrderWorkflow, "order-1")

	var state OrderState
	if err := env.GetWorkflowResult(&state); err != nil {
		t.Fatal(err)
	}
	if state.Plain.Worker != "a" || state.Plain.Steps[0] != "done" || state.Plain.Steps[1] != "waiting" {
		t.Fatalf("got plain %+v, want charge done on a and pack waiting", state.Plain)
	}
}

func TestPaymentsChargesEachKeyOnce(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	var results []Payment
	charge := func(updateID, key string) {
		env.UpdateWorkflow("charge", updateID, &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("charge %s rejected: %v", key, err) },
			OnAccept: func() {},
			OnComplete: func(v any, err error) {
				if err != nil {
					t.Errorf("charge %s: %v", key, err)
					return
				}
				results = append(results, v.(Payment))
			},
		}, key)
	}
	env.RegisterDelayedCallback(func() { charge("first", "order-1") }, time.Second)
	env.RegisterDelayedCallback(func() { charge("retry", "order-1") }, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow("payment", "order-1")
		if err != nil {
			t.Fatal(err)
		}
		var p Payment
		if err := v.Get(&p); err != nil {
			t.Fatal(err)
		}
		if p.Attempts != 2 || !p.Charged {
			t.Errorf("got %+v, want 2 attempts and charged", p)
		}
		env.CancelWorkflow()
	}, 3*time.Second)

	env.ExecuteWorkflow(PaymentsWorkflow)

	if len(results) != 2 {
		t.Fatalf("got %d charge results, want 2", len(results))
	}
}
