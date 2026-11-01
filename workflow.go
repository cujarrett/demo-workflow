package main

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// sharedQueue: every worker polls it, so whichever is alive picks up new work.
// Each worker also polls its own queue, workerQueue(identity), so one order sticks
// to one worker until that worker dies. This is Temporal's worker-specific task
// queue pattern.
const sharedQueue = "orders"

// paymentsID is the one payments workflow per sandbox.
const paymentsID = "payments"

// maxOrders caps what guests can start. Each order makes two charges, one per copy.
const maxOrders = 200

var orderSteps = []string{"charge", "pack", "ship", "email"}

func workerQueue(identity string) string { return "worker-" + identity }

// Assignment is the worker an order runs on.
type Assignment struct {
	Queue    string
	Identity string
}

// StepState is one step of the workflow copy.
type StepState struct {
	Name   string `json:"name"`
	Status string `json:"status"` // waiting, running, interrupted, done
	Worker string `json:"worker"`
}

// PlainState mirrors the in-memory copy, so a guest can see where it stopped.
// The copy itself lives only in one worker's memory.
type PlainState struct {
	Worker    string    `json:"worker"`
	Steps     []string  `json:"steps"` // status per step in orderSteps
	UpdatedAt time.Time `json:"updatedAt"`
}

// OrderState is what the "state" query returns.
type OrderState struct {
	Steps    []StepState `json:"steps"`
	Handoffs int         `json:"handoffs"`
	Done     bool        `json:"done"`
	Plain    PlainState  `json:"plain"`
}

// PlainUpdate is a signal from the in-memory copy after each change.
type PlainUpdate struct {
	Worker string
	Step   string
	Status string
}

// StepInput tells a worker which step to run.
type StepInput struct {
	OrderID string
	Step    string
	// StartPlain starts the in-memory copy of the order on this worker, once per order.
	StartPlain bool
}

func newOrderState() OrderState {
	s := OrderState{}
	for _, name := range orderSteps {
		s.Steps = append(s.Steps, StepState{Name: name, Status: "waiting"})
		s.Plain.Steps = append(s.Plain.Steps, "waiting")
	}
	return s
}

func (p PlainState) done() bool { return p.Steps[len(p.Steps)-1] == "done" }

// OrderWorkflow runs the four steps on one worker.
// When that worker stops answering, it picks another worker and reruns the step.
func OrderWorkflow(ctx workflow.Context, orderID string) (OrderState, error) {
	state := newOrderState()
	if err := workflow.SetQueryHandler(ctx, "state", func() (OrderState, error) { return state, nil }); err != nil {
		return state, err
	}

	plainCh := workflow.GetSignalChannel(ctx, "plain")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var u PlainUpdate
			plainCh.Receive(ctx, &u)
			for i, name := range orderSteps {
				if name == u.Step {
					state.Plain.Steps[i] = u.Status
				}
			}
			state.Plain.Worker = u.Worker
			state.Plain.UpdatedAt = workflow.Now(ctx)
		}
	})

	pickCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           sharedQueue,
		StartToCloseTimeout: 10 * time.Second,
	})

	var worker Assignment
	plainStarted := false
	for i := 0; i < len(orderSteps); {
		if worker.Queue == "" {
			if err := workflow.ExecuteActivity(pickCtx, "PickWorker").Get(ctx, &worker); err != nil {
				return state, err
			}
		}

		state.Steps[i].Status = "running"
		state.Steps[i].Worker = worker.Identity

		stepCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue: worker.Queue,
			// A dead worker never picks up the step.
			ScheduleToStartTimeout: 5 * time.Second,
			StartToCloseTimeout:    30 * time.Second,
			// A worker that dies mid-step stops heartbeating. Two seconds keeps the handoff visible but quick.
			HeartbeatTimeout: 2 * time.Second,
			// The workflow moves the step to another worker itself, rather than retrying on a dead one.
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		input := StepInput{OrderID: orderID, Step: orderSteps[i], StartPlain: !plainStarted}
		plainStarted = true

		err := workflow.ExecuteActivity(stepCtx, "Step", input).Get(ctx, nil)
		if isWorkerLost(err) {
			state.Steps[i].Status = "interrupted"
			state.Handoffs++
			worker = Assignment{}
			continue
		}
		if err != nil {
			return state, err
		}
		state.Steps[i].Status = "done"
		i++
	}
	state.Done = true

	// The in-memory copy's last report can trail the workflow by a moment.
	// If its worker died, that report never comes, so the wait is capped.
	if _, err := workflow.AwaitWithTimeout(ctx, 2*stepDuration, state.Plain.done); err != nil {
		return state, err
	}
	return state, nil
}

// isWorkerLost reports a step that timed out because its worker stopped answering.
func isWorkerLost(err error) bool {
	var timeoutErr *temporal.TimeoutError
	if !errors.As(err, &timeoutErr) {
		return false
	}
	switch timeoutErr.TimeoutType() {
	case enumspb.TIMEOUT_TYPE_HEARTBEAT, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return true
	}
	return false
}

// Payment is one charge key as the payment provider sees it.
type Payment struct {
	Attempts int  `json:"attempts"`
	Charged  bool `json:"charged"`
}

// PaymentsWorkflow stands in for the payment provider. It charges each key once and counts every attempt.
// A workflow holds the ledger, so both replicas read the same one.
func PaymentsWorkflow(ctx workflow.Context) error {
	payments := map[string]*Payment{}

	if err := workflow.SetQueryHandler(ctx, "payment", func(key string) (Payment, error) {
		if p := payments[key]; p != nil {
			return *p, nil
		}
		return Payment{}, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, "count", func() (int, error) { return len(payments), nil }); err != nil {
		return err
	}
	err := workflow.SetUpdateHandlerWithOptions(ctx, "charge",
		func(ctx workflow.Context, key string) (Payment, error) {
			p := payments[key]
			if p == nil {
				p = &Payment{}
				payments[key] = p
			}
			p.Attempts++
			p.Charged = true
			return *p, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(key string) error {
			if _, known := payments[key]; !known && len(payments) >= 2*maxOrders {
				return fmt.Errorf("this sandbox has run %d orders", maxOrders)
			}
			return nil
		}},
	)
	if err != nil {
		return err
	}

	// Runs for the life of the sandbox. Sandbox cleanup deletes it with the namespace.
	return workflow.Await(ctx, func() bool { return false })
}

// UnplugWorkflow sends one Unplug task to a worker's own queue. That worker exits its process.
func UnplugWorkflow(ctx workflow.Context, queue string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              queue,
		ScheduleToStartTimeout: 5 * time.Second,
		StartToCloseTimeout:    5 * time.Second,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	// The worker exits right after it accepts the task, so an error here is expected.
	_ = workflow.ExecuteActivity(ctx, "Unplug").Get(ctx, nil)
	return nil
}
