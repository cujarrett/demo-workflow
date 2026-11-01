package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

// stepDuration is how long each step works. Long enough for a guest to press Unplug mid-step.
const stepDuration = 4 * time.Second

// Activities runs on a worker. Its methods are the Temporal activities.
type Activities struct {
	identity string
	temporal client.Client
	logger   *slog.Logger
	// exit stops the process. Tests replace it.
	exit func()
}

// PickWorker runs on the shared queue, so it lands on a live worker. That worker claims the order.
func (a *Activities) PickWorker(ctx context.Context) (Assignment, error) {
	return Assignment{Queue: workerQueue(a.identity), Identity: a.identity}, nil
}

// Step runs one order step and heartbeats while it works.
func (a *Activities) Step(ctx context.Context, in StepInput) error {
	if in.StartPlain {
		go a.runPlainOrder(in.OrderID)
	}

	if in.Step == "charge" {
		// The order ID is the idempotency key. A charge retried after a crash is refused.
		if _, err := a.charge(ctx, in.OrderID); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(stepDuration)
	for time.Now().Before(deadline) {
		activity.RecordHeartbeat(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil
}

// Unplug stops this worker's process mid-work, as a crash, deploy or lost server would.
// It returns first, so Temporal records that the worker accepted the task.
func (a *Activities) Unplug(ctx context.Context) error {
	a.logger.Info("unplugged, exiting")
	go func() {
		time.Sleep(200 * time.Millisecond)
		a.exit()
	}()
	return nil
}

// runPlainOrder is the same order as a plain loop, held only in this process's memory.
// When this process dies, so does the order. It reports each step so a guest can see where it stopped.
func (a *Activities) runPlainOrder(orderID string) {
	ctx := context.Background()
	report := func(step, status string) {
		u := PlainUpdate{Worker: a.identity, Step: step, Status: status}
		if err := a.temporal.SignalWorkflow(ctx, orderID, "", "plain", u); err != nil {
			a.logger.Warn("plain order report failed", "error", err)
		}
	}
	for _, step := range orderSteps {
		report(step, "running")
		if step == "charge" {
			if _, err := a.charge(ctx, "plain-"+orderID); err != nil {
				a.logger.Warn("plain order charge failed", "error", err)
			}
		}
		time.Sleep(stepDuration)
		report(step, "done")
	}
}

func (a *Activities) charge(ctx context.Context, key string) (Payment, error) {
	handle, err := a.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   paymentsID,
		UpdateName:   "charge",
		Args:         []any{key},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return Payment{}, err
	}
	var p Payment
	err = handle.Get(ctx, &p)
	return p, err
}

func exitProcess() { os.Exit(1) }

func activityName(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}
