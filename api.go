package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// plainStuckAfter: the in-memory copy reports every step. Silence past one step means its worker died.
const plainStuckAfter = stepDuration + 3*time.Second

var orderIDPattern = regexp.MustCompile(`^order-[0-9]+$`)

// api is the public API both replicas serve. Every answer comes from Temporal,
// so it does not matter which replica a request lands on.
type api struct {
	temporal client.Client
	logger   *slog.Logger
}

func (s *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("POST /orders", s.startOrder)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("POST /orders/{id}/unplug", s.unplug)
	return allowAnyOrigin(mux)
}

// allowAnyOrigin lets the Launchpad page call this API. It sends no cookies or tokens,
// and the POSTs carry no body, so browsers need no preflight.
func allowAnyOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func (s *api) startOrder(w http.ResponseWriter, r *http.Request) {
	var count int
	if resp, err := s.temporal.QueryWorkflow(r.Context(), paymentsID, "", "count"); err == nil {
		_ = resp.Get(&count)
	}
	if count >= 2*maxOrders {
		http.Error(w, "this sandbox has run enough orders", http.StatusTooManyRequests)
		return
	}

	id := fmt.Sprintf("order-%d", time.Now().UnixNano())
	opts := client.StartWorkflowOptions{ID: id, TaskQueue: sharedQueue, WorkflowExecutionTimeout: 10 * time.Minute}
	if _, err := s.temporal.ExecuteWorkflow(r.Context(), opts, OrderWorkflow, id); err != nil {
		s.logger.Error("starting order", "error", err)
		http.Error(w, "could not start the order", http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]string{"id": id})
}

// orderView is everything a page draws for one order.
// Workers are pod names. The page knows which pods are online from its own pod stream.
type orderView struct {
	Workflow OrderState `json:"workflow"`
	Plain    plainView  `json:"plain"`
	Payments struct {
		Attempts     int  `json:"attempts"`
		Charges      int  `json:"charges"`
		PlainCharged bool `json:"plainCharged"`
	} `json:"payments"`
	Events []eventRow `json:"events"`
}

type plainView struct {
	PlainState
	// Stuck: its worker died mid-order, and its progress died with the process.
	Stuck bool `json:"stuck"`
}

type eventRow struct {
	Time string `json:"time"`
	Text string `json:"text"`
}

func (s *api) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, ok := s.orderState(r.Context(), id)
	if !ok {
		http.Error(w, "unknown order", http.StatusNotFound)
		return
	}

	view := orderView{Workflow: state, Plain: plainView{PlainState: state.Plain}}
	started := !state.Plain.UpdatedAt.IsZero()
	view.Plain.Stuck = started && !state.Plain.done() && time.Since(state.Plain.UpdatedAt) > plainStuckAfter

	if p, err := s.payment(r.Context(), id); err == nil {
		view.Payments.Attempts = p.Attempts
		if p.Charged {
			view.Payments.Charges = 1
		}
	}
	if p, err := s.payment(r.Context(), "plain-"+id); err == nil {
		view.Payments.PlainCharged = p.Charged
	}

	events, err := s.events(r.Context(), id)
	if err != nil {
		s.logger.Warn("reading history", "error", err)
	}
	view.Events = events
	writeJSON(w, view)
}

// unplug stops the worker running the order's current step.
func (s *api) unplug(w http.ResponseWriter, r *http.Request) {
	state, ok := s.orderState(r.Context(), r.PathValue("id"))
	if !ok {
		http.Error(w, "unknown order", http.StatusNotFound)
		return
	}
	target := ""
	for _, step := range state.Steps {
		if step.Status == "running" {
			target = step.Worker
		}
	}
	if target == "" {
		http.Error(w, "no worker is busy right now", http.StatusConflict)
		return
	}

	opts := client.StartWorkflowOptions{ID: fmt.Sprintf("unplug-%d", time.Now().UnixNano()), TaskQueue: sharedQueue}
	if _, err := s.temporal.ExecuteWorkflow(r.Context(), opts, UnplugWorkflow, workerQueue(target)); err != nil {
		s.logger.Error("starting unplug", "error", err)
		http.Error(w, "could not unplug the worker", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *api) orderState(ctx context.Context, id string) (OrderState, bool) {
	var state OrderState
	if !orderIDPattern.MatchString(id) {
		return state, false
	}
	resp, err := s.temporal.QueryWorkflow(ctx, id, "", "state")
	if err == nil {
		err = resp.Get(&state)
	}
	return state, err == nil
}

func (s *api) payment(ctx context.Context, key string) (Payment, error) {
	var p Payment
	resp, err := s.temporal.QueryWorkflow(ctx, paymentsID, "", "payment", key)
	if err == nil {
		err = resp.Get(&p)
	}
	return p, err
}

// events turns the order's history into short lines for the page.
func (s *api) events(ctx context.Context, id string) ([]eventRow, error) {
	iter := s.temporal.GetWorkflowHistory(ctx, id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	steps := map[int64]string{} // scheduled event ID -> step name
	var rows []eventRow
	add := func(e *historypb.HistoryEvent, text string) {
		rows = append(rows, eventRow{Time: e.GetEventTime().AsTime().Format(time.RFC3339Nano), Text: text})
	}
	for iter.HasNext() {
		e, err := iter.Next()
		if err != nil {
			return rows, err
		}
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			add(e, "Order started")
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attrs := e.GetActivityTaskScheduledEventAttributes()
			if attrs.GetActivityType().GetName() != "Step" {
				continue
			}
			var in StepInput
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &in); err == nil {
				steps[e.GetEventId()] = in.Step
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attrs := e.GetActivityTaskStartedEventAttributes()
			if step, ok := steps[attrs.GetScheduledEventId()]; ok {
				add(e, fmt.Sprintf("%s started %s", attrs.GetIdentity(), step))
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if step, ok := steps[e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()]; ok {
				add(e, step+" done")
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			if step, ok := steps[e.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()]; ok {
				add(e, "The worker on "+step+" stopped answering. Moving it to another worker")
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			add(e, "Order complete")
		}
	}
	return rows, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("writing response", "error", err)
	}
}
