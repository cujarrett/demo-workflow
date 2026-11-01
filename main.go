// demo-workflow is the Launchpad workflow demo: an order that finishes even when a worker dies.
//
// Two replicas run this one image. Each is a worker and each serves the public API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	port := envOr("PORT", "8080")
	// The pod name. It survives a container restart, so a worker keeps its own queue after an unplug.
	identity := envOr("HOSTNAME", "local")

	tc, err := dial(ctx, logger, client.Options{
		HostPort:        envOr("TEMPORAL_ADDRESS", "localhost:7233"),
		Namespace:       envOr("TEMPORAL_NAMESPACE", "default"),
		Identity:        identity,
		HeadersProvider: newTokenHeaders(os.Getenv("TEMPORAL_TOKEN_SCOPE")),
		Logger:          logger,
	})
	if err != nil {
		return err
	}
	defer tc.Close()

	// Both replicas try. The second finds it running and keeps it.
	_, err = tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       paymentsID,
		TaskQueue:                sharedQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, PaymentsWorkflow)
	if err != nil {
		return err
	}

	acts := &Activities{identity: identity, temporal: tc, logger: logger, exit: exitProcess}

	shared := worker.New(tc, sharedQueue, worker.Options{
		// A worker caches the workflows it ran. When it dies, Temporal waits this long before
		// giving the next workflow task to another worker. The 5s default makes the handoff drag.
		StickyScheduleToStartTimeout: time.Second,
	})
	shared.RegisterWorkflow(OrderWorkflow)
	shared.RegisterWorkflow(PaymentsWorkflow)
	shared.RegisterWorkflow(UnplugWorkflow)
	shared.RegisterActivityWithOptions(acts.PickWorker, activityName("PickWorker"))

	own := worker.New(tc, workerQueue(identity), worker.Options{})
	own.RegisterActivityWithOptions(acts.Step, activityName("Step"))
	own.RegisterActivityWithOptions(acts.Unplug, activityName("Unplug"))

	if err := shared.Start(); err != nil {
		return err
	}
	defer shared.Stop()
	if err := own.Start(); err != nil {
		return err
	}
	defer own.Stop()

	logger.Info("demo-workflow started", "worker", identity, "port", port)
	return serve(ctx, port, (&api{temporal: tc, logger: logger}).routes())
}

// dial retries until Temporal accepts this worker.
// In a new sandbox the token file and its Entra app take up to a minute to exist.
// Exiting instead would pile up restarts, and slow every restart after an unplug.
func dial(ctx context.Context, logger *slog.Logger, opts client.Options) (client.Client, error) {
	for {
		tc, err := client.Dial(opts)
		if err == nil {
			return tc, nil
		}
		logger.Warn("cannot reach Temporal yet, retrying", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func serve(ctx context.Context, port string, h http.Handler) error {
	srv := &http.Server{Addr: ":" + port, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
