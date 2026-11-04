# demo-workflow

The sample workflow app that [Launchpad](https://launchpad.mattjarrett.dev) puts in a guest sandbox. It shows an order that finishes even when the worker running it dies.

Two replicas run this one image. Each is a worker, and each serves the public API.

## What it shows

- **Handoff:** each order sticks to one worker. When that worker dies mid-step, the step moves to the other worker.
- **Charged once:** the charge step reruns on the new worker. The payments workflow refuses the repeat by order ID, so the order shows 2 attempts and 1 charge.
- **Without a workflow:** the same order also runs as a plain loop in the first worker's memory. When that worker dies, the loop stops where it was and never finishes.

## API

Every answer comes from the workflows, so either replica can serve any request.

| Route | Does |
|---|---|
| `POST /orders` | Starts an order and returns its ID |
| `GET /orders/{id}` | Steps, payments, the plain copy and the event history |
| `POST /orders/{id}/unplug` | Stops the worker running the current step |
| `GET /healthz`, `GET /readyz` | Readiness probes. Both answer once the worker has connected |

Workers are named by pod name.

## Environment

| Variable | Value |
|---|---|
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TOKEN_SCOPE` | Set by the platform from `workflowSpaceRef` |
| `PORT` | Default `8080` |

## Run it locally

Start a dev server, then two workers in two more terminals.

```bash
temporal server start-dev
HOSTNAME=pod-a PORT=8081 just run
HOSTNAME=pod-b PORT=8082 just run
```

Start an order with `curl -X POST localhost:8081/orders`. Unplug stops a worker's process. Start it again by hand.
