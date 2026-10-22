# OpenAPI connectors (proof of concept)

This repo turns an OpenAPI spec into a Nexus service with no hand-written HTTP code. Each
operation in the spec becomes a typed Nexus operation backed by one standalone activity. The
activity sends its request through the [HTTP connector](https://github.com/Evanthx/nexus-connector-http) used as a
library, which provides retries, rate limiting and secret redaction. nexgen generates typed
callers for the service in Go, Java, Python and TypeScript.

It is a proof of concept: it reads only what its two example specs need (see
[Limits](#limits)). It shows three ways to expose an API's calls over Nexus:

1. [**One operation per call**](#1-one-operation-per-call), generated from the spec. Each Nexus
   operation is one standalone activity making one HTTP request.
2. [**Several calls in one activity**](#2-several-calls-in-one-activity-start-then-poll): an
   operation that starts a job and polls for its result in a loop, within one activity attempt.
3. [**Several calls in a workflow**](#3-several-calls-in-a-workflow): an operation backed by a
   workflow that runs the generated activities, for calls that can't be repeated as one unit.

## How it works

```
interzoid.json ─┐
connector.yaml ─┴─► openapi2nexus ─┬─► weather.nexusrpc.yaml ─► nexgen ─► weatherapi/ (Go types; callers in any language)
                                   └─► operations.gen.go (the operation table and one activity per operation)
```

- **`connector.yaml`** holds what the spec can't say:
  - the Nexus service name;
  - which query parameter is the API key, left out of every input and filled in from the
    worker's environment;
  - operation renames;
  - any operations written by hand, with their types.
- **`cmd/openapi2nexus`** is the converter. It turns each spec operation into a Nexus operation
  whose input holds the path, query and header parameters plus `body`, and whose output is the
  2xx response's schema. It converts OpenAPI 3.0 schemas to the JSON Schema 2020-12 that nexgen
  reads, for example writing `nullable` as a `oneOf` with `null`.
- **`openapiconnector`** is the runtime:
  - `Call` turns a typed input into the HTTP connector's `SendRequestInput`, calls `SendRequest`,
    and decodes the body into the typed output.
  - `ActivityOperation` is the Nexus operation that starts the activity. It first rejects a
    request that can't be made, or that outlasts the caller's timeout.

The deployment decides each API's base URL, API key variable and timeout in
`openapiconnector.Config`. Callers can't change them.

## The examples

Two APIs show the three ways to expose calls.

### 1. One operation per call

`WeatherService.getWeatherByZip` comes from Interzoid's weather API (`apis/weather/interzoid.json`,
the real spec). `ForecastReportService.createReport` and `getReport` come from a made-up reports
API (`apis/forecastreports/forecastreports.yaml`).

Each is generated, and each is one Nexus operation backed by one standalone activity making one
HTTP request.

### 2. Several calls in one activity: start, then poll

`ForecastReportService.runReport` (`apis/forecastreports/reports.go`) starts a report and returns
it once it is done, in **one standalone activity** that polls in a loop within one attempt. It
calls the generated `CreateReport` and `GetReport` as plain functions, not as activities.

**Timeouts.** A plain activity's attempt is one HTTP request, limited to 60 seconds; the whole
operation, every attempt and wait, is limited by `ScheduleToCloseSeconds` (25 hours by default).
A polling attempt can last the whole operation instead. `PollingOperation` starts it with
`PollingActivityOptions`: its `StartToCloseTimeout` is the operation's `ScheduleToCloseTimeout`,
and a `HeartbeatTimeout` notices a worker that died. `WaitToPoll` heartbeats before each wait, so
the longest gap between heartbeats is one poll interval plus one request, which is what the
`HeartbeatTimeout` allows.

**Errors.** The loop returns its calls' errors unchanged, which ends the attempt. The HTTP
connector's error types and retry delays then apply as for any of its calls: a Transient error
waits its backoff, and the next attempt starts again from the top. Starting again is safe only
because the start is idempotent: every attempt sends the activity ID as the `Idempotency-Key`,
and the API returns the report that key first started.

**Heartbeat details.** The HTTP connector keeps its count of Transient errors in a row in the
heartbeat details: each Transient error reads the count the attempt started with and records it
plus one; a RateLimited error records 0; a success records nothing. The loop doesn't interfere, as
long as it:

- heartbeats with no details, as `WaitToPoll` does. A failed attempt's last heartbeat is the
  count its error recorded, which the SDK sends even when heartbeats are being throttled.
- returns errors instead of retrying them in the loop. The count only advances between attempts.
- doesn't keep progress of its own in the heartbeat details. That is why the start must be
  idempotent instead of the report ID being saved there.

### 3. Several calls in a workflow

`WeatherService.weatherForZips` (`apis/weather/weather.go`) is a Nexus operation backed by a
**workflow**. The workflow runs the generated `GetWeatherByZip` activity for each zip code at
once, and returns all the results or the first failure.

Use a workflow when the calls aren't one idempotent unit that can be repeated from the top. For
example: steps that depend on earlier results, a wait longer than a retry should cover, or a
compensation step. An activity can't durably run other activities and wait for them; a workflow
can.

## Commands

This repo builds against a local copy of the HTTP connector (a `replace` in `go.mod`), so clone
both repos side by side:

```bash
git clone https://github.com/Evanthx/nexus-connector-http
git clone https://github.com/Evanthx/nexus-connector-openapi
cd nexus-connector-openapi
```

```bash
make generate   # run openapi2nexus and nexgen (Go) for both APIs
make langs      # also generate Java, Python and TypeScript callers into .build/langs
make test       # unit tests, plus an end-to-end test that needs the temporal CLI
```

The end-to-end test (`e2e/e2e_test.go`) works like this:
- It starts a dev server with `activity.enableCallbacks=true`.
- It runs both connectors in one namespace, against `httptest` fakes of the two APIs, and calls
  them through a Nexus endpoint from another namespace.
- It covers the spec's request and the API key, errors reaching the caller, polling at the poll
  interval within one attempt, a failed poll resuming the same report in the next attempt, and
  the workflow's fan-out.

## Limits

- **OpenAPI support.** It reads OpenAPI 3.0 only. Parameters must be strings, numbers or
  booleans in the path, query or a header. Request and response bodies must be JSON. It uses the
  lowest 2xx response, and an API key only as a query parameter. The converter rejects anything
  else, naming the operation.
- **Outputs are pointers.** The Go SDK encodes an activity's result even when the activity also
  returns an error, and if that encoding fails, its error replaces the activity's. nexgen's
  `MarshalJSON` validates, so a zero output whose enum or required fields don't validate would
  hide the real error. Activities therefore return `*Output`, nil on error (see
  `openapiconnector.Call`).
- **Activity names.** Activities are named after their operations, so two APIs on one worker
  can't both have an operation with the same name.
- **Backoff in a polling activity.** Successful polls don't reset the HTTP connector's count of
  Transient errors in a row, which only a RateLimited error resets. A Transient error after an
  earlier attempt's Transient error backs off from where that one left off, however many polls
  succeeded in between.
- **Response fields.** Fields the spec doesn't list are kept, in `AdditionalProperties`. A
  response missing a field the spec marks required fails as `BadResponse`.
