// Package openapiconnector runs the operations that openapi2nexus generates from an OpenAPI spec.
// Each operation is a Nexus operation backed by a standalone activity. The activity turns the
// operation's typed input into an HTTP request and sends it with the HTTP connector's SendRequest,
// called as a library, so retries, rate limiting and secret redaction are the HTTP connector's.
// It then decodes the response body into the operation's typed output.
package openapiconnector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
)

// ErrorTypeBadResponse is the error type, besides the HTTP connector's httpconnector.ErrorType*
// values, for an API that answered 2xx with a body that doesn't decode into the operation's output
// type. Not retried.
const ErrorTypeBadResponse = "BadResponse"

// API is what openapi2nexus generates for one OpenAPI spec, besides its operations.
type API struct {
	// BaseURL is the spec's first server.
	BaseURL string
	// APIKeyParam is the query parameter the API key goes in, or "" to send it as a Bearer token.
	APIKeyParam string
}

// Param is where one field of an operation's input goes in the HTTP request.
type Param struct {
	// Name is the parameter's name in the spec: the path placeholder, query key or header name.
	Name string
	// In is "path", "query" or "header".
	In string
	// Field is the input's JSON property that holds the value.
	Field string
}

// Operation is how one generated Nexus operation becomes an HTTP request.
type Operation struct {
	// Name is the Nexus operation's name.
	Name string
	// Method is the HTTP method.
	Method string
	// Path is the path after the base URL, with {name} placeholders for path parameters.
	Path string
	// Params are the input's fields that go in the path, query or headers.
	Params []Param
	// HasBody is true if the input's "body" property is the JSON request body.
	HasBody bool
}

// Config is the deployment's settings for one API. The caller can't change them.
type Config struct {
	// BaseURL replaces API.BaseURL if set.
	BaseURL string
	// APIKeyEnvVar names the environment variable on the worker that holds the API key. It must
	// start with httpconnector.SecretEnvPrefix. Empty means the API takes no key.
	APIKeyEnvVar string
	// ScheduleToCloseSeconds bounds each operation, including every retry. Zero means the HTTP
	// connector's default, 25 hours. A caller's Nexus operation timeout must be longer.
	ScheduleToCloseSeconds int64
}

// Connector runs one API's operations.
type Connector struct {
	api  API
	cfg  Config
	http *httpconnector.HTTPActivities
}

// New returns a Connector for api. Every Connector on a worker should share one http, so they
// share one rate-limit cache.
func New(api API, cfg Config, http *httpconnector.HTTPActivities) *Connector {
	return &Connector{api: api, cfg: cfg, http: http}
}

// Request is the HTTP connector request for op with input in.
func (c *Connector) Request(op Operation, in any) (httpapi.SendRequestInput, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return httpapi.SendRequestInput{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return httpapi.SendRequestInput{}, fmt.Errorf("the input to %s isn't a JSON object: %w", op.Name, err)
	}

	path := op.Path
	query := map[string]string{}
	headers := map[string]string{"Accept": "application/json"}
	for _, p := range op.Params {
		v, ok, err := scalar(fields[p.Field])
		if err != nil {
			return httpapi.SendRequestInput{}, fmt.Errorf("%s: %w", p.Field, err)
		}
		if !ok {
			continue
		}
		switch p.In {
		case "path":
			path = strings.ReplaceAll(path, "{"+p.Name+"}", url.PathEscape(v))
		case "query":
			query[p.Name] = v
		case "header":
			headers[p.Name] = v
		default:
			return httpapi.SendRequestInput{}, fmt.Errorf("%s: parameter location %q isn't supported", p.Field, p.In)
		}
	}
	if strings.Contains(path, "{") {
		return httpapi.SendRequestInput{}, fmt.Errorf("%s: path %s is missing a parameter", op.Name, path)
	}

	base := c.api.BaseURL
	if c.cfg.BaseURL != "" {
		base = c.cfg.BaseURL
	}
	method := httpapi.SendRequestInputMethod(op.Method)
	req := httpapi.SendRequestInput{URL: strings.TrimSuffix(base, "/") + path, Method: &method}
	if len(query) > 0 {
		req.QueryParams = &httpapi.SendRequestInputQueryParams{AdditionalProperties: query}
	}
	if body, ok := fields["body"]; op.HasBody && ok {
		b := string(body)
		req.Body = &b
		headers["Content-Type"] = "application/json"
	}
	req.Headers = &httpapi.SendRequestInputHeaders{AdditionalProperties: headers}
	if c.cfg.APIKeyEnvVar != "" {
		req.APIKeyEnvVar = new(c.cfg.APIKeyEnvVar)
		if c.api.APIKeyParam != "" {
			req.APIKeyParam = new(c.api.APIKeyParam)
		}
	}
	if c.cfg.ScheduleToCloseSeconds > 0 {
		req.ScheduleToCloseSeconds = new(c.cfg.ScheduleToCloseSeconds)
	}
	return req, nil
}

// scalar is the text of a JSON string, number or boolean, for a path, query or header value. ok
// is false for a missing or null value.
func scalar(v json.RawMessage) (s string, ok bool, err error) {
	switch {
	case len(v) == 0 || string(v) == "null":
		return "", false, nil
	case v[0] == '"':
		err := json.Unmarshal(v, &s)
		return s, err == nil, err
	case v[0] == '{' || v[0] == '[':
		return "", false, errors.New("only strings, numbers and booleans can go in a path, query or header")
	default:
		return string(v), true, nil
	}
}

// Call makes op's HTTP request for in and decodes the response body into an O. It must run in an
// activity. It returns the HTTP connector's errors unchanged, so their types and retry delays still
// apply; a body that doesn't decode is a BadResponse error.
//
// The output is a pointer, nil with an error, and activities must return it that way. The SDK
// encodes an activity's result even when the activity also returns an error, and if that fails
// the encoding error replaces the activity's. A generated type's MarshalJSON validates, so a zero
// value with a required enum, such as an empty status, would fail and hide the real error.
func Call[O any](ctx context.Context, c *Connector, op Operation, in any) (*O, error) {
	req, err := c.Request(op, in)
	if err != nil {
		return nil, httpconnector.InvalidRequestError(err)
	}
	resp, err := c.http.SendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	out := new(O)
	if strings.TrimSpace(resp.Body) == "" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(resp.Body), out); err != nil {
		return nil, temporal.NewApplicationErrorWithOptions(
			fmt.Sprintf("%s: the response doesn't match the spec: %v", op.Name, err), ErrorTypeBadResponse,
			temporal.ApplicationErrorOptions{NonRetryable: true, Details: []any{resp}})
	}
	return out, nil
}

// ActivityOptions are the options for an activity of this API with the given ID: the HTTP
// connector's retry policy and timeouts, bounded by Config.ScheduleToCloseSeconds.
func (c *Connector) ActivityOptions(id string) client.StartActivityOptions {
	return httpconnector.ActivityOptions(id, c.timeouts())
}

// WorkflowActivityOptions are ActivityOptions for running this API's activities from a workflow.
func (c *Connector) WorkflowActivityOptions() workflow.ActivityOptions {
	opts := c.ActivityOptions("")
	return workflow.ActivityOptions{
		StartToCloseTimeout:    opts.StartToCloseTimeout,
		ScheduleToCloseTimeout: opts.ScheduleToCloseTimeout,
		RetryPolicy:            opts.RetryPolicy,
	}
}

// PollingActivityOptions are the options for an activity that polls in a loop within one
// attempt, with the given ID. The attempt may last as long as the whole operation
// (Config.ScheduleToCloseSeconds) instead of one request's 60 seconds. A HeartbeatTimeout of
// pollInterval plus one request's 60 seconds, the longest gap between WaitToPoll's heartbeats,
// notices a worker that died; the attempt is then retried. Errors retry as for any of this API's
// activities.
func (c *Connector) PollingActivityOptions(id string, pollInterval time.Duration) client.StartActivityOptions {
	opts := c.ActivityOptions(id)
	opts.HeartbeatTimeout = pollInterval + opts.StartToCloseTimeout
	opts.StartToCloseTimeout = opts.ScheduleToCloseTimeout
	return opts
}

// WaitToPoll heartbeats, then waits interval, for an activity polling in a loop. It returns early
// with ctx's error if the activity is canceled or times out.
//
// The heartbeat carries no details, because the HTTP connector keeps its count of Transient
// errors in a row there. A polling activity must leave them to it, and must return the errors
// its calls return rather than retry them in the loop: the count only advances between attempts.
func WaitToPoll(ctx context.Context, interval time.Duration) error {
	activity.RecordHeartbeat(ctx)
	t := time.NewTimer(interval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// timeouts is a request that carries only this API's timeouts.
func (c *Connector) timeouts() httpapi.SendRequestInput {
	var req httpapi.SendRequestInput
	if c.cfg.ScheduleToCloseSeconds > 0 {
		req.ScheduleToCloseSeconds = new(c.cfg.ScheduleToCloseSeconds)
	}
	return req
}

// ActivityOperation is the Nexus operation for op, backed by the standalone activity act. It
// rejects a request that can't be made, or that would outlast the caller's operation timeout,
// with a BadRequest handler error, without starting the activity.
func ActivityOperation[I, O any](c *Connector, op Operation, act func(context.Context, I) (O, error)) nexus.RegisterableOperation {
	validate := func(in I) error {
		req, err := c.Request(op, in)
		if err != nil {
			return err
		}
		return httpconnector.Validate(req)
	}
	return activityOperation(c, op.Name, act, validate, c.ActivityOptions)
}

// PollingOperation is the Nexus operation name, backed by the standalone activity act: one written
// by hand that makes several of this API's calls, polling in a loop within one attempt (see
// PollingActivityOptions and WaitToPoll).
func PollingOperation[I, O any](c *Connector, name string, pollInterval time.Duration, act func(context.Context, I) (O, error)) nexus.RegisterableOperation {
	return activityOperation(c, name, act, func(I) error { return nil }, func(id string) client.StartActivityOptions {
		return c.PollingActivityOptions(id, pollInterval)
	})
}

func activityOperation[I, O any](
	c *Connector,
	name string,
	act func(context.Context, I) (O, error),
	validate func(I) error,
	options func(id string) client.StartActivityOptions,
) nexus.RegisterableOperation {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[I, O]{
		Name: name,
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			in I,
			opts temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[O], error) {
			err := validate(in)
			if err == nil {
				err = httpconnector.CheckCallerTimeout(c.timeouts(), opts.Header.Get(nexus.HeaderOperationTimeout))
			}
			if err != nil {
				return temporalnexus.TemporalOperationResult[O]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: httpconnector.InvalidRequestError(err),
				}
			}
			// The ID comes from the Nexus request ID, so a retried start reaches the same activity.
			return temporalnexus.StartActivity(ctx, nc, options(name+"-"+opts.RequestID), act, in)
		},
	})
}
