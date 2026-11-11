// Package weather is the connector for Interzoid's weather-by-zip-code API (interzoid.json).
// operations.gen.go and weatherapi are generated (make generate); this file adds what the spec
// can't: weatherForZips, an operation backed by a workflow that makes several calls.
package weather

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-openapi/apis/weather/weatherapi"
	"github.com/temporalio/nexus-connector-openapi/openapiconnector"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Register registers the WeatherService Nexus service on w, with its activities and workflow:
//
//   - getWeatherByZip, generated from the spec, is backed by a standalone activity.
//   - weatherForZips is backed by the WeatherForZips workflow. Like the activity-backed
//     operations, it is refused if the caller's operation has too little time left, and its
//     activities stop shortly before the caller's operation times out (see
//     openapiconnector.Connector.ScheduleToClose).
func Register(w worker.Registry, c *openapiconnector.Connector) {
	a := &Activities{Connector: c}
	svc := nexus.NewService(weatherapi.WeatherService.ServiceName)
	a.RegisterGenerated(svc, w)

	wf := &workflows{activities: a, activityOptions: c.WorkflowActivityOptions(c.MaxDuration())}
	w.RegisterWorkflowWithOptions(wf.WeatherForZips, workflow.RegisterOptions{Name: "WeatherForZips"})
	svc.MustRegister(temporalnexus.NewWorkflowRunOperation(
		weatherapi.WeatherService.WeatherForZips.Name(),
		wf.WeatherForZips,
		func(_ context.Context, _ weatherapi.WeatherForZipsInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			total, err := c.ScheduleToClose(opts.Header.Get(nexus.HeaderOperationTimeout))
			if err != nil {
				return client.StartWorkflowOptions{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: httpconnector.InvalidRequestError(err),
				}
			}
			return client.StartWorkflowOptions{
				// The ID comes from the Nexus request ID, so a retried start reaches the same workflow.
				ID: "weatherForZips-" + opts.RequestID,
				// The workflow outlasts its activities by workflowMargin, so their errors reach
				// the caller rather than the workflow's timeout.
				WorkflowExecutionTimeout: total + workflowMargin,
			}, nil
		},
	))
	w.RegisterNexusService(svc)
}

// workflowMargin is how much longer WeatherForZips may run than its activities: time to fail with
// their error before its own timeout.
const workflowMargin = 5 * time.Second

// workflows holds what the workflows need from the worker. Both are set once at registration,
// so using them is deterministic.
type workflows struct {
	// activities names the activities to run; workflow code never calls them directly.
	activities      *Activities
	activityOptions workflow.ActivityOptions
}

// WeatherForZips gets the weather for each zip code by running the GetWeatherByZip activity for
// all of them at once. Each activity retries on its own, as it would behind getWeatherByZip. The
// first that fails fails the workflow, and so the operation.
func (wf *workflows) WeatherForZips(ctx workflow.Context, in weatherapi.WeatherForZipsInput) (weatherapi.WeatherForZipsOutput, error) {
	opts := wf.activityOptions
	if t := workflow.GetInfo(ctx).WorkflowExecutionTimeout; t > 0 {
		opts.ScheduleToCloseTimeout = t - workflowMargin
	}
	ctx = workflow.WithActivityOptions(ctx, opts)
	futures := make([]workflow.Future, len(in.Zips))
	for i, zip := range in.Zips {
		futures[i] = workflow.ExecuteActivity(ctx, wf.activities.GetWeatherByZip, weatherapi.GetWeatherByZipInput{Zip: zip})
	}
	out := weatherapi.WeatherForZipsOutput{Weather: make([]weatherapi.GetWeatherByZipOutput, len(futures))}
	for i, f := range futures {
		if err := f.Get(ctx, &out.Weather[i]); err != nil {
			return weatherapi.WeatherForZipsOutput{}, err
		}
	}
	return out, nil
}
