// Package weather is the connector for Interzoid's weather-by-zip-code API (interzoid.json).
// operations.gen.go and weatherapi are generated (make generate); this file adds what the spec
// can't: weatherForZips, an operation backed by a workflow that makes several calls.
package weather

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
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
//   - weatherForZips is backed by the WeatherForZips workflow.
func Register(w worker.Registry, c *openapiconnector.Connector) {
	a := &Activities{Connector: c}
	svc := nexus.NewService(weatherapi.WeatherService.ServiceName)
	a.RegisterGenerated(svc, w)

	wf := &workflows{activities: a, activityOptions: c.WorkflowActivityOptions()}
	w.RegisterWorkflowWithOptions(wf.WeatherForZips, workflow.RegisterOptions{Name: "WeatherForZips"})
	svc.MustRegister(temporalnexus.NewWorkflowRunOperation(
		weatherapi.WeatherService.WeatherForZips.Name(),
		wf.WeatherForZips,
		func(_ context.Context, _ weatherapi.WeatherForZipsInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			// The ID comes from the Nexus request ID, so a retried start reaches the same workflow.
			return client.StartWorkflowOptions{ID: "weatherForZips-" + opts.RequestID}, nil
		},
	))
	w.RegisterNexusService(svc)
}

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
	ctx = workflow.WithActivityOptions(ctx, wf.activityOptions)
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
