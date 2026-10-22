// Package forecastreports is the connector for the made-up Forecast Reports API
// (forecastreports.yaml), whose reports are built in the background: one call starts a report, and
// another gets it once it is done. operations.gen.go and reportsapi are generated (make generate);
// this file adds runReport, an operation that starts a report and polls it in one activity.
package forecastreports

import (
	"context"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-openapi/apis/forecastreports/reportsapi"
	"github.com/temporalio/nexus-connector-openapi/openapiconnector"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

// ErrorTypeReportFailed is the error type for a report the API built and that failed. Not retried.
const ErrorTypeReportFailed = "ReportFailed"

// Register registers the ForecastReportService Nexus service on w, with its activities. Each
// operation is backed by a standalone activity: createReport and getReport, generated from the
// spec, make one call each; runReport makes both, checking a pending report again every
// pollInterval.
func Register(w worker.Registry, c *openapiconnector.Connector, pollInterval time.Duration) {
	a := &Activities{Connector: c}
	svc := nexus.NewService(reportsapi.ForecastReportService.ServiceName)
	a.RegisterGenerated(svc, w)

	r := &reports{activities: a, pollInterval: pollInterval}
	svc.MustRegister(openapiconnector.PollingOperation(c, reportsapi.ForecastReportService.RunReport.Name(), pollInterval, r.RunReport))
	w.RegisterActivity(r.RunReport)
	w.RegisterNexusService(svc)
}

type reports struct {
	activities   *Activities
	pollInterval time.Duration
}

// RunReport starts a report and returns it once it is done. It is one activity that polls in a
// loop within one attempt, checking the report every pollInterval while it is pending.
//
// It calls the generated activities as plain functions, and returns their errors unchanged, so
// the HTTP connector's error types and retry delays still apply: a Transient error ends the
// attempt, and the next attempt, after the HTTP connector's backoff, starts again from the top.
// Starting the report again is safe only because createReport is idempotent: every attempt sends
// the activity ID as the Idempotency-Key, and the API returns the report that key first started.
// Like the generated activities, it returns a nil report with an error (see openapiconnector.Call).
func (r *reports) RunReport(ctx context.Context, in reportsapi.NewReport) (*reportsapi.Report, error) {
	report, err := r.activities.CreateReport(ctx, reportsapi.CreateReportInput{
		IdempotencyKey: activity.GetInfo(ctx).ActivityID,
		Body:           in,
	})
	for err == nil && report.Status == reportsapi.ReportStatusPending {
		if err = openapiconnector.WaitToPoll(ctx, r.pollInterval); err == nil {
			report, err = r.activities.GetReport(ctx, reportsapi.GetReportInput{ID: report.ID})
		}
	}
	if err != nil {
		return nil, err
	}

	if report.Status == reportsapi.ReportStatusFailed {
		reason := "no reason given"
		if report.Error != nil {
			reason = *report.Error
		}
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("report %s failed: %s", report.ID, reason), ErrorTypeReportFailed, nil)
	}
	return report, nil
}
