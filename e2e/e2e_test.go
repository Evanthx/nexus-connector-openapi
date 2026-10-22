// Package e2e_test runs the generated connectors on a dev server and calls them through a Nexus
// endpoint from another namespace, as a real caller would. It needs the temporal CLI on the PATH.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-openapi/apis/forecastreports"
	"github.com/temporalio/nexus-connector-openapi/apis/forecastreports/reportsapi"
	"github.com/temporalio/nexus-connector-openapi/apis/weather"
	"github.com/temporalio/nexus-connector-openapi/apis/weather/weatherapi"
	"github.com/temporalio/nexus-connector-openapi/openapiconnector"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	connectorNamespace = "openapi-connector"
	connectorTaskQueue = "nexus-connector-openapi"
	callerNamespace    = "openapi-caller"
	callerTaskQueue    = "openapi-caller"
	endpointName       = "openapi-endpoint"

	interzoidKeyVar = httpconnector.SecretEnvPrefix + "INTERZOID"
	interzoidKey    = "test-license"

	// connectorTimeout is each operation's ScheduleToCloseSeconds; callers wait longer.
	connectorTimeout = 60
	callerTimeout    = 2 * time.Minute
	// pollInterval differs from the retry policy's backoff (1s, then 2s), so the test can tell
	// which one spaced the polls.
	pollInterval = 3 * time.Second
)

// The caller workflows run each operation through the endpoint and return its result.

func callGetWeatherByZip(ctx workflow.Context, in weatherapi.GetWeatherByZipInput) (weatherapi.GetWeatherByZipOutput, error) {
	var out weatherapi.GetWeatherByZipOutput
	err := workflow.NewNexusClient(endpointName, weatherapi.WeatherService.ServiceName).
		ExecuteOperation(ctx, weatherapi.WeatherService.GetWeatherByZip, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: callerTimeout}).
		Get(ctx, &out)
	return out, err
}

func callWeatherForZips(ctx workflow.Context, in weatherapi.WeatherForZipsInput) (weatherapi.WeatherForZipsOutput, error) {
	var out weatherapi.WeatherForZipsOutput
	err := workflow.NewNexusClient(endpointName, weatherapi.WeatherService.ServiceName).
		ExecuteOperation(ctx, weatherapi.WeatherService.WeatherForZips, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: callerTimeout}).
		Get(ctx, &out)
	return out, err
}

func callGetReport(ctx workflow.Context, in reportsapi.GetReportInput) (reportsapi.Report, error) {
	var out reportsapi.Report
	err := workflow.NewNexusClient(endpointName, reportsapi.ForecastReportService.ServiceName).
		ExecuteOperation(ctx, reportsapi.ForecastReportService.GetReport, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: callerTimeout}).
		Get(ctx, &out)
	return out, err
}

func callRunReport(ctx workflow.Context, in reportsapi.NewReport) (reportsapi.Report, error) {
	var out reportsapi.Report
	err := workflow.NewNexusClient(endpointName, reportsapi.ForecastReportService.ServiceName).
		ExecuteOperation(ctx, reportsapi.ForecastReportService.RunReport, in, workflow.NexusOperationOptions{ScheduleToCloseTimeout: callerTimeout}).
		Get(ctx, &out)
	return out, err
}

// interzoid is a fake of Interzoid's weather API. Zip 00000 doesn't exist.
type interzoid struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (s *interzoid) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.reqs = append(s.reqs, r)
	s.mu.Unlock()
	switch {
	case r.URL.Path != "/getweatherzipcode" || r.Method != http.MethodGet:
		http.Error(w, "not found", http.StatusNotFound)
	case r.URL.Query().Get("license") != interzoidKey:
		http.Error(w, "invalid license API key", http.StatusForbidden)
	case r.URL.Query().Get("zip") == "00000":
		http.Error(w, "zip code not found", http.StatusNotFound)
	default:
		zip := r.URL.Query().Get("zip")
		// Extra isn't in the spec, as real APIs often add fields; the output keeps it.
		_, _ = fmt.Fprintf(w, `{"City":"City %s","State":"CA","TempF":"61","Weather":"Fog","Code":"Success","Extra":"x"}`, zip)
	}
}

func (s *interzoid) requests() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*http.Request(nil), s.reqs...)
}

// reports is a fake of the Forecast Reports API. A report is pending until it has been fetched
// readyAfter times; one for zip 99999 fails instead, and fetching one for zip 50300 a second time
// fails once with 503.
type reports struct {
	mu         sync.Mutex
	readyAfter int
	byKey      map[string]string      // Idempotency-Key → report ID
	posts      map[string]int         // Idempotency-Key → POSTs with it
	zips       map[string]string      // report ID → zip
	gets       map[string]int         // report ID → times fetched
	getTimes   map[string][]time.Time // report ID → when it was fetched
	failed     map[string]bool        // report ID → its 503 was sent
}

func newReports(readyAfter int) *reports {
	return &reports{
		readyAfter: readyAfter,
		byKey:      map[string]string{},
		posts:      map[string]int{},
		zips:       map[string]string{},
		gets:       map[string]int{},
		getTimes:   map[string][]time.Time{},
		failed:     map[string]bool{},
	}
}

// keysFor is the Idempotency-Keys of the reports for zip.
func (s *reports) keysFor(zip string) []string {
	var keys []string
	for key, id := range s.byKey {
		if s.zips[id] == zip {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *reports) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/reports":
		key := r.Header.Get("Idempotency-Key")
		s.posts[key]++
		var in struct{ Zip string }
		if key == "" || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&in) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		id, ok := s.byKey[key]
		if !ok {
			id = fmt.Sprintf("r%d", len(s.byKey)+1)
			s.byKey[key] = id
			s.zips[id] = in.Zip
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(s.report(id))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/reports/"):
		id := strings.TrimPrefix(r.URL.Path, "/reports/")
		if _, ok := s.zips[id]; !ok {
			http.Error(w, "no such report", http.StatusNotFound)
			return
		}
		if s.zips[id] == "50300" && s.gets[id] == 1 && !s.failed[id] {
			s.failed[id] = true
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		s.gets[id]++
		s.getTimes[id] = append(s.getTimes[id], time.Now())
		_, _ = w.Write(s.report(id))
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// report is report id's JSON. The caller holds s.mu.
func (s *reports) report(id string) []byte {
	switch {
	case s.zips[id] == "99999":
		return fmt.Appendf(nil, `{"id":%q,"status":"failed","error":"no forecast for this zip"}`, id)
	case s.gets[id] < s.readyAfter:
		return fmt.Appendf(nil, `{"id":%q,"status":"pending"}`, id)
	default:
		return fmt.Appendf(nil, `{"id":%q,"status":"done","forecast":[`+
			`{"date":"2026-10-22","highF":64,"lowF":51,"summary":"Fog"},`+
			`{"date":"2026-10-23","highF":66,"lowF":52,"summary":"Sun"}]}`, id)
	}
}

// TestEndToEnd runs both connectors in one namespace and calls them from another through a Nexus
// endpoint.
func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
	t.Setenv(interzoidKeyVar, interzoidKey)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  cli,
		ClientOptions: &client.Options{Namespace: connectorNamespace},
		ExtraArgs: []string{
			"--namespace", callerNamespace,
			"--dynamic-config-value", "activity.enableCallbacks=true",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })

	connectorClient := server.Client()
	_, err = connectorClient.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: connectorNamespace,
				TaskQueue: connectorTaskQueue,
			}}},
		},
	})
	require.NoError(t, err)

	weatherAPI := &interzoid{}
	weatherSrv := httptest.NewServer(weatherAPI)
	t.Cleanup(weatherSrv.Close)
	reportsAPI := newReports(3)
	reportsSrv := httptest.NewServer(reportsAPI)
	t.Cleanup(reportsSrv.Close)

	// Both connectors share one HTTPActivities, so they share one rate-limit cache.
	httpActivities, err := httpconnector.New()
	require.NoError(t, err)
	connectorWorker := worker.New(connectorClient, connectorTaskQueue, worker.Options{})
	weather.Register(connectorWorker, openapiconnector.New(weather.API, openapiconnector.Config{
		BaseURL:                weatherSrv.URL,
		APIKeyEnvVar:           interzoidKeyVar,
		ScheduleToCloseSeconds: connectorTimeout,
	}, httpActivities))
	forecastreports.Register(connectorWorker, openapiconnector.New(forecastreports.API, openapiconnector.Config{
		BaseURL:                reportsSrv.URL,
		ScheduleToCloseSeconds: connectorTimeout,
	}, httpActivities), pollInterval)
	require.NoError(t, connectorWorker.Start())
	t.Cleanup(connectorWorker.Stop)

	callerClient, err := client.Dial(client.Options{HostPort: server.FrontendHostPort(), Namespace: callerNamespace})
	require.NoError(t, err)
	t.Cleanup(callerClient.Close)
	callerWorker := worker.New(callerClient, callerTaskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callGetWeatherByZip)
	callerWorker.RegisterWorkflow(callWeatherForZips)
	callerWorker.RegisterWorkflow(callGetReport)
	callerWorker.RegisterWorkflow(callRunReport)
	require.NoError(t, callerWorker.Start())
	t.Cleanup(callerWorker.Stop)

	run := func(t *testing.T, wf, in, out any) error {
		t.Helper()
		r, err := callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: callerTaskQueue}, wf, in)
		require.NoError(t, err)
		return r.Get(ctx, out)
	}

	t.Run("getWeatherByZip sends the request the spec describes", func(t *testing.T) {
		before := len(weatherAPI.requests())
		var out weatherapi.GetWeatherByZipOutput
		require.NoError(t, run(t, callGetWeatherByZip, weatherapi.GetWeatherByZipInput{Zip: "94105"}, &out))

		require.Equal(t, "City 94105", *out.City)
		require.Equal(t, "Fog", *out.Weather)
		require.JSONEq(t, `"x"`, string(out.AdditionalProperties["Extra"]), "a field the spec doesn't list is kept")

		reqs := weatherAPI.requests()[before:]
		require.Len(t, reqs, 1)
		require.Equal(t, http.MethodGet, reqs[0].Method)
		require.Equal(t, "94105", reqs[0].URL.Query().Get("zip"))
		require.Equal(t, interzoidKey, reqs[0].URL.Query().Get("license"), "the key comes from the worker's environment")
	})

	t.Run("an API error reaches the caller as the HTTP connector's error", func(t *testing.T) {
		err := run(t, callGetWeatherByZip, weatherapi.GetWeatherByZipInput{Zip: "00000"}, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeClientError, appErr.Type())
	})

	t.Run("a missing API key is rejected before any activity starts", func(t *testing.T) {
		t.Setenv(interzoidKeyVar, "")
		before := len(weatherAPI.requests())
		err := run(t, callGetWeatherByZip, weatherapi.GetWeatherByZipInput{Zip: "94105"}, nil)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), interzoidKeyVar)
		require.Len(t, weatherAPI.requests(), before, "nothing was sent")
	})

	t.Run("weatherForZips calls the API once per zip and returns them in order", func(t *testing.T) {
		before := len(weatherAPI.requests())
		zips := []string{"94105", "10001", "60601"}
		var out weatherapi.WeatherForZipsOutput
		require.NoError(t, run(t, callWeatherForZips, weatherapi.WeatherForZipsInput{Zips: zips}, &out))

		require.Len(t, out.Weather, len(zips))
		for i, zip := range zips {
			require.Equal(t, "City "+zip, *out.Weather[i].City)
		}
		require.Len(t, weatherAPI.requests()[before:], len(zips))
	})

	t.Run("weatherForZips fails if any call fails", func(t *testing.T) {
		err := run(t, callWeatherForZips, weatherapi.WeatherForZipsInput{Zips: []string{"94105", "00000"}}, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeClientError, appErr.Type())
	})

	t.Run("getReport's API error reaches the caller", func(t *testing.T) {
		// A zero Report doesn't validate (its status must be in the enum), so this fails if the
		// activity returns one with its error: encoding it would replace the ClientError.
		err := run(t, callGetReport, reportsapi.GetReportInput{ID: "missing"}, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeClientError, appErr.Type())
	})

	t.Run("runReport polls within one attempt until the report is done", func(t *testing.T) {
		var out reportsapi.Report
		require.NoError(t, run(t, callRunReport, reportsapi.NewReport{Zip: "94105", Days: 2}, &out))

		require.Equal(t, reportsapi.ReportStatusDone, out.Status)
		require.Len(t, out.Forecast, 2)
		require.Equal(t, "Sun", *out.Forecast[1].Summary)

		reportsAPI.mu.Lock()
		defer reportsAPI.mu.Unlock()
		keys := reportsAPI.keysFor("94105")
		require.Len(t, keys, 1)
		require.Equal(t, 1, reportsAPI.posts[keys[0]], "one attempt, so one start")
		times := reportsAPI.getTimes[out.ID]
		require.Len(t, times, 3, "pending twice, then done")
		for i := 1; i < len(times); i++ {
			require.InDelta(t, pollInterval, times[i].Sub(times[i-1]), float64(500*time.Millisecond), "poll %d waits the poll interval", i+1)
		}
	})

	t.Run("a failed poll ends the attempt, and the next attempt resumes the same report", func(t *testing.T) {
		var out reportsapi.Report
		require.NoError(t, run(t, callRunReport, reportsapi.NewReport{Zip: "50300", Days: 1}, &out))
		require.Equal(t, reportsapi.ReportStatusDone, out.Status)

		reportsAPI.mu.Lock()
		defer reportsAPI.mu.Unlock()
		keys := reportsAPI.keysFor("50300")
		require.Len(t, keys, 1, "the second attempt's start returned the first attempt's report")
		require.Equal(t, 2, reportsAPI.posts[keys[0]], "two attempts, each starting with the same Idempotency-Key")
		require.True(t, reportsAPI.failed[out.ID])
	})

	t.Run("runReport fails when the report does", func(t *testing.T) {
		err := run(t, callRunReport, reportsapi.NewReport{Zip: "99999", Days: 1}, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, forecastreports.ErrorTypeReportFailed, appErr.Type())
		require.Contains(t, appErr.Message(), "no forecast for this zip")
	})
}
