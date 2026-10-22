package openapiconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

func TestRequest(t *testing.T) {
	op := Operation{
		Name:   "updateItem",
		Method: "PUT",
		Path:   "/items/{id}",
		Params: []Param{
			{Name: "id", In: "path", Field: "id"},
			{Name: "dryRun", In: "query", Field: "dryRun"},
			{Name: "limit", In: "query", Field: "limit"},
			{Name: "X-Trace", In: "header", Field: "xTrace"},
		},
		HasBody: true,
	}
	c := New(API{BaseURL: "https://api.example.com/v1/", APIKeyParam: "key"},
		Config{APIKeyEnvVar: "HTTP_CONNECTOR_SECRET_X", ScheduleToCloseSeconds: 60}, nil)

	req, err := c.Request(op, map[string]any{
		"id":     "a/b c",
		"dryRun": true,
		"limit":  10,
		"xTrace": "t1",
		"body":   map[string]any{"name": "n"},
	})
	require.NoError(t, err)

	require.Equal(t, "https://api.example.com/v1/items/a%2Fb%20c", req.URL)
	require.Equal(t, httpapi.SendRequestInputMethodPut, *req.Method)
	require.Equal(t, map[string]string{"dryRun": "true", "limit": "10"}, req.QueryParams.AdditionalProperties)
	require.Equal(t, map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
		"X-Trace":      "t1",
	}, req.Headers.AdditionalProperties)
	require.JSONEq(t, `{"name":"n"}`, *req.Body)
	require.Equal(t, "HTTP_CONNECTOR_SECRET_X", *req.APIKeyEnvVar)
	require.Equal(t, "key", *req.APIKeyParam)
	require.Equal(t, int64(60), *req.ScheduleToCloseSeconds)
}

func TestPollingActivityOptions(t *testing.T) {
	c := New(API{BaseURL: "https://api.example.com"}, Config{ScheduleToCloseSeconds: 3600}, nil)
	opts := c.PollingActivityOptions("id", 5*time.Second)
	require.Equal(t, time.Hour, opts.ScheduleToCloseTimeout)
	require.Equal(t, time.Hour, opts.StartToCloseTimeout, "one attempt can poll for the whole operation")
	require.Equal(t, 65*time.Second, opts.HeartbeatTimeout, "the poll interval plus one request")
	require.Equal(t, c.ActivityOptions("id").RetryPolicy, opts.RetryPolicy)
}

func TestRequestLeavesOutUnsetParams(t *testing.T) {
	op := Operation{Name: "list", Method: "GET", Path: "/items", Params: []Param{{Name: "q", In: "query", Field: "q"}}}
	req, err := New(API{BaseURL: "https://api.example.com"}, Config{}, nil).Request(op, map[string]any{"q": nil})
	require.NoError(t, err)
	require.Nil(t, req.QueryParams)
	require.Nil(t, req.Body)
	require.Nil(t, req.APIKeyEnvVar)
}

func TestRequestRejectsAnObjectParam(t *testing.T) {
	op := Operation{Name: "get", Method: "GET", Path: "/items", Params: []Param{{Name: "f", In: "query", Field: "f"}}}
	_, err := New(API{BaseURL: "https://api.example.com"}, Config{}, nil).Request(op, map[string]any{"f": map[string]any{}})
	require.ErrorContains(t, err, "only strings, numbers and booleans")
}
