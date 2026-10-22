package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGeneratedFilesAreCurrent fails if an API's checked-in contract or activities differ from
// what its connector.yaml generates now. Run make generate to update them.
func TestGeneratedFilesAreCurrent(t *testing.T) {
	for _, dir := range []string{"../../apis/weather", "../../apis/forecastreports"} {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			cfg, err := LoadConfig(filepath.Join(dir, "connector.yaml"))
			require.NoError(t, err)
			files, err := Convert(cfg, dir)
			require.NoError(t, err)

			contract, err := os.ReadFile(filepath.Join(dir, cfg.Output.NexusRPC))
			require.NoError(t, err)
			require.Equal(t, string(contract), string(files.NexusRPC), "run make generate")
			src, err := os.ReadFile(filepath.Join(dir, cfg.Output.Go))
			require.NoError(t, err)
			require.Equal(t, string(src), string(files.Go), "run make generate")
		})
	}
}

func TestConvertRejectsWhatItCantRun(t *testing.T) {
	for name, tc := range map[string]struct{ spec, wantErr string }{
		"array query parameter": {
			spec: `
paths:
  /x:
    get:
      operationId: x
      parameters:
        - {name: ids, in: query, schema: {type: array, items: {type: string}}}
      responses: {"200": {description: ok, content: {application/json: {schema: {type: object}}}}}`,
			wantErr: "parameter ids isn't a string, number or boolean",
		},
		"form body": {
			spec: `
paths:
  /x:
    post:
      operationId: x
      requestBody: {content: {application/x-www-form-urlencoded: {schema: {type: object}}}}
      responses: {"200": {description: ok, content: {application/json: {schema: {type: object}}}}}`,
			wantErr: "its request body isn't JSON",
		},
		"no JSON response": {
			spec: `
paths:
  /x:
    get:
      operationId: x
      responses: {"200": {description: ok, content: {text/plain: {schema: {type: string}}}}}`,
			wantErr: "its 200 response isn't JSON",
		},
		"no operationId": {
			spec: `
paths:
  /x:
    get:
      responses: {"200": {description: ok, content: {application/json: {schema: {type: object}}}}}`,
			wantErr: "it has no operationId",
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			spec := "openapi: 3.0.3\ninfo: {title: T, version: '1'}\n" + tc.spec
			require.NoError(t, os.WriteFile(filepath.Join(dir, "spec.yaml"), []byte(spec), 0o600))
			_, err := Convert(testConfig(), dir)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestConvertRejectsAnUnknownAPIKeyParam(t *testing.T) {
	dir := t.TempDir()
	spec := `openapi: 3.0.3
info: {title: T, version: '1'}
paths:
  /x:
    get:
      operationId: x
      parameters:
        - {name: license, in: query, schema: {type: string}}
      responses: {"200": {description: ok, content: {application/json: {schema: {type: object}}}}}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "spec.yaml"), []byte(spec), 0o600))
	cfg := testConfig()
	cfg.APIKeyParam = "licence"
	_, err := Convert(cfg, dir)
	require.ErrorContains(t, err, `apiKeyParam "licence" isn't a query parameter of any operation`)

	cfg.APIKeyParam = "license"
	_, err = Convert(cfg, dir)
	require.NoError(t, err)
}

// TestConvertIsStable guards TestGeneratedFilesAreCurrent: the same spec must give the same
// contract every time, including the order of allOf, anyOf and oneOf.
func TestConvertIsStable(t *testing.T) {
	dir := t.TempDir()
	spec := `openapi: 3.0.3
info: {title: T, version: '1'}
paths: {}
components:
  schemas:
    A: {type: object, properties: {a: {type: string}}}
    B: {type: object, properties: {b: {type: string}}}
    Both:
      allOf: [{$ref: '#/components/schemas/A'}]
      anyOf: [{$ref: '#/components/schemas/A'}]
      oneOf: [{$ref: '#/components/schemas/A'}, {$ref: '#/components/schemas/B'}]
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "spec.yaml"), []byte(spec), 0o600))
	cfg := testConfig()
	first, err := Convert(cfg, dir)
	require.NoError(t, err)
	require.Regexp(t, `(?s)allOf:.*anyOf:.*oneOf:`, string(first.NexusRPC))
	for range 20 {
		again, err := Convert(cfg, dir)
		require.NoError(t, err)
		require.Equal(t, string(first.NexusRPC), string(again.NexusRPC))
	}
}

// testConfig is a Config for spec.yaml with every required field set.
func testConfig() Config {
	cfg := Config{Spec: "spec.yaml", Service: "S"}
	cfg.Output.NexusRPC = "s.nexusrpc.yaml"
	cfg.Output.Go = "operations.gen.go"
	cfg.Output.GoPackage = "s"
	cfg.Output.TypesImport = "example.com/s/sapi"
	cfg.Output.TypesPackage = "sapi"
	return cfg
}

func TestFieldName(t *testing.T) {
	for in, want := range map[string]string{"zip": "zip", "Idempotency-Key": "idempotencyKey", "page_size": "pageSize"} {
		require.Equal(t, want, fieldName(in))
	}
}
