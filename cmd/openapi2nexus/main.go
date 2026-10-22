// Command openapi2nexus converts an OpenAPI 3.0 spec into a Nexus contract and the Go that runs
// it. Given an API's connector.yaml, it writes:
//
//   - the .nexusrpc.yaml contract, with one operation per spec operation, which nexgen turns into
//     typed callers in each language and the Go types the activities use;
//   - a Go file with the operation table and one activity per operation, each sending its request
//     through the HTTP connector (package openapiconnector).
//
// Usage: openapi2nexus path/to/connector.yaml
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: openapi2nexus path/to/connector.yaml")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "openapi2nexus:", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	dir := filepath.Dir(configPath)
	files, err := Convert(cfg, dir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, cfg.Output.NexusRPC), files.NexusRPC, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, cfg.Output.Go), files.Go, 0o644)
}
