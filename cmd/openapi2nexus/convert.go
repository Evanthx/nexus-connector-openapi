package main

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"go/format"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
	"go.yaml.in/yaml/v3"
)

// Config is an API's connector.yaml: which spec to convert, and what the spec can't say.
type Config struct {
	// Spec is the OpenAPI 3.0 file, relative to the config file.
	Spec string `yaml:"spec"`
	// Service is the Nexus service name.
	Service string `yaml:"service"`
	// Description is the service's description. Empty means the spec's title.
	Description string `yaml:"description"`
	// APIKeyParam is the spec's query parameter that holds the API key. It is left out of every
	// input; the worker fills it in from its environment.
	APIKeyParam string `yaml:"apiKeyParam"`
	// Rename maps an operationId to the Nexus operation's name. Unlisted operations keep their
	// operationId, with its first letter lowered.
	Rename map[string]string `yaml:"rename"`
	// Operations are operations written by hand, added to the service as given.
	Operations yaml.Node `yaml:"operations"`
	// Defs are types for those operations, added to $defs as given.
	Defs yaml.Node `yaml:"defs"`
	// Output says where the generated files go and what Go packages they use.
	Output struct {
		// NexusRPC is the contract nexgen generates the types and callers from.
		NexusRPC string `yaml:"nexusrpc"`
		// Go is the file with the operation table and one activity per operation.
		Go string `yaml:"go"`
		// GoPackage is that file's package.
		GoPackage string `yaml:"goPackage"`
		// TypesImport and TypesPackage are the package nexgen generates from the contract.
		TypesImport  string `yaml:"typesImport"`
		TypesPackage string `yaml:"typesPackage"`
	} `yaml:"output"`
}

// Files are a conversion's results.
type Files struct {
	NexusRPC []byte
	Go       []byte
}

// operation is one spec operation, converted.
type operation struct {
	name        string // Nexus operation name
	goName      string // activity and Go identifier stem
	description string
	method      string
	path        string
	params      []param
	hasBody     bool
	input       *omap  // the input's $defs entry
	output      *omap  // the output's $defs entry, or nil if outputType names a spec schema
	outputType  string // Go type name of the output
}

type param struct{ name, in, field string }

// LoadConfig reads a connector.yaml.
func LoadConfig(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Convert converts the spec that cfg names. dir is the directory cfg's paths are relative to.
func Convert(cfg Config, dir string) (Files, error) {
	for name, v := range map[string]string{
		"spec": cfg.Spec, "service": cfg.Service, "output.nexusrpc": cfg.Output.NexusRPC,
		"output.go": cfg.Output.Go, "output.goPackage": cfg.Output.GoPackage,
		"output.typesImport": cfg.Output.TypesImport, "output.typesPackage": cfg.Output.TypesPackage,
	} {
		if v == "" {
			return Files{}, fmt.Errorf("connector.yaml: %s is required", name)
		}
	}
	doc, err := openapi3.NewLoader().LoadFromFile(filepath.Join(dir, cfg.Spec))
	if err != nil {
		return Files{}, fmt.Errorf("loading %s: %w", cfg.Spec, err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.0") {
		return Files{}, fmt.Errorf("%s is OpenAPI %s; this proof of concept reads 3.0 only", cfg.Spec, doc.OpenAPI)
	}

	ops, err := operations(doc, cfg)
	if err != nil {
		return Files{}, err
	}
	contract, err := nexusRPC(doc, cfg, ops)
	if err != nil {
		return Files{}, err
	}
	src, err := goFile(doc, cfg, ops)
	if err != nil {
		return Files{}, err
	}
	return Files{NexusRPC: contract, Go: src}, nil
}

// methods are the HTTP methods the HTTP connector can send, in the order operations are listed.
var methods = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}

func operations(doc *openapi3.T, cfg Config) ([]operation, error) {
	var ops []operation
	keyFound := false
	paths := doc.Paths.Map()
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		item := paths[path]
		if item.Trace != nil || item.Connect != nil {
			return nil, fmt.Errorf("%s: the HTTP connector can't send TRACE or CONNECT", path)
		}
		for _, method := range methods {
			specOp := item.GetOperation(method)
			if specOp == nil {
				continue
			}
			op, err := convertOperation(cfg, path, method, item.Parameters, specOp)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}
			if mergeParams(item.Parameters, specOp.Parameters).GetByInAndName("query", cfg.APIKeyParam) != nil {
				keyFound = true
			}
			ops = append(ops, op)
		}
	}
	if cfg.APIKeyParam != "" && !keyFound {
		return nil, fmt.Errorf("connector.yaml: apiKeyParam %q isn't a query parameter of any operation", cfg.APIKeyParam)
	}
	return ops, nil
}

func convertOperation(cfg Config, path, method string, shared openapi3.Parameters, specOp *openapi3.Operation) (operation, error) {
	if specOp.OperationID == "" {
		return operation{}, errors.New("it has no operationId")
	}
	name := cfg.Rename[specOp.OperationID]
	if name == "" {
		name = lowerFirst(specOp.OperationID)
	}
	op := operation{
		name:        name,
		goName:      upperFirst(name),
		description: cmp.Or(specOp.Summary, specOp.Description),
		method:      method,
		path:        path,
	}

	props := newOmap()
	var required []string
	params, err := inputParams(cfg, mergeParams(shared, specOp.Parameters), props, &required)
	if err != nil {
		return operation{}, err
	}
	op.params = params
	if op.hasBody, err = inputBody(specOp.RequestBody, props, &required); err != nil {
		return operation{}, err
	}
	op.input = newOmap()
	op.input.set("type", "object")
	op.input.set("properties", props)
	if len(required) > 0 {
		op.input.set("required", required)
	}
	op.input.set("additionalProperties", false)

	out, err := responseSchema(specOp)
	if err != nil {
		return operation{}, err
	}
	if name, ok := strings.CutPrefix(out.Ref, "#/components/schemas/"); ok {
		op.outputType = name
		return op, nil
	}
	if op.output, err = schema(out); err != nil {
		return operation{}, fmt.Errorf("response: %w", err)
	}
	op.outputType = op.goName + "Output"
	return op, nil
}

// inputParams adds a property to props for each parameter except the API key, and returns where
// each goes in the request.
func inputParams(cfg Config, specParams openapi3.Parameters, props *omap, required *[]string) ([]param, error) {
	var params []param
	for _, ref := range specParams {
		p := ref.Value
		if p.In == "query" && p.Name == cfg.APIKeyParam {
			continue
		}
		if p.In != "path" && p.In != "query" && p.In != "header" {
			return nil, fmt.Errorf("parameter %s is in %s; only path, query and header are supported", p.Name, p.In)
		}
		if p.Schema == nil || !isScalar(p.Schema.Value) {
			return nil, fmt.Errorf("parameter %s isn't a string, number or boolean; this proof of concept supports only those", p.Name)
		}
		field := fieldName(p.Name)
		if field == "body" || props.has(field) {
			return nil, fmt.Errorf("parameter %s's field %q is already used", p.Name, field)
		}
		s, err := schema(p.Schema)
		if err != nil {
			return nil, fmt.Errorf("parameter %s: %w", p.Name, err)
		}
		if p.Description != "" && p.Schema.Ref == "" && !s.has("description") {
			s.set("description", p.Description)
		}
		props.set(field, s)
		if p.Required {
			*required = append(*required, field)
		}
		params = append(params, param{name: p.Name, in: p.In, field: field})
	}
	return params, nil
}

// inputBody adds the JSON request body, if there is one, to props as "body".
func inputBody(rb *openapi3.RequestBodyRef, props *omap, required *[]string) (bool, error) {
	if rb == nil {
		return false, nil
	}
	media := rb.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return false, errors.New("its request body isn't JSON; this proof of concept supports only JSON")
	}
	s, err := schema(media.Schema)
	if err != nil {
		return false, fmt.Errorf("request body: %w", err)
	}
	props.set("body", s)
	if rb.Value.Required {
		*required = append(*required, "body")
	}
	return true, nil
}

// mergeParams is the path's parameters with the operation's, an operation's replacing the path's
// with the same name and location.
func mergeParams(shared, own openapi3.Parameters) openapi3.Parameters {
	merged := slices.Clone(own)
	for _, p := range shared {
		if own.GetByInAndName(p.Value.In, p.Value.Name) == nil {
			merged = append(merged, p)
		}
	}
	return merged
}

// responseSchema is the JSON schema of the operation's lowest 2xx response.
func responseSchema(specOp *openapi3.Operation) (*openapi3.SchemaRef, error) {
	codes := slices.Sorted(maps.Keys(specOp.Responses.Map()))
	for _, code := range codes {
		if len(code) != 3 || code[0] != '2' {
			continue
		}
		resp := specOp.Responses.Value(code)
		media := resp.Value.Content.Get("application/json")
		if media == nil || media.Schema == nil {
			return nil, fmt.Errorf("its %s response isn't JSON; this proof of concept supports only JSON", code)
		}
		return media.Schema, nil
	}
	return nil, errors.New("it has no 2xx response")
}

func isScalar(s *openapi3.Schema) bool {
	for _, t := range []string{"string", "integer", "number", "boolean"} {
		if s.Type.Is(t) {
			return true
		}
	}
	return false
}

// schema converts an OpenAPI 3.0 schema to JSON Schema 2020-12: a reference to a component
// schema becomes a reference to the $defs entry of the same name, a nullable schema becomes a
// oneOf of the schema and null (nexgen doesn't take a list of types), and a boolean
// exclusiveMinimum or exclusiveMaximum becomes a number. Keywords with no meaning for a contract
// (example, readOnly, writeOnly, xml and the like) are dropped.
func schema(ref *openapi3.SchemaRef) (*omap, error) {
	m := newOmap()
	if ref.Ref != "" {
		name, ok := strings.CutPrefix(ref.Ref, "#/components/schemas/")
		if !ok {
			return nil, fmt.Errorf("reference %s isn't to a component schema", ref.Ref)
		}
		m.set("$ref", "#/$defs/"+name)
		return m, nil
	}
	s := ref.Value
	if s.Discriminator != nil || s.Not != nil {
		return nil, errors.New("discriminator and not aren't supported")
	}
	if s.Nullable {
		notNull := *s
		notNull.Nullable = false
		notNull.Description = ""
		v, err := schema(&openapi3.SchemaRef{Value: &notNull})
		if err != nil {
			return nil, err
		}
		if s.Description != "" {
			m.set("description", s.Description)
		}
		null := newOmap()
		null.set("type", "null")
		m.set("oneOf", []*omap{v, null})
		return m, nil
	}

	switch types := s.Type.Slice(); len(types) {
	case 0:
	case 1:
		m.set("type", types[0])
	default:
		return nil, errors.New("a list of types isn't supported")
	}
	if s.Description != "" {
		m.set("description", s.Description)
	}
	scalarKeywords(m, s)
	for _, add := range []func(*omap, *openapi3.Schema) error{arrayKeywords, objectKeywords, compositionKeywords} {
		if err := add(m, s); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func scalarKeywords(m *omap, s *openapi3.Schema) {
	if s.Format != "" {
		m.set("format", s.Format)
	}
	if len(s.Enum) > 0 {
		m.set("enum", s.Enum)
	}
	if s.Default != nil {
		m.set("default", s.Default)
	}
	bound(m, "minimum", "exclusiveMinimum", s.Min, s.ExclusiveMin)
	bound(m, "maximum", "exclusiveMaximum", s.Max, s.ExclusiveMax)
	if s.MinLength > 0 {
		m.set("minLength", s.MinLength)
	}
	if s.MaxLength != nil {
		m.set("maxLength", *s.MaxLength)
	}
	if s.Pattern != "" {
		m.set("pattern", s.Pattern)
	}
}

func arrayKeywords(m *omap, s *openapi3.Schema) error {
	if s.Items != nil {
		items, err := schema(s.Items)
		if err != nil {
			return err
		}
		m.set("items", items)
	}
	if s.MinItems > 0 {
		m.set("minItems", s.MinItems)
	}
	if s.MaxItems != nil {
		m.set("maxItems", *s.MaxItems)
	}
	if s.UniqueItems {
		m.set("uniqueItems", true)
	}
	return nil
}

func objectKeywords(m *omap, s *openapi3.Schema) error {
	if len(s.Properties) > 0 {
		props := newOmap()
		for _, name := range slices.Sorted(maps.Keys(s.Properties)) {
			p, err := schema(s.Properties[name])
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			props.set(name, p)
		}
		m.set("properties", props)
	}
	if len(s.Required) > 0 {
		m.set("required", s.Required)
	}
	switch ap := s.AdditionalProperties; {
	case ap.Schema != nil:
		v, err := schema(ap.Schema)
		if err != nil {
			return err
		}
		m.set("additionalProperties", v)
	case ap.Has != nil:
		m.set("additionalProperties", *ap.Has)
	default:
		// Not set: any other properties are allowed.
	}
	return nil
}

func compositionKeywords(m *omap, s *openapi3.Schema) error {
	for _, c := range []struct {
		key  string
		refs openapi3.SchemaRefs
	}{{"allOf", s.AllOf}, {"anyOf", s.AnyOf}, {"oneOf", s.OneOf}} {
		if len(c.refs) == 0 {
			continue
		}
		var list []*omap
		for _, r := range c.refs {
			v, err := schema(r)
			if err != nil {
				return err
			}
			list = append(list, v)
		}
		m.set(c.key, list)
	}
	return nil
}

// bound sets an OpenAPI 3.0 minimum or maximum, which is exclusive if its boolean says so.
func bound(m *omap, inclusive, exclusive string, v *float64, ex openapi3.ExclusiveBound) {
	switch {
	case ex.Value != nil:
		m.set(exclusive, *ex.Value)
	case v != nil && ex.Bool != nil && *ex.Bool:
		m.set(exclusive, *v)
	case v != nil:
		m.set(inclusive, *v)
	default:
		// No bound.
	}
}

func nexusRPC(doc *openapi3.T, cfg Config, ops []operation) ([]byte, error) {
	svcOps := newOmap()
	defs := newOmap()
	for _, op := range ops {
		o := newOmap()
		o.set("fqn", op.name)
		if op.description != "" {
			o.set("description", op.description)
		}
		o.set("input", ref(op.goName+"Input"))
		o.set("output", ref(op.outputType))
		svcOps.set(op.name, o)

		defs.set(op.goName+"Input", op.input)
		if op.output != nil {
			defs.set(op.outputType, op.output)
		}
	}
	if err := addNodes(svcOps, &cfg.Operations, "operations"); err != nil {
		return nil, err
	}

	var components openapi3.Schemas
	if doc.Components != nil {
		components = doc.Components.Schemas
	}
	for _, name := range slices.Sorted(maps.Keys(components)) {
		if defs.has(name) {
			return nil, fmt.Errorf("component schema %s has the same name as a generated type", name)
		}
		s, err := schema(components[name])
		if err != nil {
			return nil, fmt.Errorf("component schema %s: %w", name, err)
		}
		defs.set(name, s)
	}
	if err := addNodes(defs, &cfg.Defs, "defs"); err != nil {
		return nil, err
	}

	svc := newOmap()
	svc.set("fqn", cfg.Service)
	svc.set("description", cmp.Or(cfg.Description, doc.Info.Title))
	svc.set("operations", svcOps)
	services := newOmap()
	services.set(cfg.Service, svc)

	root := newOmap()
	root.set("nexusrpc", "1.0.0")
	root.set("$schema", "https://json-schema.org/draft/2020-12/schema")
	root.set("services", services)
	root.set("$defs", defs)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# Code generated by openapi2nexus from %s. DO NOT EDIT.\n", cfg.Spec)
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// addNodes adds the entries of a mapping from connector.yaml to m.
func addNodes(m *omap, n *yaml.Node, what string) error {
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("connector.yaml: %s must be a mapping", what)
	}
	for i := 0; i < len(n.Content); i += 2 {
		key := n.Content[i].Value
		if m.has(key) {
			return fmt.Errorf("connector.yaml: %s %s is already generated from the spec", what, key)
		}
		m.set(key, n.Content[i+1])
	}
	return nil
}

func ref(def string) *omap {
	m := newOmap()
	m.set("$ref", "#/$defs/"+def)
	return m
}

func goFile(doc *openapi3.T, cfg Config, ops []operation) ([]byte, error) {
	baseURL := ""
	if len(doc.Servers) > 0 {
		baseURL = doc.Servers[0].URL
	}
	t := cfg.Output.TypesPackage

	var b bytes.Buffer
	fmt.Fprintf(&b, "// Code generated by openapi2nexus from %s. DO NOT EDIT.\n\n", cfg.Spec)
	fmt.Fprintf(&b, "package %s\n\n", cfg.Output.GoPackage)
	fmt.Fprintf(&b, "import (\n\"context\"\n\n\"github.com/nexus-rpc/sdk-go/nexus\"\n%q\n\"github.com/temporalio/nexus-connector-openapi/openapiconnector\"\n\"go.temporal.io/sdk/worker\"\n)\n\n", cfg.Output.TypesImport)

	fmt.Fprintf(&b, "// API is the server and API key parameter of %s.\n", doc.Info.Title)
	fmt.Fprintf(&b, "var API = openapiconnector.API{BaseURL: %q, APIKeyParam: %q}\n\n", baseURL, cfg.APIKeyParam)

	for _, op := range ops {
		fmt.Fprintf(&b, "// %sOperation is how the %s operation becomes an HTTP request.\n", op.goName, op.name)
		fmt.Fprintf(&b, "var %sOperation = openapiconnector.Operation{\nName: %q,\nMethod: %q,\nPath: %q,\n", op.goName, op.name, op.method, op.path)
		if len(op.params) > 0 {
			b.WriteString("Params: []openapiconnector.Param{\n")
			for _, p := range op.params {
				fmt.Fprintf(&b, "{Name: %q, In: %q, Field: %q},\n", p.name, p.in, p.field)
			}
			b.WriteString("},\n")
		}
		if op.hasBody {
			b.WriteString("HasBody: true,\n")
		}
		b.WriteString("}\n\n")
	}

	b.WriteString("// Activities has one activity per operation in the spec.\n")
	b.WriteString("type Activities struct {\nConnector *openapiconnector.Connector\n}\n\n")
	for _, op := range ops {
		fmt.Fprintf(&b, "// %s is the activity for the %s operation", op.goName, op.name)
		if op.description != "" {
			fmt.Fprintf(&b, ": %s", oneLine(op.description))
		}
		b.WriteString("\n")
		fmt.Fprintf(&b, "func (a *Activities) %s(ctx context.Context, in %s.%sInput) (*%s.%s, error) {\n", op.goName, t, op.goName, t, op.outputType)
		fmt.Fprintf(&b, "return openapiconnector.Call[%s.%s](ctx, a.Connector, %sOperation, in)\n}\n\n", t, op.outputType, op.goName)
	}

	b.WriteString("// RegisterGenerated registers each operation's activity on w, and its Nexus operation in svc.\n")
	b.WriteString("func (a *Activities) RegisterGenerated(svc *nexus.Service, w worker.Registry) {\n")
	for _, op := range ops {
		fmt.Fprintf(&b, "svc.MustRegister(openapiconnector.ActivityOperation(a.Connector, %sOperation, a.%s))\n", op.goName, op.goName)
		fmt.Fprintf(&b, "w.RegisterActivity(a.%s)\n", op.goName)
	}
	b.WriteString("}\n")

	src, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("formatting the generated Go: %w\n%s", err, b.String())
	}
	return src, nil
}

// fieldName is a parameter's input field: its name in lower camel case ("Idempotency-Key" is
// "idempotencyKey").
func fieldName(name string) string {
	words := strings.FieldsFunc(name, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	for i, w := range words {
		if i == 0 {
			words[i] = strings.ToLower(w[:1]) + w[1:]
		} else {
			words[i] = upperFirst(w)
		}
	}
	return strings.Join(words, "")
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// omap is a mapping that keeps its keys in the order they were set, so the YAML is stable.
type omap struct {
	keys []string
	vals map[string]any
}

func newOmap() *omap { return &omap{vals: map[string]any{}} }

func (m *omap) has(key string) bool {
	_, ok := m.vals[key]
	return ok
}

func (m *omap) set(key string, v any) {
	if !m.has(key) {
		m.keys = append(m.keys, key)
	}
	m.vals[key] = v
}

// MarshalYAML implements yaml.Marshaler.
func (m *omap) MarshalYAML() (any, error) {
	n := &yaml.Node{Kind: yaml.MappingNode}
	for _, k := range m.keys {
		var v yaml.Node
		if err := v.Encode(m.vals[k]); err != nil {
			return nil, fmt.Errorf("%s: %w", strconv.Quote(k), err)
		}
		n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: k}, &v)
	}
	return n, nil
}
