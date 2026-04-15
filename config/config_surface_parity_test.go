package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigSurfaceParity(t *testing.T) {
	root := repoRootFromTestFile(t)

	runtimeKeys := runtimeConfigKeys()
	runtimeCore := filterKeysByPrefixes(runtimeKeys, "nats.", "clickhouse.", "vault.", "server.", "state.")

	exampleRoot := mustReadYAML(t, filepath.Join(root, "config.example.yaml"))
	exampleKeys := map[string]struct{}{}
	flattenYAMLKeys(exampleRoot, "", exampleKeys)
	exampleCore := filterKeysByPrefixes(exampleKeys, "nats.", "clickhouse.", "vault.", "server.", "state.")

	valuesRoot := mustReadYAML(t, filepath.Join(root, "charts/siphon/values.yaml"))
	configSectionRaw, ok := valuesRoot["config"]
	if !ok {
		t.Fatalf("charts/siphon/values.yaml is missing top-level config")
	}
	configSection, ok := configSectionRaw.(map[string]any)
	if !ok {
		t.Fatalf("charts/siphon/values.yaml config section has unexpected type %T", configSectionRaw)
	}
	valuesKeys := map[string]struct{}{}
	flattenYAMLKeys(configSection, "", valuesKeys)
	valuesCore := filterKeysByPrefixes(valuesKeys, "nats.", "clickhouse.", "vault.", "server.", "state.")

	schemaDoc := mustReadJSON(t, filepath.Join(root, "charts/siphon/values.schema.json"))
	schemaConfig := mustNestedMap(t, schemaDoc, "properties", "config")
	schemaKeys := map[string]struct{}{}
	flattenSchemaKeys(schemaConfig, "", schemaKeys)
	schemaCore := filterKeysByPrefixes(schemaKeys, "nats.", "clickhouse.", "vault.", "server.", "state.")

	assertKeySetsEqual(t, "config.example.yaml core config keys", runtimeCore, exampleCore)
	assertKeySetsEqual(t, "charts/siphon/values.yaml config core keys", runtimeCore, valuesCore)
	assertKeySetsEqual(t, "charts/siphon/values.schema.json config core keys", runtimeCore, schemaCore)
}

func repoRootFromTestFile(t *testing.T) string {
	t.Helper()
	_, filePath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("resolve runtime caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filePath), ".."))
}

func runtimeConfigKeys() map[string]struct{} {
	out := map[string]struct{}{}
	collectRuntimeKeys(reflect.TypeOf(Config{}), "", out)
	return out
}

func collectRuntimeKeys(t reflect.Type, prefix string, out map[string]struct{}) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		key := strings.TrimSpace(field.Tag.Get("koanf"))
		if key == "" || key == "-" {
			continue
		}
		path := joinPath(prefix, key)
		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		switch fieldType.Kind() {
		case reflect.Struct:
			collectRuntimeKeys(fieldType, path, out)
		case reflect.Map:
			out[path] = struct{}{}
			elemType := fieldType.Elem()
			for elemType.Kind() == reflect.Pointer {
				elemType = elemType.Elem()
			}
			if elemType.Kind() == reflect.Struct {
				collectRuntimeKeys(elemType, path+".*", out)
			}
		default:
			out[path] = struct{}{}
		}
	}
}

func mustReadYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read yaml %s: %v", path, err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse yaml %s: %v", path, err)
	}
	return root
}

func mustReadJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json %s: %v", path, err)
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("parse json %s: %v", path, err)
	}
	return root
}

func mustNestedMap(t *testing.T, root map[string]any, keys ...string) map[string]any {
	t.Helper()
	current := root
	for _, key := range keys {
		value, ok := current[key]
		if !ok {
			t.Fatalf("missing object key %q in schema traversal", key)
		}
		next, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("expected object for key %q, got %T", key, value)
		}
		current = next
	}
	return current
}

func flattenYAMLKeys(value any, prefix string, out map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			if prefix != "" {
				out[prefix] = struct{}{}
			}
			return
		}
		for key, child := range typed {
			normalized := key
			if prefix == "providers" || prefix == "providers.*.tenants" {
				normalized = "*"
			}
			next := joinPath(prefix, normalized)
			flattenYAMLKeys(child, next, out)
		}
	case map[any]any:
		converted := make(map[string]any, len(typed))
		for key, child := range typed {
			converted[fmt.Sprint(key)] = child
		}
		flattenYAMLKeys(converted, prefix, out)
	case []any:
		if prefix != "" {
			out[prefix] = struct{}{}
		}
	default:
		if prefix != "" {
			out[prefix] = struct{}{}
		}
	}
}

func flattenSchemaKeys(node any, prefix string, out map[string]struct{}) {
	object, ok := node.(map[string]any)
	if !ok {
		if prefix != "" {
			out[prefix] = struct{}{}
		}
		return
	}

	if propsRaw, ok := object["properties"]; ok {
		if props, ok := propsRaw.(map[string]any); ok && len(props) > 0 {
			for key, child := range props {
				next := joinPath(prefix, key)
				flattenSchemaKeys(child, next, out)
			}
			return
		}
	}

	if additionalRaw, ok := object["additionalProperties"]; ok {
		switch additional := additionalRaw.(type) {
		case map[string]any:
			flattenSchemaKeys(additional, joinPath(prefix, "*"), out)
			return
		case bool:
			if additional && prefix != "" {
				out[joinPath(prefix, "*")] = struct{}{}
				return
			}
		}
	}

	if prefix != "" {
		out[prefix] = struct{}{}
	}
}

func filterKeysByPrefixes(keys map[string]struct{}, prefixes ...string) map[string]struct{} {
	filtered := map[string]struct{}{}
	for key := range keys {
		for _, prefix := range prefixes {
			if strings.HasPrefix(key, prefix) {
				filtered[key] = struct{}{}
				break
			}
		}
	}
	return filtered
}

func assertKeySetsEqual(t *testing.T, name string, expected map[string]struct{}, actual map[string]struct{}) {
	t.Helper()
	missing := setDiff(expected, actual)
	extra := setDiff(actual, expected)
	if len(missing) == 0 && len(extra) == 0 {
		return
	}

	sort.Strings(missing)
	sort.Strings(extra)
	t.Fatalf(
		"%s mismatch\nmissing (%d): %s\nextra (%d): %s",
		name,
		len(missing),
		strings.Join(missing, ", "),
		len(extra),
		strings.Join(extra, ", "),
	)
}

func setDiff(left map[string]struct{}, right map[string]struct{}) []string {
	diff := make([]string, 0)
	for key := range left {
		if _, ok := right[key]; !ok {
			diff = append(diff, key)
		}
	}
	return diff
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
