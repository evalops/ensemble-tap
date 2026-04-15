package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	vaultapi "github.com/hashicorp/vault/api"
)

const vaultRefPrefix = "vault://"

func (c *Config) resolveVaultReferences() error {
	root := reflect.ValueOf(c).Elem()
	if !hasVaultReferences(root) {
		return nil
	}

	reader, err := newVaultReferenceReader(c.Vault)
	if err != nil {
		return err
	}
	return resolveVaultReferencesInValue(root, "config", reader)
}

func hasVaultReferences(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return false
		}
		return hasVaultReferences(value.Elem())
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if hasVaultReferences(value.Field(i)) {
				return true
			}
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			if hasVaultReferences(value.MapIndex(key)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if hasVaultReferences(value.Index(i)) {
				return true
			}
		}
	case reflect.String:
		return strings.HasPrefix(strings.TrimSpace(value.String()), vaultRefPrefix)
	}
	return false
}

func resolveVaultReferencesInValue(value reflect.Value, fieldPath string, reader func(path, key string) (string, error)) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return resolveVaultReferencesInValue(value.Elem(), fieldPath, reader)
	case reflect.Struct:
		valueType := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := valueType.Field(i)
			if field.PkgPath != "" {
				continue
			}
			nextPath := field.Name
			if fieldPath != "" {
				nextPath = fieldPath + "." + field.Name
			}
			if err := resolveVaultReferencesInValue(value.Field(i), nextPath, reader); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil
		}
		for _, key := range value.MapKeys() {
			item := value.MapIndex(key)
			editable := reflect.New(item.Type()).Elem()
			editable.Set(item)
			nextPath := fmt.Sprintf("%s[%s]", fieldPath, key.String())
			if err := resolveVaultReferencesInValue(editable, nextPath, reader); err != nil {
				return err
			}
			value.SetMapIndex(key, editable)
		}
		return nil
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			nextPath := fmt.Sprintf("%s[%d]", fieldPath, i)
			if err := resolveVaultReferencesInValue(value.Index(i), nextPath, reader); err != nil {
				return err
			}
		}
		return nil
	case reflect.String:
		raw := strings.TrimSpace(value.String())
		if !strings.HasPrefix(raw, vaultRefPrefix) {
			return nil
		}
		secretPath, secretKey, err := parseVaultReference(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		resolved, err := reader(secretPath, secretKey)
		if err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		value.SetString(resolved)
	}
	return nil
}

func parseVaultReference(raw string) (string, string, error) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, vaultRefPrefix) {
		return "", "", fmt.Errorf("vault reference %q must start with %s", raw, vaultRefPrefix)
	}
	ref := strings.TrimPrefix(trimmed, vaultRefPrefix)
	pathPart, keyPart, hasKey := strings.Cut(ref, "#")
	path := strings.TrimSpace(strings.TrimPrefix(pathPart, "/"))
	if path == "" {
		return "", "", fmt.Errorf("vault reference %q must include a non-empty path", raw)
	}

	key := "value"
	if hasKey {
		key = strings.TrimSpace(keyPart)
		if key == "" {
			return "", "", fmt.Errorf("vault reference %q has an empty key selector", raw)
		}
	}
	return path, key, nil
}

func newVaultReferenceReader(cfg VaultConfig) (func(path, key string) (string, error), error) {
	address := strings.TrimSpace(cfg.Address)
	if address == "" {
		return nil, fmt.Errorf("vault.address must not be empty when vault:// references are used")
	}

	clientCfg := vaultapi.DefaultConfig()
	clientCfg.Address = address
	client, err := vaultapi.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("create vault client: %w", err)
	}
	if namespace := strings.TrimSpace(cfg.Namespace); namespace != "" {
		client.SetNamespace(namespace)
	}

	authMethod := strings.ToLower(strings.TrimSpace(cfg.AuthMethod))
	if authMethod == "" {
		authMethod = "kubernetes"
	}
	switch authMethod {
	case "token":
		token := strings.TrimSpace(cfg.Token)
		if token == "" && strings.TrimSpace(cfg.TokenFile) != "" {
			tokenBytes, readErr := readFileWithinRoot(strings.TrimSpace(cfg.TokenFile))
			if readErr != nil {
				return nil, fmt.Errorf("read vault token file: %w", readErr)
			}
			token = strings.TrimSpace(string(tokenBytes))
		}
		if token == "" {
			token = strings.TrimSpace(os.Getenv("VAULT_TOKEN"))
		}
		if token == "" {
			return nil, fmt.Errorf("vault.token or vault.token_file must be set for vault.auth_method=token")
		}
		client.SetToken(token)
	case "kubernetes":
		role := strings.TrimSpace(cfg.KubernetesRole)
		if role == "" {
			return nil, fmt.Errorf("vault.kubernetes_role must not be empty for vault.auth_method=kubernetes")
		}
		jwtFile := strings.TrimSpace(cfg.KubernetesJWTFile)
		if jwtFile == "" {
			return nil, fmt.Errorf("vault.kubernetes_jwt_file must not be empty for vault.auth_method=kubernetes")
		}
		jwtBytes, readErr := readFileWithinRoot(jwtFile)
		if readErr != nil {
			return nil, fmt.Errorf("read vault kubernetes jwt: %w", readErr)
		}
		jwt := strings.TrimSpace(string(jwtBytes))
		if jwt == "" {
			return nil, fmt.Errorf("vault kubernetes jwt file %q is empty", jwtFile)
		}

		mountPath := strings.Trim(strings.TrimSpace(cfg.KubernetesMountPath), "/")
		if mountPath == "" {
			mountPath = "kubernetes"
		}
		loginPath := fmt.Sprintf("auth/%s/login", mountPath)
		secret, writeErr := client.Logical().Write(loginPath, map[string]interface{}{
			"role": role,
			"jwt":  jwt,
		})
		if writeErr != nil {
			return nil, fmt.Errorf("vault kubernetes login: %w", writeErr)
		}
		if secret == nil || secret.Auth == nil || strings.TrimSpace(secret.Auth.ClientToken) == "" {
			return nil, fmt.Errorf("vault kubernetes login returned an empty client token")
		}
		client.SetToken(strings.TrimSpace(secret.Auth.ClientToken))
	default:
		return nil, fmt.Errorf("vault.auth_method must be one of kubernetes|token")
	}

	cache := map[string]map[string]interface{}{}
	return func(path, key string) (string, error) {
		secretData, ok := cache[path]
		if !ok {
			secret, readErr := client.Logical().Read(path)
			if readErr != nil {
				return "", fmt.Errorf("read vault path %q: %w", path, readErr)
			}
			if secret == nil || secret.Data == nil {
				return "", fmt.Errorf("vault path %q returned no data", path)
			}
			secretData = secret.Data
			if nestedRaw, nestedOK := secretData["data"]; nestedOK {
				if nestedMap, castOK := nestedRaw.(map[string]interface{}); castOK {
					secretData = nestedMap
				}
			}
			cache[path] = secretData
		}

		rawValue, found := secretData[key]
		if !found {
			return "", fmt.Errorf("vault key %q was not found at %q", key, path)
		}
		if rawValue == nil {
			return "", fmt.Errorf("vault key %q at %q is null", key, path)
		}
		return fmt.Sprint(rawValue), nil
	}, nil
}

func readFileWithinRoot(rawPath string) ([]byte, error) {
	path := filepath.Clean(strings.TrimSpace(rawPath))
	if path == "" || path == "." {
		return nil, fmt.Errorf("file path must not be empty")
	}
	if !filepath.IsAbs(path) {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve absolute path: %w", err)
		}
		path = absPath
	}

	rootPath := filepath.Dir(path)
	fileName := filepath.Base(path)
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open path root %q: %w", rootPath, err)
	}
	defer func() {
		_ = root.Close()
	}()

	file, err := root.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("open file %q: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	return io.ReadAll(file)
}
