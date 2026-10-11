package resultgen

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"sigs.k8s.io/yaml"
)

// HashCanonical returns a stable SHA-256 hex digest of a JSON- or YAML-encoded
// document: formatting and map key order do not affect the result. Documents
// that fail to parse are hashed verbatim so a distinct fingerprint is still
// produced (the coordinator rejects invalid patch specs separately).
func HashCanonical(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}

	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return hashBytes(raw)
		}
	}
	canonical, err := json.Marshal(doc)
	if err != nil {
		return hashBytes(raw)
	}
	return hashBytes(canonical)
}

// ComputeFingerprint builds the resume-correlation fingerprint of a frozen
// hook result. It is identical across retries of the same hook execution with
// the same parsed outputs.
func ComputeFingerprint(hookName, bindingHash string, patchBytes []byte, metrics []FrozenMetricOp) string {
	h := sha256.New()
	fmt.Fprintf(h, "hook=%s\n", hookName)
	fmt.Fprintf(h, "binding=%s\n", bindingHash)
	fmt.Fprintf(h, "patch=%s\n", HashCanonical(patchBytes))
	if len(metrics) > 0 {
		canonical, err := json.Marshal(metrics)
		if err != nil {
			canonical = []byte("unserializable")
		}
		fmt.Fprintf(h, "metrics=%s\n", hashBytes(canonical))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// HashAny returns a stable digest of arbitrary JSON-marshalable values (e.g.
// binding context lists).
func HashAny(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return hashBytes(raw)
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
