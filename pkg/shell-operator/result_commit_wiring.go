// Copyright 2025 Flant JSC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shell_operator

import (
	"path/filepath"
	"sync"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/flant/shell-operator/pkg/hook/resultgen"
	objectpatch "github.com/flant/shell-operator/pkg/kube/object_patch"
)

func (op *ShellOperator) newPatchVerifier() *objectpatch.OperationVerifier {
	return objectpatch.NewOperationVerifier(op.ObjectPatcher)
}

// resultGenerationsDirName is the journal subdirectory under TMP_DIR.
const resultGenerationsDirName = "result-generations"

// valueCommitterHolder is an indirection so extenders (addon-operator) can
// register a values committer before or after the coordinator is built.
type valueCommitterHolder struct {
	mu    sync.RWMutex
	inner resultgen.ValueCommitter
}

func (h *valueCommitterHolder) Set(v resultgen.ValueCommitter) {
	h.mu.Lock()
	h.inner = v
	h.mu.Unlock()
}

// get returns the currently registered extender committer, if any.
func (h *valueCommitterHolder) get() resultgen.ValueCommitter {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.inner
}

// SetupResultCommitter builds the durable result-generation coordinator in
// tempDir. It is called during hook-manager setup; extenders assembling the
// operator manually may also call it explicitly with their state directory.
func (op *ShellOperator) SetupResultCommitter(tempDir string) error {
	if op.ObjectPatcher == nil || op.HookMetricStorage == nil {
		return nil
	}

	journalsDir := filepath.Join(tempDir, resultGenerationsDirName)
	store, err := resultgen.NewFileStore(journalsDir, op.logger.Named("result-generations"))
	if err != nil {
		return err
	}

	registry := resultgen.NewRegistry(store, op.logger.Named("result-generations"))
	active, failures, err := registry.Recover()
	if err != nil {
		op.logger.Warn("recover result generations finished with error", log.Err(err))
	}
	if len(active) > 0 || len(failures) > 0 {
		op.logger.Info("recovered result-generation journal",
			"activeGenerations", len(active), "deadLetters", len(failures))
	}

	verifier := op.newPatchVerifier()
	coordinator := resultgen.NewCoordinator(
		registry,
		op.ObjectPatcher,
		verifier,
		op.HookMetricStorage,
		op.logger.Named("result-commit"),
	)
	// No values committer is registered by shell-operator itself: the values
	// stage stays Skipped. An extender committer registered before setup is
	// forwarded; later registrations go through SetValueCommitter.
	if v := op.valueCommitters.get(); v != nil {
		coordinator.SetValueCommitter(v)
	}
	op.ResultCommitter = coordinator
	return nil
}

// RegisterValueCommitter registers the configuration-values stage used by
// extenders built on top of shell-operator. shell-operator itself registers
// none: the values stage then stays Skipped.
func (op *ShellOperator) RegisterValueCommitter(v resultgen.ValueCommitter) {
	op.valueCommitters.Set(v)
	if op.ResultCommitter != nil {
		op.ResultCommitter.SetValueCommitter(v)
	}
}
