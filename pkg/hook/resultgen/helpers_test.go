package resultgen

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
)

func testLogger() *log.Logger { return log.NewNop() }

func errTemp(msg string) error { return errors.New(msg) }

// filepathJoinTemp returns a fresh (not yet created) journal directory under
// the test temp dir, to simulate a process restart by re-opening it.
func filepathJoinTemp(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "result-generations")
}
