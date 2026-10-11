package resultgen

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T, opts ...FileStoreOption) (*FileStore, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "result-generations")
	s, err := NewFileStore(dir, log.NewNop(), opts...)
	require.NoError(t, err)
	return s, dir
}

func floatPtr(f float64) *float64 { return &f }

// requireQuarantined asserts that dir contains a quarantined artifact for
// baseName (renamed to <baseName>.corrupt-<nanos>).
func requireQuarantined(t *testing.T, dir, baseName string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if matched, mErr := filepath.Match(baseName+".corrupt-*", e.Name()); mErr == nil && matched {
			return
		}
	}
	require.Failf(t, "no quarantined artifact found", "dir=%s base=%s", dir, baseName)
}

func sampleFreezeInput(taskID string) FreezeInput {
	return FreezeInput{
		HookName:     "hooks/test.sh",
		TaskID:       taskID,
		QueueName:    "main",
		BindingType:  "kubernetes",
		BindingHash:  "bindhash",
		Fingerprint:  "fp-1",
		Queued:       true,
		PatchBytes:   []byte(`[{"operation":"MergePatch","kind":"ConfigMap","namespace":"default","name":"cm","mergePatch":{"data":{"a":"b"}}}]`),
		PatchOpCount: 1,
		Metrics: []FrozenMetricOp{
			{Name: "m", Action: "set", Value: floatPtr(1), Labels: map[string]string{"hook": "x"}},
		},
	}
}

func Test_FileStore_FreezeRoundTrip(t *testing.T) {
	s, dir := newTestStore(t)

	rec, err := s.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)
	require.NotEmpty(t, rec.ID)
	require.Equal(t, Seq(1), rec.Seq)
	require.Equal(t, GenActive, rec.State)
	require.Equal(t, StagePending, rec.Stages[StageObjectPatches].State)
	require.Equal(t, StagePending, rec.Stages[StageValues].State)
	require.Equal(t, StagePending, rec.Stages[StageMetrics].State)
	require.Equal(t, StagePending, rec.Stages[StageQueue].State)
	require.Len(t, rec.Ops, 1)
	require.Equal(t, OpPending, rec.Ops[0].State)

	// Journal files exist and are readable before any commit stage runs.
	_, err = os.Stat(filepath.Join(dir, "gen", string(rec.ID)+".json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "gen", string(rec.ID)+".patch"))
	require.NoError(t, err)

	loaded, err := s.Get(rec.ID)
	require.NoError(t, err)
	require.Equal(t, rec.ID, loaded.ID)
	require.Equal(t, rec.PatchBytes, loaded.PatchBytes)
	require.Len(t, loaded.Metrics, 1)

	// Second freeze for another hook gets an independent seq.
	other := sampleFreezeInput("task-2")
	other.HookName = "hooks/other.sh"
	rec2, err := s.Freeze(other)
	require.NoError(t, err)
	require.Equal(t, Seq(1), rec2.Seq)
}

func Test_FileStore_InitialStages(t *testing.T) {
	s, _ := newTestStore(t)

	t.Run("no patches and no metrics are skipped, sync task skips queue", func(t *testing.T) {
		in := FreezeInput{HookName: "h", TaskID: "t", Queued: false}
		rec, err := s.Freeze(in)
		require.NoError(t, err)
		require.Equal(t, StageSkipped, rec.Stages[StageObjectPatches].State)
		require.Equal(t, StageSkipped, rec.Stages[StageMetrics].State)
		require.Equal(t, StageSkipped, rec.Stages[StageQueue].State)
		require.Equal(t, StagePending, rec.Stages[StageValues].State)
	})

	t.Run("parse error fails patches stage", func(t *testing.T) {
		in := FreezeInput{HookName: "h2", TaskID: "t", Queued: true, PatchBytes: []byte("{bad"), ParseError: "bad patch spec"}
		rec, err := s.Freeze(in)
		require.NoError(t, err)
		require.Equal(t, StageFailed, rec.Stages[StageObjectPatches].State)
		require.Equal(t, "bad patch spec", rec.Stages[StageObjectPatches].LastError)
	})
}

func Test_FileStore_SaveAndSucceed(t *testing.T) {
	s, dir := newTestStore(t)
	rec, err := s.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)

	rec.Ops[0].State = OpConfirmed
	rec.Stages[StageObjectPatches].State = StageCommitted
	require.NoError(t, s.Save(rec))

	loaded, err := s.Get(rec.ID)
	require.NoError(t, err)
	require.Equal(t, OpConfirmed, loaded.Ops[0].State)
	require.Equal(t, StageCommitted, loaded.Stages[StageObjectPatches].State)

	require.NoError(t, s.Succeed(rec.ID))
	_, err = s.Get(rec.ID)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(dir, "gen", string(rec.ID)+".json"))
	require.True(t, os.IsNotExist(err))
	require.Nil(t, s.ByTask("hooks/test.sh", "task-1"))
}

func Test_FileStore_DeadLetterAndGC(t *testing.T) {
	s, dir := newTestStore(t, WithDeadLetterLimit(3), WithClock(func() time.Time {
		// deterministic clock is unnecessary; real clock ordering is fine
		return time.Now()
	}))

	for i := 0; i < 5; i++ {
		in := sampleFreezeInput("task-x")
		in.Fingerprint = "fp"
		rec, err := s.Freeze(in)
		require.NoError(t, err)
		require.NoError(t, s.FailTerminal(rec, FailureRecord{
			GenerationID: rec.ID,
			Seq:          rec.Seq,
			HookName:     rec.HookName,
			TaskID:       rec.TaskID,
			Reason:       "Conflict",
			Message:      "boom",
			Object:       "v1/ConfigMap/default/cm",
		}))
	}

	failures, err := s.ListFailures()
	require.NoError(t, err)
	require.Len(t, failures, 3, "dead letters must be bounded")
	require.Equal(t, "Conflict", failures[0].Reason)
	require.Equal(t, "v1/ConfigMap/default/cm", failures[0].Object)
	require.NotEmpty(t, failures[0].FailedAt)

	entries, err := os.ReadDir(filepath.Join(dir, "dead"))
	require.NoError(t, err)
	require.Len(t, entries, 3)

	got, err := s.GetFailure(failures[0].GenerationID)
	require.NoError(t, err)
	require.Equal(t, failures[0].HookName, got.HookName)
}

func Test_FileStore_RecoverAndQuarantine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")

	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	good, err := s1.Freeze(sampleFreezeInput("task-good"))
	require.NoError(t, err)
	bad, err := s1.Freeze(sampleFreezeInput("task-bad"))
	require.NoError(t, err)

	// Corrupt one generation record file (simulated torn write).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "gen", string(bad.ID)+".json"), []byte("{partial"), 0o600))
	// Add a dead letter to verify it survives restart.
	require.NoError(t, s1.FailTerminal(good, FailureRecord{GenerationID: good.ID, HookName: good.HookName, Reason: "Conflict"}))

	// Restart: new store instance on the same directory.
	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	active, failures, err := s2.Recover()
	require.NoError(t, err)
	require.Len(t, active, 0, "good generation was dead-lettered; bad one was quarantined")
	require.Len(t, failures, 1)

	// Recreate scenario with one healthy active generation.
	s3dir := filepath.Join(t.TempDir(), "result-generations")
	s3, err := NewFileStore(s3dir, log.NewNop())
	require.NoError(t, err)
	alive, err := s3.Freeze(sampleFreezeInput("task-alive"))
	require.NoError(t, err)
	corrupt, err := s3.Freeze(sampleFreezeInput("task-corrupt"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(s3dir, "gen", string(corrupt.ID)+".json"), []byte("garbage"), 0o600))

	s4, err := NewFileStore(s3dir, log.NewNop())
	require.NoError(t, err)
	active2, _, err := s4.Recover()
	require.NoError(t, err)
	require.Len(t, active2, 1)
	require.Equal(t, alive.ID, active2[0].ID)
	require.Equal(t, alive.PatchBytes, active2[0].PatchBytes)

	// Corrupt file was quarantined, not fatal.
	requireQuarantined(t, filepath.Join(s3dir, "gen"), string(corrupt.ID)+".json")

	// Recovery via fingerprint works on the restarted store: the active
	// generation was flagged orphaned during Recover.
	got := s4.ResumeableByFingerprint("hooks/test.sh", "fp-1")
	require.NotNil(t, got)
	require.Equal(t, alive.ID, got.ID)
	require.Equal(t, Seq(2), s4.LatestSeq("hooks/test.sh"))
}

func Test_FileStore_CorruptIndexRebuildsFromRecordFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")

	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	dead, err := s1.Freeze(sampleFreezeInput("task-dead"))
	require.NoError(t, err)
	require.NoError(t, s1.FailTerminal(dead, FailureRecord{GenerationID: dead.ID, HookName: dead.HookName, Reason: "Conflict"}))
	alive, err := s1.Freeze(sampleFreezeInput("task-alive"))
	require.NoError(t, err)

	// Truncate the index: NewFileStore must not fail and Recover must rebuild
	// both the active generation and the dead-letter list from files.
	require.NoError(t, os.WriteFile(filepath.Join(dir, indexFileName), []byte("{not-json"), 0o600))

	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err, "corrupt index must not disable the journal")
	active, failures, err := s2.Recover()
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, alive.ID, active[0].ID)
	require.Len(t, failures, 1)
	require.Equal(t, dead.ID, failures[0].GenerationID)

	// Quarantined index kept for inspection; rebuilt index persisted.
	requireQuarantined(t, dir, indexFileName)
	listed, err := s2.ListFailures()
	require.NoError(t, err)
	require.Len(t, listed, 1)

	got := s2.ResumeableByFingerprint("hooks/test.sh", "fp-1")
	require.NotNil(t, got)
	require.Equal(t, alive.ID, got.ID)
}

func Test_FileStore_OrphanDeadLetterReindexedAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")
	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	rec, err := s1.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)
	require.NoError(t, s1.FailTerminal(rec, FailureRecord{GenerationID: rec.ID, HookName: rec.HookName, Reason: "Conflict"}))

	// Simulate losing the index after the dead file was durable.
	require.NoError(t, os.Remove(filepath.Join(dir, indexFileName)))

	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	_, failures, err := s2.Recover()
	require.NoError(t, err)
	require.Len(t, failures, 1)
	listed, err := s2.ListFailures()
	require.NoError(t, err)
	require.Len(t, listed, 1, "crash-window dead letter must stay queryable")
}

func Test_FileStore_GCPrunedIndexPersistsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")
	s1, err := NewFileStore(dir, log.NewNop(), WithDeadLetterLimit(2))
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		rec, err := s1.Freeze(sampleFreezeInput("task-x"))
		require.NoError(t, err)
		require.NoError(t, s1.FailTerminal(rec, FailureRecord{GenerationID: rec.ID, HookName: rec.HookName, Reason: "Conflict"}))
	}

	// A fresh process reads the pruned index without re-running Recover: the
	// pruned state must already be durable.
	s2, err := NewFileStore(dir, log.NewNop(), WithDeadLetterLimit(2))
	require.NoError(t, err)
	listed, err := s2.ListFailures()
	require.NoError(t, err)
	require.Len(t, listed, 2)
}

func Test_FileStore_RebindTaskRewritesRecordFile(t *testing.T) {
	s, _ := newTestStore(t)
	rec, err := s.Freeze(sampleFreezeInput("old-task"))
	require.NoError(t, err)

	require.NoError(t, s.RebindTask(rec.ID, "new-task"))

	// The record file itself carries the new task ID, so a crash right after
	// the rebind cannot leave an index/record mismatch.
	loaded, err := s.Get(rec.ID)
	require.NoError(t, err)
	require.Equal(t, "new-task", loaded.TaskID)
}

func Test_FileStore_MissingPatchFileQuarantined(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")
	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	rec, err := s1.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)
	// The frozen patch file is gone: resuming could never confirm the op.
	require.NoError(t, os.Remove(filepath.Join(dir, genDirName, string(rec.ID)+".patch")))

	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	active, _, err := s2.Recover()
	require.NoError(t, err)
	require.Empty(t, active)
	requireQuarantined(t, filepath.Join(dir, genDirName), string(rec.ID)+".json")
}

func Test_FileStore_SupersedeAndFenceData(t *testing.T) {
	s, _ := newTestStore(t)
	g1, err := s.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)
	in2 := sampleFreezeInput("task-2")
	in2.Fingerprint = "fp-2"
	g2, err := s.Freeze(in2)
	require.NoError(t, err)
	require.Equal(t, Seq(2), g2.Seq)
	require.Equal(t, Seq(2), s.LatestSeq("hooks/test.sh"))

	require.NoError(t, s.MarkSuperseded(g1, "newer generation fp-2"))
	_, err = s.Get(g1.ID)
	require.Error(t, err)
	require.NotNil(t, s.ByTask("hooks/test.sh", "task-2"))
}

// withIndexSaveFaultAtCall makes the Nth index write fail while leaving the
// on-disk index untouched (emulating a failed atomic rename); later writes
// succeed.
func withIndexSaveFaultAtCall(failAt int) FileStoreOption {
	return func(s *FileStore) {
		var calls int
		s.indexSaveFault = func() error {
			calls++
			if calls == failAt {
				return errTemp("simulated journal index write failure")
			}
			return nil
		}
	}
}

// Test_FileStore_TerminalTransitionsIdempotentAfterIndexFault covers the
// crash window of the final index write: Succeed/FailTerminal/MarkSuperseded
// must be replayable without losing the generation association or
// duplicating failure records.
func Test_FileStore_TerminalTransitionsIdempotentAfterIndexFault(t *testing.T) {
	t.Run("Succeed retries without losing the generation", func(t *testing.T) {
		s, _ := newTestStore(t, withIndexSaveFaultAtCall(2)) // 1=Freeze, 2=Succeed
		rec, err := s.Freeze(sampleFreezeInput("task-1"))
		require.NoError(t, err)

		require.Error(t, s.Succeed(rec.ID), "first finalization fails at the index write")
		// Memory was rolled back: the task still finds its generation and the
		// record file already carries the durable terminal verdict.
		require.NotNil(t, s.ByTask("hooks/test.sh", "task-1"))
		loaded, err := s.Get(rec.ID)
		require.NoError(t, err)
		require.Equal(t, GenSucceeded, loaded.State)

		require.NoError(t, s.Succeed(rec.ID), "replay completes the transition")
		require.Nil(t, s.ByTask("hooks/test.sh", "task-1"))
		_, err = s.Get(rec.ID)
		require.Error(t, err)
	})

	t.Run("FailTerminal retries with a single failure record", func(t *testing.T) {
		s, _ := newTestStore(t, withIndexSaveFaultAtCall(2))
		rec, err := s.Freeze(sampleFreezeInput("task-1"))
		require.NoError(t, err)
		failure := FailureRecord{GenerationID: rec.ID, HookName: rec.HookName, Reason: "Conflict", Message: "boom"}

		require.Error(t, s.FailTerminal(rec, failure))
		require.NotNil(t, s.ByTask("hooks/test.sh", "task-1"), "active ref survives the failed finalization")
		// The dead-letter evidence is already durable and readable.
		got, err := s.GetFailure(rec.ID)
		require.NoError(t, err)
		require.Equal(t, "boom", got.Message)

		require.NoError(t, s.FailTerminal(rec, failure), "replay completes the transition")
		failures, err := s.ListFailures()
		require.NoError(t, err)
		require.Len(t, failures, 1, "the failure must be registered exactly once")
		require.Nil(t, s.ByTask("hooks/test.sh", "task-1"))
		_, err = s.Get(rec.ID)
		require.Error(t, err)
	})

	t.Run("MarkSuperseded retries and retires the generation", func(t *testing.T) {
		s, _ := newTestStore(t, withIndexSaveFaultAtCall(2))
		rec, err := s.Freeze(sampleFreezeInput("task-1"))
		require.NoError(t, err)

		require.Error(t, s.MarkSuperseded(rec, "newer generation"))
		require.NotNil(t, s.ByTask("hooks/test.sh", "task-1"))
		loaded, err := s.Get(rec.ID)
		require.NoError(t, err)
		require.Equal(t, GenSuperseded, loaded.State)

		require.NoError(t, s.MarkSuperseded(rec, "newer generation"))
		require.Nil(t, s.ByTask("hooks/test.sh", "task-1"))
		_, err = s.Get(rec.ID)
		require.Error(t, err)
	})
}

// Test_FileStore_RecoverArchivesTerminalRecords verifies that generation
// records carrying a terminal state (a crash between the terminal marker and
// index drop) are archived during recovery and never revived as active
// orphans; failed-terminal markers keep their queryable dead letter.
func Test_FileStore_RecoverArchivesTerminalRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")
	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	succeeded, err := s1.Freeze(sampleFreezeInput("task-ok"))
	require.NoError(t, err)
	dead, err := s1.Freeze(sampleFreezeInput("task-dead"))
	require.NoError(t, err)

	// Emulate an interrupted terminal transition: marker written, index not
	// updated, files not removed.
	markTerminal := func(id GenerationID, state GenState, failure *FailureRecord) {
		s1.mu.Lock()
		defer s1.mu.Unlock()
		rec, err := s1.loadRecordLocked(id)
		require.NoError(t, err)
		rec.State = state
		rec.Failure = failure
		require.NoError(t, s1.writeRecordFilesLocked(rec))
	}
	markTerminal(succeeded.ID, GenSucceeded, nil)
	markTerminal(dead.ID, GenFailedTerminal, &FailureRecord{
		GenerationID: dead.ID, Seq: dead.Seq, HookName: dead.HookName, TaskID: dead.TaskID,
		Reason: "Conflict", Message: "left by interrupted transition",
	})

	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	active, failures, err := s2.Recover()
	require.NoError(t, err)
	require.Empty(t, active, "finalized generations are never revived")
	require.Len(t, failures, 1)
	require.Equal(t, dead.ID, failures[0].GenerationID)
	require.Equal(t, "left by interrupted transition", failures[0].Message)

	// Both generation file sets were archived.
	entries, err := os.ReadDir(filepath.Join(dir, genDirName))
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Nil(t, s2.ByTask("hooks/test.sh", "task-ok"))
	require.Nil(t, s2.ByTask("hooks/test.sh", "task-dead"))
}

// Test_FileStore_RecoverDeadSiblingBeatsActiveRecord covers the triple-fault
// window: the dead-letter file was committed, the terminal marker rename was
// lost (record still Active), and the index is gone. The dead sibling is
// authoritative: the generation must be archived, not revived as an orphan
// that could replay a terminally-conflicted operation.
func Test_FileStore_RecoverDeadSiblingBeatsActiveRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "result-generations")
	s1, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	rec, err := s1.Freeze(sampleFreezeInput("task-1"))
	require.NoError(t, err)

	// Dead file durable; record keeps reading Active; index removed.
	require.NoError(t, atomicWriteJSON(s1.deadPath(rec.ID), FailureRecord{
		GenerationID: rec.ID, Seq: rec.Seq, HookName: rec.HookName,
		TaskID: rec.TaskID, Reason: "Conflict", Message: "marker lost",
	}))
	require.NoError(t, os.Remove(filepath.Join(dir, indexFileName)))

	s2, err := NewFileStore(dir, log.NewNop())
	require.NoError(t, err)
	active, failures, err := s2.Recover()
	require.NoError(t, err)
	require.Empty(t, active, "a dead-lettered generation is never revived")
	require.Len(t, failures, 1)
	require.Equal(t, rec.ID, failures[0].GenerationID)
	require.Nil(t, s2.ResumeableByFingerprint("hooks/test.sh", "fp-1"))

	entries, err := os.ReadDir(filepath.Join(dir, genDirName))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func Test_FileStore_ConcurrentFreeze(t *testing.T) {
	s, _ := newTestStore(t)

	const n = 20
	var wg sync.WaitGroup
	ids := make(chan GenerationID, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := sampleFreezeInput("task-conc")
			in.TaskID = "task-conc"
			in.Fingerprint = "fp"
			rec, err := s.Freeze(in)
			require.NoError(t, err)
			rec.Stages[StageValues].State = StageCommitted
			require.NoError(t, s.Save(rec))
			ids <- rec.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	seen := map[GenerationID]Seq{}
	for id := range ids {
		_, dup := seen[id]
		require.False(t, dup)
		seen[id] = 0
	}
	require.Len(t, seen, n)
	require.Equal(t, Seq(n), s.LatestSeq("hooks/test.sh"))
}
