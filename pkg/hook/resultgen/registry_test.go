package resultgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	s, dir := newTestStore(t)
	return NewRegistry(s, testLogger()), dir
}

func Test_Registry_TaskIDRetry(t *testing.T) {
	r, _ := newTestRegistry(t)
	in := sampleFreezeInput("task-1")

	g1, resumed, err := r.BeginOrResume(in)
	require.NoError(t, err)
	require.False(t, resumed)
	require.Equal(t, Seq(1), g1.Seq)

	// Same task retried in-process: exact correlation, no new generation.
	g1Again, resumed, err := r.BeginOrResume(in)
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, g1.ID, g1Again.ID)
	require.Equal(t, Seq(1), g1Again.Seq)
}

func Test_Registry_RestartSameFingerprintResumes(t *testing.T) {
	dir := filepathJoinTemp(t)

	// Previous process: freeze G1.
	s1, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	r1 := NewRegistry(s1, testLogger())
	g1, _, err := r1.BeginOrResume(sampleFreezeInput("old-task"))
	require.NoError(t, err)

	// New process: Recover flags G1 orphaned; same frozen result resumes it.
	s2, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	r2 := NewRegistry(s2, testLogger())
	active, _, err := r2.Recover()
	require.NoError(t, err)
	require.Len(t, active, 1)

	in := sampleFreezeInput("new-task") // new task, same fingerprint "fp-1"
	got, resumed, err := r2.BeginOrResume(in)
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, g1.ID, got.ID)
	require.Equal(t, "new-task", got.TaskID)
	require.Equal(t, Seq(1), got.Seq)

	// Retry of the new task now correlates by task ID.
	got2, resumed, err := r2.BeginOrResume(in)
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, g1.ID, got2.ID)
}

func Test_Registry_RestartDifferentFingerprintSupersedes(t *testing.T) {
	dir := filepathJoinTemp(t)

	s1, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	r1 := NewRegistry(s1, testLogger())
	g1, _, err := r1.BeginOrResume(sampleFreezeInput("old-task"))
	require.NoError(t, err)

	s2, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	r2 := NewRegistry(s2, testLogger())
	_, _, err = r2.Recover()
	require.NoError(t, err)

	in := sampleFreezeInput("new-task")
	in.Fingerprint = "fp-different"
	g2, resumed, err := r2.BeginOrResume(in)
	require.NoError(t, err)
	require.False(t, resumed)
	require.Equal(t, Seq(2), g2.Seq)

	// G1 was retired: its journal is gone and no active generation references it.
	_, err = s2.Get(g1.ID)
	require.Error(t, err)
	require.Nil(t, s2.ByTask("hooks/test.sh", "old-task"))
	require.Nil(t, s2.ResumeableByFingerprint("hooks/test.sh", "fp-1"))
}

func Test_Registry_LiveGenNotMatchedByFingerprint(t *testing.T) {
	r, _ := newTestRegistry(t)

	// G1 is bound to a live task (not orphaned).
	g1, _, err := r.BeginOrResume(sampleFreezeInput("task-1"))
	require.NoError(t, err)

	// A different task with the identical fingerprint must NOT steal G1;
	// it gets a fresh generation.
	other := sampleFreezeInput("task-2")
	g2, resumed, err := r.BeginOrResume(other)
	require.NoError(t, err)
	require.False(t, resumed)
	require.NotEqual(t, g1.ID, g2.ID)
	require.Equal(t, Seq(2), g2.Seq)
}

func Test_Registry_Fence(t *testing.T) {
	r, _ := newTestRegistry(t)

	g1, _, err := r.BeginOrResume(sampleFreezeInput("task-1"))
	require.NoError(t, err)
	require.True(t, r.CheckFence(g1))

	other := sampleFreezeInput("task-2")
	other.Fingerprint = "fp-2"
	g2, _, err := r.BeginOrResume(other)
	require.NoError(t, err)

	// G1 is now stale; G2 is current.
	require.False(t, r.CheckFence(g1))
	require.True(t, r.CheckFence(g2))

	// Superseding G1 clears it out; G2 commits normally.
	require.NoError(t, r.Supersede(g1, "newer generation frozen"))
	require.NoError(t, r.Succeed(g2))
	failures, err := r.ListFailures()
	require.NoError(t, err)
	require.Empty(t, failures)
}
