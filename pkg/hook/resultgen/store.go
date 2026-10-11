package resultgen

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	uuid "github.com/gofrs/uuid/v5"
)

const (
	indexFileName = "index.json"
	genDirName    = "gen"
	deadDirName   = "dead"
	// defaultDeadLetterLimit bounds how many terminal failure records survive.
	defaultDeadLetterLimit = 100
)

// Store is the durable generation journal abstraction used by the registry
// and the coordinator.
type Store interface {
	// Freeze allocates ID and per-hook seq and durably persists a new active
	// generation before any commit stage runs.
	Freeze(in FreezeInput) (*GenerationRecord, error)
	// Get loads one generation record (including its raw patch bytes).
	Get(id GenerationID) (*GenerationRecord, error)
	// Save durably rewrites an active record.
	Save(rec *GenerationRecord) error
	// Succeed marks the generation finished and removes its journal files.
	Succeed(id GenerationID) error
	// FailTerminal stores the queryable dead-letter record and removes the
	// generation from the active set.
	FailTerminal(rec *GenerationRecord, failure FailureRecord) error
	// MarkSuperseded retires a generation replaced by a newer one.
	MarkSuperseded(rec *GenerationRecord, reason string) error
	// Recover loads all active generations and dead-letter records from disk,
	// quarantining unreadable entries.
	Recover() (active []*GenerationRecord, failures []*FailureRecord, err error)
	// ListFailures returns retained dead-letter records, newest first.
	ListFailures() ([]*FailureRecord, error)
	// GetFailure loads one dead-letter record.
	GetFailure(id GenerationID) (*FailureRecord, error)
	// ByTask returns the active generation bound to a task ID, if any.
	ByTask(hookName, taskID string) *GenerationRecord
	// ResumeableByFingerprint returns a restart-orphaned active generation
	// whose frozen result fingerprint matches. Non-orphaned (live-task)
	// generations are never matched this way.
	ResumeableByFingerprint(hookName, fingerprint string) *GenerationRecord
	// LatestSeq returns the highest seq allocated for a hook (0 if unknown).
	LatestSeq(hookName string) Seq
	// RebindTask attaches a restart-resumed orphan generation to a new task.
	RebindTask(id GenerationID, taskID string) error
	// MarkOrphaned flags an active generation whose owner task is gone (a
	// synchronous webhook execution that failed or was canceled) so a later
	// identical retry can resume it by fingerprint and a newer generation can
	// retire it.
	MarkOrphaned(id GenerationID) error
	// RetireOrphans removes restart-orphaned active generations of a hook and
	// returns their IDs (they are superseded by a fresh generation).
	RetireOrphans(hookName string) ([]GenerationID, error)
}

type storedActiveRef struct {
	ID          GenerationID `json:"id"`
	Seq         Seq          `json:"seq"`
	TaskID      string       `json:"taskId"`
	Fingerprint string       `json:"fingerprint"`
	// Orphaned marks generations found during a process restart: their owner
	// task no longer exists in memory. They may be resumed by fingerprint or
	// retired when a newer generation appears.
	Orphaned bool `json:"orphaned,omitempty"`
}

type storedHookIndex struct {
	NextSeq Seq                `json:"nextSeq"`
	Active  []*storedActiveRef `json:"active"`
}

type storedIndex struct {
	Hooks    map[string]*storedHookIndex `json:"hooks"`
	Failures []string                    `json:"failures"`
}

// FileStoreOption customizes a FileStore.
type FileStoreOption func(*FileStore)

// WithDeadLetterLimit overrides the retained dead-letter record count.
func WithDeadLetterLimit(n int) FileStoreOption {
	return func(s *FileStore) {
		if n > 0 {
			s.deadLetterLimit = n
		}
	}
}

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) FileStoreOption {
	return func(s *FileStore) {
		if now != nil {
			s.now = now
		}
	}
}

// FileStore persists generations as atomic JSON files under a working
// directory. All read-modify-write sequences are serialized by an in-process
// mutex; atomic rename prevents torn reads. The directory is not expected to
// be shared by two running operator processes (same as TMP_DIR today).
type FileStore struct {
	dir string

	mu    sync.Mutex
	index *storedIndex

	logger          *log.Logger
	now             func() time.Time
	deadLetterLimit int

	// indexSaveFault is a test hook: when non-nil, a returned error aborts the
	// index write BEFORE touching the on-disk file, emulating a failed atomic
	// rename (old index stays intact). Nil in production.
	indexSaveFault func() error
}

// NewFileStore prepares the journal directory and recovers the in-memory index
// from disk.
func NewFileStore(dir string, logger *log.Logger, opts ...FileStoreOption) (*FileStore, error) {
	s := &FileStore{
		dir:             dir,
		index:           &storedIndex{Hooks: map[string]*storedHookIndex{}},
		logger:          logger,
		now:             time.Now,
		deadLetterLimit: defaultDeadLetterLimit,
	}
	for _, opt := range opts {
		opt(s)
	}

	if err := os.MkdirAll(filepath.Join(s.dir, genDirName), 0o700); err != nil {
		return nil, fmt.Errorf("create generations dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(s.dir, deadDirName), 0o700); err != nil {
		return nil, fmt.Errorf("create dead-letter dir: %w", err)
	}

	if err := s.loadIndex(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *FileStore) genPath(id GenerationID) string {
	return filepath.Join(s.dir, genDirName, string(id)+".json")
}

func (s *FileStore) patchPath(id GenerationID) string {
	return filepath.Join(s.dir, genDirName, string(id)+".patch")
}

func (s *FileStore) deadPath(id GenerationID) string {
	return filepath.Join(s.dir, deadDirName, string(id)+".json")
}

func (s *FileStore) loadIndex() error {
	raw, err := os.ReadFile(filepath.Join(s.dir, indexFileName))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read journal index: %w", err)
	}
	idx := &storedIndex{Hooks: map[string]*storedHookIndex{}}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, idx); err != nil {
			// A torn/corrupt index must not disable the journal: quarantine
			// the file and start with an empty index; Recover rebuilds it by
			// scanning the gen/ and dead/ directories.
			s.logger.Warn("journal index is corrupt; quarantining and rebuilding from record files",
				"error", err.Error())
			s.quarantineLocked(filepath.Join(s.dir, indexFileName))
			s.index = &storedIndex{Hooks: map[string]*storedHookIndex{}}
			return nil
		}
	}
	if idx.Hooks == nil {
		idx.Hooks = map[string]*storedHookIndex{}
	}
	s.index = idx
	return nil
}

func (s *FileStore) saveIndexLocked() error {
	if s.indexSaveFault != nil {
		if err := s.indexSaveFault(); err != nil {
			return err
		}
	}
	return atomicWriteJSON(filepath.Join(s.dir, indexFileName), s.index)
}

// commitIndexProjectionLocked applies mutate to an isolated copy of the
// in-memory index, persists the projection, and replaces the live index only
// after the write succeeds. A failed write leaves BOTH the live in-memory
// index and the on-disk file at their previous state (atomic rename keeps the
// old file intact), so terminal transitions never lose the active generation
// association even when the failing write is immediately followed by another
// disk error. Caller must hold s.mu.
func (s *FileStore) commitIndexProjectionLocked(mutate func(*storedIndex)) error {
	projection := &storedIndex{Hooks: make(map[string]*storedHookIndex, len(s.index.Hooks))}
	for name, hi := range s.index.Hooks {
		projection.Hooks[name] = &storedHookIndex{
			NextSeq: hi.NextSeq,
			Active:  append([]*storedActiveRef(nil), hi.Active...),
		}
	}
	projection.Failures = append([]string(nil), s.index.Failures...)
	mutate(projection)

	previous := s.index
	s.index = projection
	if err := s.saveIndexLocked(); err != nil {
		s.index = previous
		return err
	}
	return nil
}

// removeActiveRef drops the generation ref from every hook index (used by
// terminal index projections).
func (idx *storedIndex) removeActiveRef(id GenerationID) {
	for _, hi := range idx.Hooks {
		filtered := hi.Active[:0]
		for _, ref := range hi.Active {
			if ref.ID != id {
				filtered = append(filtered, ref)
			}
		}
		hi.Active = filtered
	}
}

// markRecordTerminalLocked durably writes the terminal state into the
// generation record file while the active index ref still exists. It is a
// no-op when the record is already finalized (idempotent replay) or already
// archived.
func (s *FileStore) markRecordTerminalLocked(id GenerationID, state GenState, failure *FailureRecord) error {
	rec, err := s.loadRecordLocked(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if rec.State != GenActive {
		return nil
	}
	rec.State = state
	if failure != nil {
		rec.Failure = failure
	}
	now := s.now()
	rec.FinishedAt = &now
	rec.UpdatedAt = now
	return s.writeRecordFilesLocked(rec)
}

// removeGenFilesLocked archives the record and patch files (best effort;
// Recover also sweeps leftovers).
func (s *FileStore) removeGenFilesLocked(id GenerationID) {
	_ = os.Remove(s.genPath(id))
	_ = os.Remove(s.patchPath(id))
}

// archiveTerminalRecordLocked completes an interrupted terminal transition
// found during recovery: dead-letter markers are kept queryable and the
// generation files are removed.
func (s *FileStore) archiveTerminalRecordLocked(rec *GenerationRecord) {
	if rec.State == GenFailedTerminal && rec.Failure != nil {
		fr := *rec.Failure
		if fr.FailedAt.IsZero() {
			fr.FailedAt = s.now()
		}
		if err := atomicWriteJSON(s.deadPath(rec.ID), fr); err != nil {
			s.logger.Warn("failed to restore dead-letter record while archiving finalized generation",
				"generation", string(rec.ID), "error", err.Error())
		}
	}
	s.removeGenFilesLocked(rec.ID)
}

func (s *FileStore) hookIndexLocked(name string) *storedHookIndex {
	hi := s.index.Hooks[name]
	if hi == nil {
		hi = &storedHookIndex{NextSeq: 1}
		s.index.Hooks[name] = hi
	}
	return hi
}

// Freeze implements Store.
func (s *FileStore) Freeze(in FreezeInput) (*GenerationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	hi := s.hookIndexLocked(in.HookName)
	seq := hi.NextSeq
	hi.NextSeq++

	id := GenerationID(uuid.Must(uuid.NewV4()).String())

	rec := &GenerationRecord{
		ID:                     id,
		Seq:                    seq,
		State:                  GenActive,
		HookName:               in.HookName,
		TaskID:                 in.TaskID,
		QueueName:              in.QueueName,
		BindingType:            in.BindingType,
		BindingContextHash:     in.BindingHash,
		Fingerprint:            in.Fingerprint,
		FrozenAt:               now,
		UpdatedAt:              now,
		PatchBytes:             in.PatchBytes,
		Metrics:                in.Metrics,
		AdmissionResponseJSON:  in.AdmissionResponseJSON,
		ConversionResponseJSON: in.ConversionResponseJSON,
		Stages:                 initialStages(in, now),
		Ops:                    make([]*OpCommit, 0, in.PatchOpCount),
	}
	for i := 0; i < in.PatchOpCount; i++ {
		rec.Ops = append(rec.Ops, &OpCommit{Index: i, State: OpPending, UpdatedAt: now})
	}

	if err := s.writeRecordFilesLocked(rec); err != nil {
		return nil, err
	}

	hi.Active = append(hi.Active, &storedActiveRef{
		ID:          id,
		Seq:         seq,
		TaskID:      in.TaskID,
		Fingerprint: in.Fingerprint,
	})
	if err := s.saveIndexLocked(); err != nil {
		return nil, err
	}

	return rec, nil
}

func initialStages(in FreezeInput, now time.Time) map[StageName]*StageCommit {
	stages := map[StageName]*StageCommit{}
	for _, name := range OrderedStages {
		st := &StageCommit{State: StagePending, UpdatedAt: now}
		switch name {
		case StageQueue:
			if !in.Queued {
				st.State = StageSkipped
			}
		case StageObjectPatches:
			if in.ParseError != "" {
				st.State = StageFailed
				st.LastError = in.ParseError
			}
			if len(in.PatchBytes) == 0 {
				st.State = StageSkipped
			}
		case StageMetrics:
			if len(in.Metrics) == 0 {
				st.State = StageSkipped
			}
		}
		stages[name] = st
	}
	return stages
}

// Get implements Store.
func (s *FileStore) Get(id GenerationID) (*GenerationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadRecordLocked(id)
}

func (s *FileStore) loadRecordLocked(id GenerationID) (*GenerationRecord, error) {
	raw, err := os.ReadFile(s.genPath(id))
	if err != nil {
		return nil, fmt.Errorf("read generation %s: %w", id, err)
	}
	rec := &GenerationRecord{}
	if err := json.Unmarshal(raw, rec); err != nil {
		return nil, fmt.Errorf("parse generation %s: %w", id, err)
	}
	if patchRaw, err := os.ReadFile(s.patchPath(id)); err == nil {
		rec.PatchBytes = patchRaw
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read generation patch %s: %w", id, err)
	}
	return rec, nil
}

// Save implements Store.
func (s *FileStore) Save(rec *GenerationRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.UpdatedAt = s.now()
	return s.writeRecordFilesLocked(rec)
}

func (s *FileStore) writeRecordFilesLocked(rec *GenerationRecord) error {
	if err := atomicWriteJSON(s.genPath(rec.ID), rec); err != nil {
		return err
	}
	if len(rec.PatchBytes) > 0 {
		if err := atomicWriteFile(s.patchPath(rec.ID), rec.PatchBytes, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// Succeed implements Store. The transition is ordered crash-safely: the
// terminal verdict is first made durable in the record file, the index is
// persisted next, and record files are removed only afterwards. The method is
// idempotent: a retry after an index-write failure completes the same
// transition instead of letting the caller freeze a new generation.
func (s *FileStore) Succeed(id GenerationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.markRecordTerminalLocked(id, GenSucceeded, nil); err != nil {
		return err
	}
	if err := s.commitIndexProjectionLocked(func(idx *storedIndex) {
		idx.removeActiveRef(id)
	}); err != nil {
		return err
	}
	s.removeGenFilesLocked(id)
	return nil
}

// FailTerminal implements Store. Same crash-safe ordering as Succeed: the
// dead-letter record and the terminal marker are durable before the active
// ref is dropped; an index-write failure leaves the live index untouched so
// the same generation is completed (not recreated) on retry.
func (s *FileStore) FailTerminal(rec *GenerationRecord, failure FailureRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if failure.FailedAt.IsZero() {
		failure.FailedAt = s.now()
	}

	// Phase 1: durable failure evidence + terminal marker.
	if err := atomicWriteJSON(s.deadPath(rec.ID), failure); err != nil {
		return err
	}
	if err := s.markRecordTerminalLocked(rec.ID, GenFailedTerminal, &failure); err != nil {
		return err
	}

	// Phase 2: drop the active ref and register the failure exactly once, in
	// an isolated index projection that is persisted atomically.
	if err := s.commitIndexProjectionLocked(func(idx *storedIndex) {
		idx.removeActiveRef(rec.ID)
		if !containsString(idx.Failures, string(rec.ID)) {
			idx.Failures = append(idx.Failures, string(rec.ID))
		}
	}); err != nil {
		return err
	}

	// Phase 3: retention; the prune removes files itself, persist the pruned
	// index best-effort (Recover rebuilds it from the dead directory).
	if s.gcDeadLettersLocked() {
		if err := s.saveIndexLocked(); err != nil {
			s.logger.Warn("dead-letter retention pruned files but index persistence failed",
				"generation", string(rec.ID), "error", err.Error())
		}
	}

	s.removeGenFilesLocked(rec.ID)
	return nil
}

// MarkSuperseded implements Store. Crash-safe and idempotent, like Succeed.
func (s *FileStore) MarkSuperseded(rec *GenerationRecord, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.markRecordTerminalLocked(rec.ID, GenSuperseded, nil); err != nil {
		return err
	}
	if err := s.commitIndexProjectionLocked(func(idx *storedIndex) {
		idx.removeActiveRef(rec.ID)
	}); err != nil {
		return err
	}
	s.removeGenFilesLocked(rec.ID)
	return nil
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

// Recover implements Store. It reconciles the in-memory index with the
// journal directories: unreadable entries are quarantined, record files
// orphaned by a crash between file write and index update are reclaimed, and
// the dead-letter index is rebuilt from disk (so crash-window dead letters
// stay queryable).
func (s *FileStore) Recover() ([]*GenerationRecord, []*FailureRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	active := make([]*GenerationRecord, 0)
	referenced := map[GenerationID]bool{}
	dirty := false

	for hookName, hi := range s.index.Hooks {
		kept := hi.Active[:0]
		for _, ref := range hi.Active {
			// A durable dead-letter record is authoritative: FailTerminal may
			// have crashed between writing it and removing this index ref.
			// Drop the stale active ref instead of reviving the generation.
			if _, statErr := os.Stat(s.deadPath(ref.ID)); statErr == nil {
				s.logger.Warn("dropping active ref for a dead-lettered generation",
					"hook", hookName, "generation", string(ref.ID))
				_ = os.Remove(s.genPath(ref.ID))
				_ = os.Remove(s.patchPath(ref.ID))
				dirty = true
				continue
			}
			rec, err := s.loadRecordLocked(ref.ID)
			if err != nil || !s.recordFilesConsistentLocked(ref.ID, rec) {
				reason := "unreadable result generation"
				if err == nil {
					reason = "generation record misses its patch file"
				}
				s.logger.Warn("quarantining "+reason,
					"hook", hookName, "generation", string(ref.ID))
				s.quarantineLocked(s.genPath(ref.ID))
				s.quarantineLocked(s.patchPath(ref.ID))
				dirty = true
				continue
			}
			// A finalized record (interrupted terminal transition) must be
			// archived, not revived as an active generation.
			if rec.State != GenActive {
				s.logger.Warn("archiving finalized result generation left by an interrupted transition",
					"hook", hookName, "generation", string(ref.ID), "state", string(rec.State))
				s.archiveTerminalRecordLocked(rec)
				dirty = true
				continue
			}
			// Every active generation survived a process restart: its owner
			// task is gone, so it can only be resumed by frozen-result match.
			if !ref.Orphaned {
				ref.Orphaned = true
				dirty = true
			}
			// Rebuild the monotonic per-hook seq (the index itself may have
			// been rebuilt from files with NextSeq reset to zero).
			if rec.Seq+1 > hi.NextSeq {
				hi.NextSeq = rec.Seq + 1
			}
			referenced[ref.ID] = true
			active = append(active, rec)
			kept = append(kept, ref)
		}
		hi.Active = kept
	}

	// Reclaim generation files not referenced by the index (a crash between
	// the record write and the index update, or an index that was rebuilt from
	// scratch after quarantine).
	genEntries, err := os.ReadDir(filepath.Join(s.dir, genDirName))
	if err != nil {
		return active, nil, fmt.Errorf("read generations dir: %w", err)
	}
	jsonGenFiles := map[GenerationID]bool{}
	for _, e := range genEntries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch filepath.Ext(name) {
		case ".json":
			id := GenerationID(strings.TrimSuffix(name, ".json"))
			jsonGenFiles[id] = true
			if referenced[id] {
				continue
			}
			rec, loadErr := s.loadRecordLocked(id)
			if loadErr != nil || !s.recordFilesConsistentLocked(id, rec) {
				s.logger.Warn("quarantining unreferenced unreadable result generation",
					"generation", string(id))
				s.quarantineLocked(s.genPath(id))
				s.quarantineLocked(s.patchPath(id))
				dirty = true
				continue
			}
			// A durable dead-letter sibling is authoritative even when the
			// record itself still reads Active (dead file committed but the
			// terminal marker rename was lost): archive, never revive.
			if _, statErr := os.Stat(s.deadPath(id)); statErr == nil {
				s.logger.Warn("archiving unreferenced generation with a dead-letter record",
					"hook", rec.HookName, "generation", string(id))
				s.archiveTerminalRecordLocked(rec)
				dirty = true
				continue
			}
			// Finalized generation whose index drop was interrupted: finish
			// the archive instead of reviving it as a restart orphan.
			if rec.State != GenActive {
				s.logger.Warn("archiving unreferenced finalized result generation",
					"hook", rec.HookName, "generation", string(id), "state", string(rec.State))
				s.archiveTerminalRecordLocked(rec)
				dirty = true
				continue
			}
			hi := s.hookIndexLocked(rec.HookName)
			if rec.Seq+1 > hi.NextSeq {
				hi.NextSeq = rec.Seq + 1
			}
			hi.Active = append(hi.Active, &storedActiveRef{
				ID:          id,
				Seq:         rec.Seq,
				TaskID:      rec.TaskID,
				Fingerprint: rec.Fingerprint,
				Orphaned:    true,
			})
			referenced[id] = true
			active = append(active, rec)
			dirty = true
			s.logger.Info("reclaimed orphaned result generation from journal files",
				"hook", rec.HookName, "generation", string(id), "seq", rec.Seq)
		case ".patch":
			// A patch file without its record is a leftover of an interrupted
			// write/retire: remove it so the directory stays bounded.
			id := GenerationID(strings.TrimSuffix(name, ".patch"))
			if !jsonGenFiles[id] {
				if rmErr := os.Remove(filepath.Join(s.dir, genDirName, name)); rmErr != nil && !os.IsNotExist(rmErr) {
					s.logger.Warn("failed to remove orphaned patch file", "file", name, "error", rmErr.Error())
				}
			}
		}
	}

	// Rebuild the dead-letter index from the dead directory: entries written
	// but lost from the index (crash window, corrupt index) stay queryable.
	failures := make([]*FailureRecord, 0)
	failureIDs := make([]string, 0)
	deadEntries, err := os.ReadDir(filepath.Join(s.dir, deadDirName))
	if err != nil {
		return active, nil, fmt.Errorf("read dead-letter dir: %w", err)
	}
	for _, e := range deadEntries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		id := GenerationID(strings.TrimSuffix(e.Name(), ".json"))
		fr, loadErr := s.loadFailureLocked(id)
		if loadErr != nil {
			s.logger.Warn("quarantining unreadable dead-letter record",
				"file", e.Name(), "error", loadErr.Error())
			s.quarantineLocked(s.deadPath(id))
			dirty = true
			continue
		}
		// Dead-lettered seqs must keep the fence counter ahead of anything
		// allocated before the index was lost.
		if hi := s.hookIndexLocked(fr.HookName); fr.Seq+1 > hi.NextSeq {
			hi.NextSeq = fr.Seq + 1
			dirty = true
		}
		failures = append(failures, fr)
		failureIDs = append(failureIDs, string(fr.GenerationID))
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].FailedAt.After(failures[j].FailedAt) })
	beforeFailures := append([]string(nil), s.index.Failures...)
	s.index.Failures = failureIDs[:0]
	for _, fr := range failures {
		s.index.Failures = append(s.index.Failures, string(fr.GenerationID))
	}
	// Enforce retention on restart as well: without it the dead directory can
	// stay over the limit until the next FailTerminal.
	if s.gcDeadLettersLocked() {
		dirty = true
	}
	if !sameStringSet(beforeFailures, s.index.Failures) {
		dirty = true
	}
	// Return only the retained records (gc may have pruned oldest entries).
	retainedFailures := failures[:0]
	for _, fr := range failures {
		if containsString(s.index.Failures, string(fr.GenerationID)) {
			retainedFailures = append(retainedFailures, fr)
		}
	}

	if dirty {
		if err := s.saveIndexLocked(); err != nil {
			return active, retainedFailures, err
		}
	}
	return active, retainedFailures, nil
}

// recordFilesConsistentLocked reports whether a generation with pending patch
// operations still has its frozen patch file. Resuming such a record could
// never reach Confirmed, so it is quarantined instead.
func (s *FileStore) recordFilesConsistentLocked(id GenerationID, rec *GenerationRecord) bool {
	if rec == nil || len(rec.Ops) == 0 {
		return rec != nil
	}
	info, err := os.Stat(s.patchPath(id))
	return err == nil && !info.IsDir()
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	return true
}

func (s *FileStore) quarantineLocked(path string) {
	// Unique suffix keeps every quarantined artifact for inspection instead
	// of overwriting a previous .corrupt on the same path.
	target := fmt.Sprintf("%s.corrupt-%d", path, s.now().UnixNano())
	if err := os.Rename(path, target); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("failed to quarantine generation file", "path", path, "error", err.Error())
	}
}

// ListFailures implements Store.
func (s *FileStore) ListFailures() ([]*FailureRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*FailureRecord, 0, len(s.index.Failures))
	for _, idStr := range s.index.Failures {
		fr, err := s.loadFailureLocked(GenerationID(idStr))
		if err != nil {
			continue
		}
		out = append(out, fr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	return out, nil
}

// GetFailure implements Store.
func (s *FileStore) GetFailure(id GenerationID) (*FailureRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadFailureLocked(id)
}

func (s *FileStore) loadFailureLocked(id GenerationID) (*FailureRecord, error) {
	raw, err := os.ReadFile(s.deadPath(id))
	if err != nil {
		return nil, fmt.Errorf("read dead-letter record %s: %w", id, err)
	}
	fr := &FailureRecord{}
	if err := json.Unmarshal(raw, fr); err != nil {
		return nil, fmt.Errorf("parse dead-letter record %s: %w", id, err)
	}
	return fr, nil
}

// ByTask implements Store.
func (s *FileStore) ByTask(hookName, taskID string) *GenerationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ref := range s.index.Hooks[hookName].ActiveRefs() {
		if ref.TaskID == taskID {
			rec, err := s.loadRecordLocked(ref.ID)
			if err == nil {
				return rec
			}
		}
	}
	return nil
}

// ResumeableByFingerprint implements Store.
func (s *FileStore) ResumeableByFingerprint(hookName, fingerprint string) *GenerationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref := s.findRefLocked(hookName, func(r *storedActiveRef) bool {
		return r.Orphaned && r.Fingerprint == fingerprint
	})
	if ref == nil {
		return nil
	}
	rec, err := s.loadRecordLocked(ref.ID)
	if err != nil {
		return nil
	}
	return rec
}

// LatestSeq implements Store.
func (s *FileStore) LatestSeq(hookName string) Seq {
	s.mu.Lock()
	defer s.mu.Unlock()
	hi := s.index.Hooks[hookName]
	if hi == nil || hi.NextSeq == 0 {
		return 0
	}
	return hi.NextSeq - 1
}

// RebindTask implements Store.
func (s *FileStore) RebindTask(id GenerationID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hi := range s.index.Hooks {
		for _, ref := range hi.Active {
			if ref.ID != id {
				continue
			}
			ref.TaskID = taskID
			ref.Orphaned = false

			// Keep the record file consistent with the index: a crash between
			// the two writes must not leave a "live" ref pointing at a record
			// that can neither be matched by task ID nor resumed as orphan.
			if rec, err := s.loadRecordLocked(id); err == nil {
				rec.TaskID = taskID
				if err := s.writeRecordFilesLocked(rec); err != nil {
					return err
				}
			} else {
				s.logger.Warn("rebinding task: generation record unreadable, index updated only",
					"generation", string(id), "error", err.Error())
			}
			return s.saveIndexLocked()
		}
	}
	return nil
}

// MarkOrphaned implements Store. A finalized record is orphaned as well:
// its owner task is gone (e.g. a failed synchronous webhook call), and the
// next call with the identical frozen result must find it by fingerprint and
// complete the terminal transition in place instead of freezing a new
// generation and replaying its side effects.
func (s *FileStore) MarkOrphaned(id GenerationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hi := range s.index.Hooks {
		for _, ref := range hi.Active {
			if ref.ID == id && !ref.Orphaned {
				ref.Orphaned = true
				return s.saveIndexLocked()
			}
		}
	}
	return nil
}

// RetireOrphans implements Store.
func (s *FileStore) RetireOrphans(hookName string) ([]GenerationID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hi := s.index.Hooks[hookName]
	if hi == nil {
		return nil, nil
	}

	addedFailures := false
	retired := make([]GenerationID, 0)
	kept := hi.Active[:0]
	for _, ref := range hi.Active {
		if ref.Orphaned {
			retired = append(retired, ref.ID)
			// A finalized dead-lettered orphan can be retired by a newer
			// generation with a different fingerprint: keep its failure
			// evidence queryable (the dead file was written before the
			// terminal marker) instead of dropping the index ref only.
			if rec, err := s.loadRecordLocked(ref.ID); err == nil && rec.State == GenFailedTerminal {
				if rec.Failure != nil {
					fr := *rec.Failure
					if fr.FailedAt.IsZero() {
						fr.FailedAt = s.now()
					}
					_ = atomicWriteJSON(s.deadPath(ref.ID), fr)
				}
				if !containsString(s.index.Failures, string(ref.ID)) {
					s.index.Failures = append(s.index.Failures, string(ref.ID))
					addedFailures = true
				}
			}
			_ = os.Remove(s.genPath(ref.ID))
			_ = os.Remove(s.patchPath(ref.ID))
			continue
		}
		kept = append(kept, ref)
	}
	hi.Active = kept

	if len(retired) > 0 {
		if err := s.saveIndexLocked(); err != nil {
			return retired, err
		}
	}
	if addedFailures {
		if s.gcDeadLettersLocked() {
			_ = s.saveIndexLocked()
		}
	}
	return retired, nil
}

// findRefLocked returns the active ref matching pred, newest first.
func (s *FileStore) findRefLocked(hookName string, pred func(*storedActiveRef) bool) *storedActiveRef {
	hi := s.index.Hooks[hookName]
	if hi == nil {
		return nil
	}
	for i := len(hi.Active) - 1; i >= 0; i-- {
		if pred(hi.Active[i]) {
			return hi.Active[i]
		}
	}
	return nil
}

// ActiveRefs is nil-safe for a missing hook index.
func (hi *storedHookIndex) ActiveRefs() []*storedActiveRef {
	if hi == nil {
		return nil
	}
	return hi.Active
}

// gcDeadLetterLimits prunes oldest dead-letter files beyond the configured
// limit. It reports whether anything was pruned. Caller must hold s.mu.
func (s *FileStore) gcDeadLettersLocked() bool {
	if s.deadLetterLimit <= 0 || len(s.index.Failures) <= s.deadLetterLimit {
		return false
	}

	records := make([]*FailureRecord, 0, len(s.index.Failures))
	for _, idStr := range s.index.Failures {
		if fr, err := s.loadFailureLocked(GenerationID(idStr)); err == nil {
			records = append(records, fr)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].FailedAt.After(records[j].FailedAt) })

	keep := map[GenerationID]bool{}
	for i, fr := range records {
		if i < s.deadLetterLimit {
			keep[fr.GenerationID] = true
			continue
		}
		_ = os.Remove(s.deadPath(fr.GenerationID))
	}

	pruned := s.index.Failures[:0]
	for _, idStr := range s.index.Failures {
		if keep[GenerationID(idStr)] {
			pruned = append(pruned, idStr)
		}
	}
	s.index.Failures = pruned
	return true
}

func atomicWriteJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	return atomicWriteFile(path, raw, 0o600)
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	tmp := fmt.Sprintf("%s.tmp-%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", path, err)
	}
	return nil
}
