package task_metadata

import (
	"fmt"

	v1 "k8s.io/api/admission/v1"
	apixv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	bindingcontext "github.com/flant/shell-operator/pkg/hook/binding_context"
	htypes "github.com/flant/shell-operator/pkg/hook/types"
	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
	"github.com/flant/shell-operator/pkg/task"
	json "github.com/flant/shell-operator/pkg/utils/json"
)

// HookMetadata is the metadata of every HookRun/Enable*Bindings task and must
// survive a restart, so it registers a dedicated checkpoint representation.
//
// A plain JSON round-trip is not sufficient: BindingContext has a hook-facing
// MarshalJSON that depends on a version chosen when the context is sent to a
// hook, and ObjectAndFilterResult only exposes a reduced Map(). The DTOs below
// persist the full in-memory shape instead.
func init() {
	task.RegisterMetadataType("HookMetadata", func() any { return &HookMetadata{} })
}

type hookMetadataCheckpoint struct {
	HookName                 string
	Binding                  string
	Group                    string
	BindingType              htypes.BindingType
	BindingContext           []bindingContextCheckpoint
	AllowFailure             bool
	MonitorIDs               []string
	ExecuteOnSynchronization bool
}

type bindingContextCheckpoint struct {
	Version             string
	BindingType         htypes.BindingType
	JqFilter            string
	IncludeSnapshots    []string
	IncludeAllSnapshots bool
	Group               string

	Binding    string
	Type       kemtypes.KubeEventType
	WatchEvent kemtypes.WatchEventType
	Objects    []objectAndFilterResultCheckpoint
	Snapshots  map[string][]objectAndFilterResultCheckpoint

	AdmissionReview  *v1.AdmissionReview
	ConversionReview *apixv1.ConversionReview
	FromVersion      string
	ToVersion        string
}

// objectAndFilterResultCheckpoint mirrors kemtypes.ObjectAndFilterResult.
// FilterResult is kept as raw JSON because its concrete type is unknown
// (a jq output string or an arbitrary decoded value).
type objectAndFilterResultCheckpoint struct {
	JqFilter     string
	Checksum     string
	ResourceId   string
	RemoveObject bool
	Object       *unstructured.Unstructured
	FilterResult json.RawMessage
}

// MarshalCheckpoint implements task.CheckpointMarshaler.
func (m HookMetadata) MarshalCheckpoint() (any, error) {
	dto := hookMetadataCheckpoint{
		HookName:                 m.HookName,
		Binding:                  m.Binding,
		Group:                    m.Group,
		BindingType:              m.BindingType,
		BindingContext:           make([]bindingContextCheckpoint, 0, len(m.BindingContext)),
		AllowFailure:             m.AllowFailure,
		MonitorIDs:               m.MonitorIDs,
		ExecuteOnSynchronization: m.ExecuteOnSynchronization,
	}

	for i := range m.BindingContext {
		bcDTO, err := toBindingContextCheckpoint(m.BindingContext[i])
		if err != nil {
			return nil, fmt.Errorf("binding context %d: %w", i, err)
		}
		dto.BindingContext = append(dto.BindingContext, bcDTO)
	}

	return &dto, nil
}

// CheckpointMetadataValue implements task.CheckpointMetadataValue: tasks keep
// HookMetadata by value, not by pointer.
func (m *HookMetadata) CheckpointMetadataValue() any {
	return *m
}

// UnmarshalCheckpoint implements task.CheckpointUnmarshaler.
func (m *HookMetadata) UnmarshalCheckpoint(data []byte) error {
	dto := hookMetadataCheckpoint{}
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}

	contexts := make([]bindingcontext.BindingContext, len(dto.BindingContext))
	for i, bcDTO := range dto.BindingContext {
		contexts[i] = fromBindingContextCheckpoint(bcDTO)
	}

	*m = HookMetadata{
		HookName:                 dto.HookName,
		Binding:                  dto.Binding,
		Group:                    dto.Group,
		BindingType:              dto.BindingType,
		BindingContext:           contexts,
		AllowFailure:             dto.AllowFailure,
		MonitorIDs:               dto.MonitorIDs,
		ExecuteOnSynchronization: dto.ExecuteOnSynchronization,
	}
	return nil
}

func toBindingContextCheckpoint(bc bindingcontext.BindingContext) (bindingContextCheckpoint, error) {
	dto := bindingContextCheckpoint{
		Version:             bc.Metadata.Version,
		BindingType:         bc.Metadata.BindingType,
		JqFilter:            bc.Metadata.JqFilter,
		IncludeSnapshots:    bc.Metadata.IncludeSnapshots,
		IncludeAllSnapshots: bc.Metadata.IncludeAllSnapshots,
		Group:               bc.Metadata.Group,
		Binding:             bc.Binding,
		Type:                bc.Type,
		WatchEvent:          bc.WatchEvent,
		Objects:             make([]objectAndFilterResultCheckpoint, 0, len(bc.Objects)),
		AdmissionReview:     bc.AdmissionReview,
		ConversionReview:    bc.ConversionReview,
		FromVersion:         bc.FromVersion,
		ToVersion:           bc.ToVersion,
	}

	for i := range bc.Objects {
		objDTO, err := toObjectCheckpoint(bc.Objects[i])
		if err != nil {
			return dto, fmt.Errorf("objects[%d]: %w", i, err)
		}
		dto.Objects = append(dto.Objects, objDTO)
	}

	if len(bc.Snapshots) > 0 {
		dto.Snapshots = make(map[string][]objectAndFilterResultCheckpoint, len(bc.Snapshots))
		for key, objects := range bc.Snapshots {
			arr := make([]objectAndFilterResultCheckpoint, 0, len(objects))
			for i := range objects {
				objDTO, err := toObjectCheckpoint(objects[i])
				if err != nil {
					return dto, fmt.Errorf("snapshots[%s][%d]: %w", key, i, err)
				}
				arr = append(arr, objDTO)
			}
			dto.Snapshots[key] = arr
		}
	}

	return dto, nil
}

func fromBindingContextCheckpoint(dto bindingContextCheckpoint) bindingcontext.BindingContext {
	bc := bindingcontext.BindingContext{
		Binding:          dto.Binding,
		Type:             dto.Type,
		WatchEvent:       dto.WatchEvent,
		Objects:          make([]kemtypes.ObjectAndFilterResult, 0, len(dto.Objects)),
		AdmissionReview:  dto.AdmissionReview,
		ConversionReview: dto.ConversionReview,
		FromVersion:      dto.FromVersion,
		ToVersion:        dto.ToVersion,
	}
	bc.Metadata.Version = dto.Version
	bc.Metadata.BindingType = dto.BindingType
	bc.Metadata.JqFilter = dto.JqFilter
	bc.Metadata.IncludeSnapshots = dto.IncludeSnapshots
	bc.Metadata.IncludeAllSnapshots = dto.IncludeAllSnapshots
	bc.Metadata.Group = dto.Group

	for _, objDTO := range dto.Objects {
		bc.Objects = append(bc.Objects, fromObjectCheckpoint(objDTO))
	}

	if len(dto.Snapshots) > 0 {
		bc.Snapshots = make(map[string][]kemtypes.ObjectAndFilterResult, len(dto.Snapshots))
		for key, arr := range dto.Snapshots {
			objects := make([]kemtypes.ObjectAndFilterResult, 0, len(arr))
			for _, objDTO := range arr {
				objects = append(objects, fromObjectCheckpoint(objDTO))
			}
			bc.Snapshots[key] = objects
		}
	}

	return bc
}

func toObjectCheckpoint(obj kemtypes.ObjectAndFilterResult) (objectAndFilterResultCheckpoint, error) {
	dto := objectAndFilterResultCheckpoint{
		Object:       obj.Object,
		JqFilter:     obj.Metadata.JqFilter,
		Checksum:     obj.Metadata.Checksum,
		ResourceId:   obj.Metadata.ResourceId,
		RemoveObject: obj.Metadata.RemoveObject,
		FilterResult: json.RawMessage("null"),
	}

	if obj.FilterResult != nil {
		raw, err := json.Marshal(obj.FilterResult)
		if err != nil {
			return dto, fmt.Errorf("encode filter result: %w", err)
		}
		dto.FilterResult = raw
	}

	return dto, nil
}

func fromObjectCheckpoint(dto objectAndFilterResultCheckpoint) kemtypes.ObjectAndFilterResult {
	obj := kemtypes.ObjectAndFilterResult{
		Object: dto.Object,
	}
	obj.Metadata.JqFilter = dto.JqFilter
	obj.Metadata.Checksum = dto.Checksum
	obj.Metadata.ResourceId = dto.ResourceId
	obj.Metadata.RemoveObject = dto.RemoveObject

	if len(dto.FilterResult) > 0 && string(dto.FilterResult) != "null" {
		var v any
		if err := json.Unmarshal(dto.FilterResult, &v); err == nil {
			obj.FilterResult = v
		}
	}

	return obj
}
