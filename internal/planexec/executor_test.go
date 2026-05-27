package planexec

import (
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

func step(k types.DerivativeKind, deps ...types.DerivativeKind) types.DerivativeStep {
	return types.DerivativeStep{Kind: k, DependsOn: deps}
}

func TestValidate_RejectsCycle(t *testing.T) {
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata, types.DerivativeFFmpegEncode),
		step(types.DerivativeFFmpegEncode, types.DerivativeMetadata),
	}
	err := validate(plan)
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestValidate_RejectsUnknownKind(t *testing.T) {
	plan := []types.DerivativeStep{
		step("not_real"),
	}
	err := validate(plan)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknown-kind error, got %v", err)
	}
}

func TestValidate_RejectsMissingDependency(t *testing.T) {
	plan := []types.DerivativeStep{
		step(types.DerivativeFFmpegEncode, types.DerivativeMetadata),
	}
	err := validate(plan)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected missing-dep error, got %v", err)
	}
}

func TestValidate_RejectsDuplicateKind(t *testing.T) {
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata),
		step(types.DerivativeMetadata),
	}
	err := validate(plan)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-kind error, got %v", err)
	}
}

func TestValidate_RejectsSelfLoop(t *testing.T) {
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata, types.DerivativeMetadata),
	}
	err := validate(plan)
	if err == nil {
		t.Fatalf("expected self-loop error, got nil")
	}
}

func TestGroupByLevel_FlatPlan(t *testing.T) {
	// Linear chain: metadata → ffmpeg_encode → mpv → publish
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata),
		step(types.DerivativeFFmpegEncode, types.DerivativeMetadata),
		step(types.DerivativeMPV, types.DerivativeFFmpegEncode),
		step(types.DerivativePublish, types.DerivativeMPV),
	}
	if err := validate(plan); err != nil {
		t.Fatal(err)
	}
	levels := groupByLevel(plan)
	if len(levels) != 4 {
		t.Fatalf("expected 4 levels, got %d: %#v", len(levels), levels)
	}
	for i, l := range levels {
		if len(l) != 1 {
			t.Fatalf("level %d expected size 1, got %d", i, len(l))
		}
	}
}

func TestGroupByLevel_ParallelLeaves(t *testing.T) {
	// metadata → {thumbnail, edit_proxy, mpv} all parallel
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata),
		step(types.DerivativeThumbnail, types.DerivativeMetadata),
		step(types.DerivativeEditProxy, types.DerivativeMetadata),
		step(types.DerivativeMPV, types.DerivativeMetadata),
	}
	if err := validate(plan); err != nil {
		t.Fatal(err)
	}
	levels := groupByLevel(plan)
	if len(levels) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(levels))
	}
	if len(levels[0]) != 1 || levels[0][0].Kind != types.DerivativeMetadata {
		t.Fatalf("level 0: %#v", levels[0])
	}
	if len(levels[1]) != 3 {
		t.Fatalf("level 1 size: %d", len(levels[1]))
	}
	// Alphabetical: edit_proxy, mpv, thumbnail
	wantOrder := []types.DerivativeKind{types.DerivativeEditProxy, types.DerivativeMPV, types.DerivativeThumbnail}
	for i, want := range wantOrder {
		if levels[1][i].Kind != want {
			t.Fatalf("level 1 pos %d: want %q got %q", i, want, levels[1][i].Kind)
		}
	}
}

func TestGroupByLevel_DAGOrdering_Spherical(t *testing.T) {
	// Spherical-shape: 12 steps, nontrivial branching
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata),
		step(types.DerivativeConcat, types.DerivativeMetadata),
		step(types.DerivativeCustomEncode, types.DerivativeConcat),
		step(types.DerivativeStabilize, types.DerivativeCustomEncode),
		step(types.DerivativeProjection, types.DerivativeStabilize),
		step(types.DerivativeMPV, types.DerivativeProjection),
		step(types.DerivativeHLS, types.DerivativeMPV),
		step(types.DerivativeEditProxy, types.DerivativeProjection),
		step(types.DerivativeThumbnail, types.DerivativeProjection),
		step(types.DerivativeBitrateLow, types.DerivativeHLS),
		step(types.DerivativeBitrateHigh, types.DerivativeHLS),
		step(types.DerivativePublish,
			types.DerivativeMetadata,
			types.DerivativeHLS,
			types.DerivativeEditProxy,
			types.DerivativeThumbnail,
			types.DerivativeBitrateLow,
			types.DerivativeBitrateHigh,
		),
	}
	if err := validate(plan); err != nil {
		t.Fatal(err)
	}
	levels := groupByLevel(plan)
	// Expected:
	//  L0: metadata
	//  L1: concat
	//  L2: custom_encode
	//  L3: stabilize
	//  L4: projection
	//  L5: edit_proxy, mpv, thumbnail
	//  L6: hls
	//  L7: bitrate_high, bitrate_low
	//  L8: publish
	want := [][]types.DerivativeKind{
		{types.DerivativeMetadata},
		{types.DerivativeConcat},
		{types.DerivativeCustomEncode},
		{types.DerivativeStabilize},
		{types.DerivativeProjection},
		{types.DerivativeEditProxy, types.DerivativeMPV, types.DerivativeThumbnail},
		{types.DerivativeHLS},
		{types.DerivativeBitrateHigh, types.DerivativeBitrateLow},
		{types.DerivativePublish},
	}
	if len(levels) != len(want) {
		t.Fatalf("levels: want %d got %d (%#v)", len(want), len(levels), levels)
	}
	for i := range want {
		gotKinds := make([]types.DerivativeKind, 0, len(levels[i]))
		for _, s := range levels[i] {
			gotKinds = append(gotKinds, s.Kind)
		}
		if !reflect.DeepEqual(gotKinds, want[i]) {
			t.Fatalf("level %d: want %v got %v", i, want[i], gotKinds)
		}
	}
}

func TestGroupByLevel_Deterministic(t *testing.T) {
	plan := []types.DerivativeStep{
		step(types.DerivativeMetadata),
		step(types.DerivativeMPV, types.DerivativeMetadata),
		step(types.DerivativeThumbnail, types.DerivativeMetadata),
		step(types.DerivativeEditProxy, types.DerivativeMetadata),
		step(types.DerivativeHLS, types.DerivativeMPV),
		step(types.DerivativePublish, types.DerivativeHLS, types.DerivativeThumbnail, types.DerivativeEditProxy),
	}
	if err := validate(plan); err != nil {
		t.Fatal(err)
	}
	first := groupByLevel(plan)
	for i := 0; i < 100; i++ {
		got := groupByLevel(plan)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("groupByLevel not deterministic on iteration %d", i)
		}
	}
}
