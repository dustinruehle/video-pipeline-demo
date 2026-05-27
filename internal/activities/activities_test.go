package activities

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

func tinyFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "fixtures", "tinier.mp4")
	if _, err := os.Stat(path); err != nil {
		path = filepath.Join("..", "..", "testdata", "fixtures", "tiny.mp4")
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("no fixture clip at %s (run scripts/ensure-sample-video.sh)", path)
	}
	return path
}

func newActivityEnv() *testsuite.TestActivityEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(ProduceDerivative)
	return env
}

func TestExtractMetadata_HappyPath(t *testing.T) {
	env := newActivityEnv()
	out, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_meta",
		Kind:    types.DerivativeMetadata,
		Req: types.MediaIngestRequest{
			MediaID:   "test_meta",
			InputPath: tinyFixture(t),
			OutputDir: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("extractMetadata: %v", err)
	}
	var got types.DerivativeOutput
	if err := out.Get(&got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != types.DerivativeMetadata {
		t.Fatalf("kind: want metadata got %s", got.Kind)
	}
	if got.Path == "" || got.Bytes == 0 {
		t.Fatalf("metadata output incomplete: %+v", got)
	}
}

func TestTranscodeFFmpeg_HappyPath(t *testing.T) {
	env := newActivityEnv()
	out, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_ffmpeg",
		Kind:    types.DerivativeMPV,
		Req: types.MediaIngestRequest{
			MediaID:   "test_ffmpeg",
			InputPath: tinyFixture(t),
			OutputDir: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("transcode: %v", err)
	}
	var got types.DerivativeOutput
	if err := out.Get(&got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != types.DerivativeMPV {
		t.Fatalf("kind: %s", got.Kind)
	}
	if _, err := os.Stat(got.Path); err != nil {
		t.Fatalf("output missing: %v", err)
	}
}

func TestTranscodeCustom_StubProducesFile(t *testing.T) {
	env := newActivityEnv()
	tmp := t.TempDir()
	out, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_custom",
		Kind:    types.DerivativeCustomEncode,
		Req: types.MediaIngestRequest{
			MediaID:   "test_custom",
			InputPath: tinyFixture(t),
			OutputDir: tmp,
		},
		Config: map[string]any{"sleep_ms": 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got types.DerivativeOutput
	if err := out.Get(&got); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got.Path, tmp) {
		t.Fatalf("output not in tempdir: %s", got.Path)
	}
	if _, err := os.Stat(got.Path); err != nil {
		t.Fatalf("expected marker file at %s: %v", got.Path, err)
	}
}

func TestPublish_WritesManifest(t *testing.T) {
	env := newActivityEnv()
	tmp := t.TempDir()
	out, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_pub",
		Kind:    types.DerivativePublish,
		Inputs: []types.DerivativeOutput{
			{Kind: types.DerivativeMetadata, Path: filepath.Join(tmp, "metadata.json")},
			{Kind: types.DerivativeMPV, Path: filepath.Join(tmp, "mpv.mp4")},
		},
		Req: types.MediaIngestRequest{
			MediaID:   "test_pub",
			InputPath: tinyFixture(t),
			OutputDir: tmp,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got types.DerivativeOutput
	if err := out.Get(&got); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(got.Path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(data), "test_pub") {
		t.Fatalf("manifest missing media id: %s", data)
	}
}

func TestMisconfigured_CustomEncodeFails(t *testing.T) {
	env := newActivityEnv()
	_, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_misconfig",
		Kind:    types.DerivativeCustomEncode,
		Req: types.MediaIngestRequest{
			MediaID:   "test_misconfig",
			MediaType: types.MediaTypeMisconfigured,
			InputPath: tinyFixture(t),
			OutputDir: t.TempDir(),
		},
	})
	if err == nil {
		t.Fatal("expected misconfigured activity to fail")
	}
	if !strings.Contains(err.Error(), "misconfigured") {
		t.Fatalf("error did not mention misconfigured: %v", err)
	}
}

func TestUnknownKind_NonRetryable(t *testing.T) {
	env := newActivityEnv()
	_, err := env.ExecuteActivity(ProduceDerivative, types.ProduceDerivativeInput{
		MediaID: "test_unknown",
		Kind:    "not_real",
		Req: types.MediaIngestRequest{
			MediaID:   "test_unknown",
			InputPath: tinyFixture(t),
			OutputDir: t.TempDir(),
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected ApplicationError, got %T: %v", err, err)
	}
	if !appErr.NonRetryable() {
		t.Fatal("unknown-kind error must be non-retryable")
	}
}

func TestSleepWithHeartbeat_ZeroDuration(t *testing.T) {
	// Pure helper test — no activity registration needed.
	if err := sleepWithHeartbeat(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestTimeToMilliseconds(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"00:00:00.000", 0},
		{"00:00:01.000", 1000},
		{"00:00:01.5", 1500},
		{"00:01:00.000", 60000},
		{"01:00:00.000", 3600000},
		{"01:02:03.456", 3723456},
	}
	for _, tc := range cases {
		if got := timeToMilliseconds(tc.in); got != tc.want {
			t.Errorf("%q: want %d got %d", tc.in, tc.want, got)
		}
	}
}
