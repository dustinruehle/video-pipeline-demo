// Package types holds the data structures shared across both demo patterns
// (Pattern V and Pattern D) and the activity layer. Centralizing them here
// keeps the workflow code and the executor decoupled from each pattern's
// plan source (Go function vs YAML file).
package types

type MediaType string

const (
	MediaTypeShortClip     MediaType = "short_clip"
	MediaTypeStandardHD    MediaType = "standard_hd"
	MediaTypeSpherical     MediaType = "spherical"
	MediaTypeMisconfigured MediaType = "misconfigured"
)

type CameraModel string

const (
	CameraMobile         CameraModel = "mobile"
	CameraActionStandard CameraModel = "action_standard"
	CameraActionPro      CameraModel = "action_pro"
	CameraSpherical      CameraModel = "spherical"
)

type Source string

const (
	SourceWiFiSync   Source = "wifi_sync"
	SourceMobileApp  Source = "mobile_app"
	SourceWebLibrary Source = "web_library"
)

type MediaIngestRequest struct {
	MediaID     string
	CameraModel CameraModel
	MediaType   MediaType
	Source      Source
	InputPath   string
	OutputDir   string
	DemoMode    bool
}

type DerivativeKind string

const (
	DerivativeMetadata     DerivativeKind = "metadata"
	DerivativeCustomEncode DerivativeKind = "custom_encode"
	DerivativeFFmpegEncode DerivativeKind = "ffmpeg_encode"
	DerivativeHLS          DerivativeKind = "hls"
	DerivativeEditProxy    DerivativeKind = "edit_proxy"
	DerivativeStabilize    DerivativeKind = "stabilize"
	DerivativeConcat       DerivativeKind = "concat"
	DerivativeMPV          DerivativeKind = "mpv"
	DerivativeThumbnail    DerivativeKind = "thumbnail"
	DerivativeProjection   DerivativeKind = "projection"
	DerivativeBitrateLow   DerivativeKind = "bitrate_low"
	DerivativeBitrateHigh  DerivativeKind = "bitrate_high"
	DerivativePublish      DerivativeKind = "publish"
)

// KnownDerivativeKinds is the authoritative set. Pattern D's validator and
// the planexec validator both consult it. Keep alphabetized by string value.
var KnownDerivativeKinds = map[DerivativeKind]bool{
	DerivativeBitrateHigh:  true,
	DerivativeBitrateLow:   true,
	DerivativeConcat:       true,
	DerivativeCustomEncode: true,
	DerivativeEditProxy:    true,
	DerivativeFFmpegEncode: true,
	DerivativeHLS:          true,
	DerivativeMetadata:     true,
	DerivativeMPV:          true,
	DerivativeProjection:   true,
	DerivativePublish:      true,
	DerivativeStabilize:    true,
	DerivativeThumbnail:    true,
}

type DerivativeStep struct {
	Kind      DerivativeKind
	DependsOn []DerivativeKind
	// Config is optional per-step configuration. Pattern V leaves this nil;
	// Pattern D populates it from the YAML `config:` block (e.g. sleep_ms).
	Config map[string]any
}

type MediaPlan struct {
	MediaType MediaType
	Steps     []DerivativeStep
}

type DerivativeOutput struct {
	Kind       DerivativeKind
	Path       string
	Bytes      int64
	DurationMs int64
}

type ProduceDerivativeInput struct {
	MediaID string
	Kind    DerivativeKind
	Inputs  []DerivativeOutput
	Req     MediaIngestRequest
	Config  map[string]any
}

// SignalAddDerivative is the signal name Pattern D's workflow listens for to
// append an extra DerivativeStep at runtime. CLI form:
//
//	temporal workflow signal --workflow-id <id> --name add_derivative \
//	    --input '{"Kind":"thumbnail","DependsOn":[]}'
const SignalAddDerivative = "add_derivative"
