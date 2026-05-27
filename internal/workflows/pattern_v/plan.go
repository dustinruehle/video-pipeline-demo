package pattern_v

import (
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// PlanFor returns the derivative plan for a given media type. This is the
// "plan in code" version (Pattern V). Adding a new media type means editing
// this function, code-reviewing the diff, and deploying a new worker.
func PlanFor(mt types.MediaType) types.MediaPlan {
	switch mt {
	case types.MediaTypeShortClip:
		return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
			{Kind: types.DerivativeMetadata},
			{Kind: types.DerivativeFFmpegEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
			{Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeFFmpegEncode}},
			{Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{types.DerivativeMetadata, types.DerivativeMPV}},
		}}
	case types.MediaTypeStandardHD:
		return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
			{Kind: types.DerivativeMetadata},
			{Kind: types.DerivativeFFmpegEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
			{Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeFFmpegEncode}},
			{Kind: types.DerivativeHLS, DependsOn: []types.DerivativeKind{types.DerivativeMPV}},
			{Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{types.DerivativeMetadata, types.DerivativeHLS}},
		}}
	case types.MediaTypeSpherical:
		return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
			{Kind: types.DerivativeMetadata},
			{Kind: types.DerivativeConcat, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
			{Kind: types.DerivativeCustomEncode, DependsOn: []types.DerivativeKind{types.DerivativeConcat}},
			{Kind: types.DerivativeStabilize, DependsOn: []types.DerivativeKind{types.DerivativeCustomEncode}},
			{Kind: types.DerivativeProjection, DependsOn: []types.DerivativeKind{types.DerivativeStabilize}},
			{Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeProjection}},
			{Kind: types.DerivativeHLS, DependsOn: []types.DerivativeKind{types.DerivativeMPV}},
			{Kind: types.DerivativeEditProxy, DependsOn: []types.DerivativeKind{types.DerivativeProjection}},
			{Kind: types.DerivativeThumbnail, DependsOn: []types.DerivativeKind{types.DerivativeProjection}},
			{Kind: types.DerivativeBitrateLow, DependsOn: []types.DerivativeKind{types.DerivativeHLS}},
			{Kind: types.DerivativeBitrateHigh, DependsOn: []types.DerivativeKind{types.DerivativeHLS}},
			{Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{
				types.DerivativeMetadata,
				types.DerivativeHLS,
				types.DerivativeEditProxy,
				types.DerivativeThumbnail,
				types.DerivativeBitrateLow,
				types.DerivativeBitrateHigh,
			}},
		}}
	case types.MediaTypeMisconfigured:
		return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
			{Kind: types.DerivativeMetadata},
			{Kind: types.DerivativeCustomEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
			{Kind: types.DerivativeFFmpegEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
			{Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeFFmpegEncode}},
			{Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{types.DerivativeCustomEncode, types.DerivativeMPV}},
		}}
	default:
		panic("unknown media type")
	}
}
