# samples/

Drop your real demo clip in this directory as `sample.mp4` (or `sample.mov`).
Constraints:

- < 50 MB
- 5–30 seconds
- `.mp4` or `.mov` container

`scripts/ensure-sample-video.sh` generates a synthetic 10-second test clip
**only if `sample.mp4` does not already exist** — it will never overwrite a
file you placed here.

To use a clip at a different path, pass `--input /absolute/path.mp4` to the
starter, e.g.:

```
./bin/starter-a --media-id vid_test --media-type short_clip \
    --camera mobile --input ~/Movies/clip.mp4
```
