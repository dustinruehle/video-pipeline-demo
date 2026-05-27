package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/temporal-community/video-pipeline-demo/internal/tclient"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
	"github.com/temporal-community/video-pipeline-demo/internal/workflows/pattern_d"
)

const taskQueue = "video-pipeline-b"

func main() {
	mediaID := flag.String("media-id", "", "MediaID (required)")
	planFile := flag.String("plan-file", "", "Path to a DSL YAML plan (required)")
	camera := flag.String("camera", "mobile", "camera model")
	source := flag.String("source", "mobile_app", "ingest source")
	input := flag.String("input", "samples/sample.mp4", "input video path")
	demoMode := flag.Bool("demo-mode", false, "pad activities for visible pause/resume")
	flag.Parse()

	if *mediaID == "" || *planFile == "" {
		log.Fatalf("--media-id and --plan-file are required")
	}

	plan, err := pattern_d.ParseFile(*planFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plan-file YAML is invalid: %v\n", err)
		os.Exit(1)
	}
	if err := plan.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "plan-file failed validation: %v\n", err)
		os.Exit(1)
	}

	c, err := tclient.New()
	if err != nil {
		log.Fatalf("tclient: %v", err)
	}
	defer c.Close()

	req := types.MediaIngestRequest{
		MediaID:     *mediaID,
		MediaType:   types.MediaType(plan.Name),
		CameraModel: types.CameraModel(*camera),
		Source:      types.Source(*source),
		InputPath:   *input,
		DemoMode:    *demoMode,
	}

	opts := client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("video-pipeline-b-%s", req.MediaID),
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 5 * time.Minute,
	}
	run, err := c.ExecuteWorkflow(context.Background(), opts, pattern_d.DynamicMediaWorkflow,
		pattern_d.PatternDInput{Req: req, Plan: plan})
	if err != nil {
		log.Fatalf("execute workflow: %v", err)
	}
	fmt.Printf("Started workflow %s (run %s)\n", run.GetID(), run.GetRunID())

	waitCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	runErr := run.Get(waitCtx, nil)

	desc, derr := c.DescribeWorkflowExecution(context.Background(), run.GetID(), run.GetRunID())
	status := "UNKNOWN"
	if derr == nil && desc.WorkflowExecutionInfo != nil && desc.WorkflowExecutionInfo.Status != 0 {
		status = desc.WorkflowExecutionInfo.Status.String()
	}
	fmt.Printf("Workflow %s ended with status %s\n", run.GetID(), status)
	fmt.Printf("View: %s/%s\n", tclient.UIBaseURL(), run.GetID())
	if runErr != nil {
		fmt.Printf("(workflow returned error: %v)\n", runErr)
	}
	os.Exit(0)
}
