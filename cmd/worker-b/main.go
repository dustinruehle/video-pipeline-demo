package main

import (
	"log"

	"go.temporal.io/sdk/worker"

	"github.com/temporal-community/video-pipeline-demo/internal/activities"
	"github.com/temporal-community/video-pipeline-demo/internal/searchattrs"
	"github.com/temporal-community/video-pipeline-demo/internal/tclient"
	"github.com/temporal-community/video-pipeline-demo/internal/workflows/pattern_d"
)

const taskQueue = "video-pipeline-b"

func main() {
	c, err := tclient.New()
	if err != nil {
		log.Fatalf("tclient: %v", err)
	}
	defer c.Close()

	_ = searchattrs.EnsureRegistered(c)

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(pattern_d.DynamicMediaWorkflow)
	w.RegisterActivity(activities.ProduceDerivative)
	w.RegisterActivity(activities.MarkCancelled)

	log.Printf("worker started on task queue %s", taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker run: %v", err)
	}
}
