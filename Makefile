.PHONY: install-prereqs setup teardown deps build test test-integration capture-histories lint demo-a demo-b demo-a-cloud demo-b-cloud clean

install-prereqs:
	@./scripts/install-prereqs.sh

setup: install-prereqs
	@go mod download
	@./scripts/start-dev-server.sh
	@./scripts/ensure-sample-video.sh
	@./scripts/register-search-attrs.sh
	@$(MAKE) build

teardown:
	@./scripts/stop-dev-server.sh
	@rm -rf tmp/dev-server.log tmp/dev-server.pid tmp/temporal.db tmp/temporal.db-* tmp/output tmp/worker-*.log tmp/worker-*.pid tmp/*.yaml

deps:
	@go mod download

build:
	@mkdir -p bin
	@go build -o bin/worker-a ./cmd/worker-a
	@go build -o bin/worker-b ./cmd/worker-b
	@go build -o bin/starter-a ./cmd/starter-a
	@go build -o bin/starter-b ./cmd/starter-b

test:
	@go test ./... -count=1

test-integration:
	@temporal operator namespace describe default >/dev/null 2>&1 \
	    || { echo "ERROR: dev server not running. Run 'make setup' first." >&2; exit 1; }
	@go test ./internal/integration/... -count=1 -tags=integration -timeout=5m

capture-histories:
	@./scripts/capture-histories.sh

lint:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt issues:"; echo "$$out"; exit 1; fi
	@go vet ./...

demo-a:
	@./demo-a.sh

demo-b:
	@./demo-b.sh

demo-a-cloud:
	@./demo-a-cloud.sh

demo-b-cloud:
	@./demo-b-cloud.sh

clean:
	@rm -rf bin/ tmp/
