.PHONY: check fmt vet staticcheck test build version

check: fmt vet staticcheck test

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

test:
	go test -race ./...

VERSION := $(shell scripts/version.sh)

build:
	CGO_ENABLED=0 go build -ldflags "-X main.version=$(VERSION)" -o bin/store ./cmd/store

version:
	@scripts/version.sh

.PHONY: pg-start pg-stop
pg-start:
	scripts/pg-start.sh
pg-stop:
	scripts/pg-stop.sh
