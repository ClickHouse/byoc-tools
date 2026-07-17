GO_DIR := go
GO_BIN := $(GO_DIR)/list-top-level-prefixes

.PHONY: all deps build python-deps fmt vet clean

all: build

## Download Go module dependencies
deps:
	cd $(GO_DIR) && go mod download

## Build the Go binary (downloads dependencies first)
build: deps
	cd $(GO_DIR) && go build -o list-top-level-prefixes .
	@echo "Built $(GO_BIN)"

## Install Python dependencies for the scripts
python-deps:
	pip install -r requirements.txt

fmt:
	cd $(GO_DIR) && gofmt -w .

vet:
	cd $(GO_DIR) && go vet ./...

clean:
	rm -f $(GO_BIN)
