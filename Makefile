GO_DIR := go
GO_BIN := $(GO_DIR)/list-top-level-prefixes/list-top-level-prefixes
GO_DELETE_BIN := $(GO_DIR)/delete-prefixes/delete-prefixes
GO_LIST_DATA_BIN := $(GO_DIR)/list-data-prefixes/list-data-prefixes

.PHONY: all deps build python-deps fmt vet clean

all: build

## Download Go module dependencies
deps:
	cd $(GO_DIR) && go mod download

## Build the Go binaries (downloads dependencies first)
build: deps
	cd $(GO_DIR) && go build -o list-top-level-prefixes/list-top-level-prefixes ./list-top-level-prefixes
	cd $(GO_DIR) && go build -o delete-prefixes/delete-prefixes ./delete-prefixes
	cd $(GO_DIR) && go build -o list-data-prefixes/list-data-prefixes ./list-data-prefixes
	@echo "Built $(GO_BIN), $(GO_DELETE_BIN) and $(GO_LIST_DATA_BIN)"

## Install Python dependencies for the scripts
python-deps:
	pip install -r requirements.txt

fmt:
	cd $(GO_DIR) && gofmt -w .

vet:
	cd $(GO_DIR) && go vet ./...

clean:
	rm -f $(GO_BIN) $(GO_DELETE_BIN) $(GO_LIST_DATA_BIN)
