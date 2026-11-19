.PHONY: all build lint clean

all: build

## Build every tool that needs building
build:
	$(MAKE) -C s3-cleanup build

## The checks CI runs
lint:
	shellcheck kill-switch/*.sh
	for f in kill-switch/*.sh; do bash -n "$$f" || exit 1; done
	python3 -m py_compile s3-cleanup/*.py
	$(MAKE) -C s3-cleanup vet

clean:
	$(MAKE) -C s3-cleanup clean
