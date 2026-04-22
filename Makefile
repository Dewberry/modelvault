.PHONY: build build-docker clean help

help:
	@echo "modelvault build targets:"
	@echo "  make build         - Build using Docker (recommended)"
	@echo "  make build-docker  - Same as 'make build'"
	@echo "  make clean         - Remove compiled binaries"
	@echo "  make help          - Show this help message"

build: build-docker

build-docker:
	@bash ./build.sh

clean:
	rm -rf ./binary/*
	@echo "Cleaned binary directory"
