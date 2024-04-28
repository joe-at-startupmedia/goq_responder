GO ?= go
GOFMT ?= gofmt "-s"
GOFILES := $(shell find . -name "*.go")

all: build

.PHONY: build
build: 
	$(GO) mod tidy
	$(GO) build -o bin/bytes example/bytes/bytes.go
	$(GO) build -o bin/mqrequest example/mqrequest/mqrequest.go
	$(GO) build -o bin/protobuf example/protobuf/protobuf.go
	$(GO) build -o bin/timeout example/timeout/timeout.go

.PHONY: test
test: 
	$(GO) test -v

.PHONY: examples
examples: 
	./bin/bytes
	./bin/mqrequest
	./bin/protobuf
	./bin/timeout

protogen:
	protoc protos/*.proto  --go_out=.
	protoc example/protobuf/protos/*.proto  --go_out=.

.PHONY: fmt
fmt:
	$(GOFMT) -w $(GOFILES)

.PHONY: fmt-check
fmt-check:
	@diff=$$($(GOFMT) -d $(GOFILES)); \
  if [ -n "$$diff" ]; then \
    echo "Please run 'make fmt' and commit the result:"; \
    echo "$${diff}"; \
    exit 1; \
  fi;
