FUZZTIME ?= 30s
FUZZ_TARGETS = FuzzDecode FuzzStreamReader FuzzStreamResync FuzzFrameRoundTrip

.PHONY: all build test race fuzz bench demo clean

all: test build

build:
	go build -o bin/ ./cmd/...

test:
	go vet ./...
	go test ./...

race:
	go test -race -count=1 ./...

fuzz:
	@for t in $(FUZZ_TARGETS); do \
		echo "== $$t"; \
		go test ./protocol -run='^$$' -fuzz="^$$t$$" -fuzztime=$(FUZZTIME) || exit 1; \
	done
	go test ./internal/bitio -run='^$$' -fuzz='^FuzzRoundTrip$$' -fuzztime=$(FUZZTIME)

bench:
	go test ./protocol -run='^$$' -bench=. -benchmem

clean:
	rm -rf bin
