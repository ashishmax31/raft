.PHONY: test test-2a test-2b test-2c

test: ARGS=
test-2a: ARGS=2A
test-2b: ARGS=2B
test-2c: ARGS=2C

test test-2a test-2b test-2c:
ifdef RACE
	go test -v -race ./test -run "$(ARGS)"
else
	go test -v ./test -run "$(ARGS)"
endif
