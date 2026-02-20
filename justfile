set shell := ["bash", "-euo", "pipefail", "-c"]

build:
    zig build -Doptimize=ReleaseFast

build-debug:
    zig build

test:
    zig build test

test-unit:
    zig test src/main.zig

test-e2e:
    zig build test-e2e
