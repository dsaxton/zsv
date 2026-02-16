set shell := ["bash", "-euo", "pipefail", "-c"]

test:
    zig build test

test-unit:
    zig test src/main.zig

test-e2e:
    zig build test-e2e
