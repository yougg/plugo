.SILENT:
.ONESHELL:
.EXPORT_ALL_VARIABLES:
.PHONY: all build build-go build-zig run test clean

log = echo "[$$(date '+%F %T.%3N')]"

export CGO_ENABLED := 0

all: build

build: build-go build-zig

run: build
	$(log) "Running host..."
	./build/host

build-go:
	$(log) "Building all Go components (host & plugins)..."
	$(log) "Building Go target: host..."
	EXE_SUFFIX=$$(go env GOEXE)
	go build -trimpath -ldflags "-s -w" -o build/host$${EXE_SUFFIX} host_go/main.go
	$(log) "Building Go target: plugin_go_json..."
	go build -trimpath -ldflags "-s -w" -o build/plugin_go_json$${EXE_SUFFIX} plugin_go/json/main.go
	$(log) "Building Go target: plugin_go_msgp..."
	go build -trimpath -ldflags "-s -w" -o build/plugin_go_msgp$${EXE_SUFFIX} plugin_go/msgp/main.go
	$(log) "Building Go target: plugin_go_abnormal..."
	go build -trimpath -ldflags "-s -w" -o build/plugin_go_abnormal$${EXE_SUFFIX} plugin_go/abnormal/main.go

build-zig:
	if ! command -v zig &>/dev/null; then
		$(log) "Zig compiler not found, skipping Zig plugins build and execution."
		exit 0
	fi
	EXAM_ROOT=$${PWD}
	mkdir -p $${EXAM_ROOT}/build
	
	TARGET_OS=$$(go env GOOS)
	TARGET_ARCH=$$(go env GOARCH)
	EXE_SUFFIX=$$(go env GOEXE)

	case "$${TARGET_ARCH}" in
		amd64) ZIG_ARCH="x86_64" ;;
		arm64) ZIG_ARCH="aarch64" ;;
		386) ZIG_ARCH="x86" ;;
		*) ZIG_ARCH="$${TARGET_ARCH}" ;;
	esac
	case "$${TARGET_OS}" in
		darwin) ZIG_OS="macos" ;;
		*) ZIG_OS="$${TARGET_OS}" ;;
	esac
	ZIG_TARGET="-Dtarget=$${ZIG_ARCH}-$${ZIG_OS}"

	$(log) "Building Zig Msgpack plugin with target: $${ZIG_TARGET}..."
	cd $${EXAM_ROOT}/plugin_zig/msgp
	zig build $${ZIG_TARGET}
	mv zig-out/bin/plugin_zig_msgp$${EXE_SUFFIX} $${EXAM_ROOT}/build/
	
	$(log) "Building Zig JSON plugin with target: $${ZIG_TARGET}..."
	cd $${EXAM_ROOT}/plugin_zig/json
	zig build $${ZIG_TARGET}
	mv zig-out/bin/plugin_zig_json$${EXE_SUFFIX} $${EXAM_ROOT}/build/
	$(log) "Zig plugins build complete."

test:
	$(log) "Running Go tests across repository..."
	EXAM_ROOT=$${PWD}
	cd $${EXAM_ROOT}/..
	go test -v ./...

	if ! command -v zig &>/dev/null; then
		$(log) "Zig compiler not found, skipping Zig unit tests."
	fi
	$(log) "Running Zig plugin unit tests..."
	cd $${EXAM_ROOT}/plugin_zig/msgp
	zig build test
	cd $${EXAM_ROOT}/plugin_zig/json
	zig build test
	$(log) "Zig plugin unit tests complete."

clean:
	$(log) "Cleaning all build binaries and caches..."
	rm -rf build
	find . -type d \( -name zig-out -o -name zig-pkg -o -name .zig-cache \) -exec rm -rf {} +
	$(log) "Cleanup complete."
