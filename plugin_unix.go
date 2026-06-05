//go:build !windows

package plugo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// Open prepares and launches a preconfigured *exec.Cmd instance on Unix systems.
// It sets up a socket pair using syscall.Socketpair and passes the child end in ExtraFiles,
// returns a Plugin instance that manages the connection and process after negotiating the codec.
func Open(ctx context.Context, pluginPath string, opts ...Option) (*Plugin, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// 1. Create a bidirectional Unix domain socket pair (SOCK_STREAM)
	// We MUST use SOCK_CLOEXEC to prevent file descriptors from leaking into other concurrently spawned plugins.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create socket pair: %w", err)
	}

	// 2. Wrap descriptors as Go file objects
	hostFd := os.NewFile(uintptr(fds[0]), "host_sock")
	childFd := os.NewFile(uintptr(fds[1]), "child_sock")

	// 3. Attach childFd to child process's ExtraFiles
	// In Go's exec.Cmd model, ExtraFiles[0] maps to FD 3 inside the subprocess
	cmd := exec.Command(pluginPath, o.args...)
	cmd.Stderr = os.Stderr
	if len(o.env) > 0 {
		cmd.Env = append(os.Environ(), o.env...)
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, childFd)

	// 4. Start the plugin subprocess
	if err := cmd.Start(); err != nil {
		_ = hostFd.Close()
		_ = childFd.Close()
		return nil, fmt.Errorf("failed to start plugin subprocess: %w", err)
	}

	// 5. Critical: The host must immediately close its reference to childFd!
	// Otherwise, the reference count never reaches zero, and the host won't receive EOF when the plugin exits/crashes.
	_ = childFd.Close()

	// 6. Negotiate codec
	msgConn, err := negotiateFromHostToPlugin(ctx, hostFd, o.codecs...)
	if err != nil {
		_ = hostFd.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("failed to negotiate codec with plugin: %w", err)
	}

	return NewPlugin(cmd, msgConn), nil
}
