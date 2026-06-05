package plugo

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
)

// Attaching initializes the communication channel with the host process in the plugin subprocess
// and performs codec negotiation.
// This function abstracts platform differences:
//   - On Windows, it looks for --plugo-pipe-read and --plugo-pipe-write command line arguments,
//     wrapping the passed inherited handles into a single io.ReadWriteCloser.
//   - On Unix/macOS systems, it directly takes over and wraps the inherited file descriptor 3 (FD 3).
func Attaching(ctx context.Context, codecs ...Codec) (*MessageConn, error) {
	var rwc io.ReadWriteCloser

	// 1. Detect if inherited pipe handles are passed via environment variables (Windows)
	var readHandle, writeHandle uintptr
	var hasWindowsPipes bool

	if readStr := os.Getenv("PLUGO_PIPE_READ"); readStr != "" {
		if val, err := strconv.ParseUint(readStr, 10, 64); err == nil {
			readHandle = uintptr(val)
			hasWindowsPipes = true
		}
	}
	if writeStr := os.Getenv("PLUGO_PIPE_WRITE"); writeStr != "" {
		if val, err := strconv.ParseUint(writeStr, 10, 64); err == nil {
			writeHandle = uintptr(val)
			hasWindowsPipes = true
		}
	}

	if hasWindowsPipes {
		// On Windows, wrap the two pipe handles into Go file objects
		if readHandle == 0 || writeHandle == 0 {
			return nil, fmt.Errorf("missing one of the pipe handles, both read and write handles are required")
		}
		readFile := os.NewFile(readHandle, "plugin_read")
		writeFile := os.NewFile(writeHandle, "plugin_write")
		rwc = &pipeClientConn{r: readFile, w: writeFile}
	} else {
		// 2. Otherwise, in Unix platforms, directly inherit and wrap descriptor 3
		sock := os.NewFile(3, "host_sock")
		if sock == nil {
			return nil, fmt.Errorf("inherited file descriptor FD 3 not found, plugin must be spawned by a plugo-compliant host process")
		}
		rwc = sock
	}

	// 3. Perform codec negotiation
	msgConn, err := negotiateFromPluginToHost(ctx, rwc, codecs...)
	if err != nil {
		_ = rwc.Close()
		return nil, fmt.Errorf("failed to negotiate codec with host: %w", err)
	}

	return msgConn, nil
}

// pipeClientConn combines two os.File pipes into a single io.ReadWriteCloser
type pipeClientConn struct {
	r *os.File
	w *os.File
}

func (p *pipeClientConn) Read(b []byte) (n int, err error) {
	return p.r.Read(b)
}

func (p *pipeClientConn) Write(b []byte) (n int, err error) {
	return p.w.Write(b)
}

func (p *pipeClientConn) Close() error {
	err1 := p.r.Close()
	err2 := p.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
