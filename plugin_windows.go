//go:build windows

package plugo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// Open prepares and launches a preconfigured *exec.Cmd instance on Windows.
// It uses syscall.CreatePipe to create anonymous pipes, passes the inheritable handles
// to the subprocess via command line arguments, and waits for the subprocess to respond.
func Open(ctx context.Context, pluginPath string, opts ...Option) (*Plugin, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// 1. Create two anonymous pipes for bidirectional communication
	// hostRead/childWrite
	var hr, cw syscall.Handle
	if err := syscall.CreatePipe(&hr, &cw, nil, 0); err != nil {
		return nil, fmt.Errorf("failed to create host-read pipe: %w", err)
	}
	// childRead/hostWrite
	var cr, hw syscall.Handle
	if err := syscall.CreatePipe(&cr, &hw, nil, 0); err != nil {
		_ = syscall.CloseHandle(hr)
		_ = syscall.CloseHandle(cw)
		return nil, fmt.Errorf("failed to create host-write pipe: %w", err)
	}

	// 2. Set child handles as inheritable
	if err := syscall.SetHandleInformation(cr, syscall.HANDLE_FLAG_INHERIT, syscall.HANDLE_FLAG_INHERIT); err != nil {
		_ = syscall.CloseHandle(hr)
		_ = syscall.CloseHandle(cw)
		_ = syscall.CloseHandle(cr)
		_ = syscall.CloseHandle(hw)
		return nil, fmt.Errorf("failed to set child-read handle inheritable: %w", err)
	}
	if err := syscall.SetHandleInformation(cw, syscall.HANDLE_FLAG_INHERIT, syscall.HANDLE_FLAG_INHERIT); err != nil {
		_ = syscall.CloseHandle(hr)
		_ = syscall.CloseHandle(cw)
		_ = syscall.CloseHandle(cr)
		_ = syscall.CloseHandle(hw)
		return nil, fmt.Errorf("failed to set child-write handle inheritable: %w", err)
	}

	// 3. Inject pipe handles as environment variables
	// Also pass via SysProcAttr so Windows passes the handles
	cmd := exec.Command(pluginPath, o.args...)
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), o.env...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("PLUGO_PIPE_READ=%d", cr), fmt.Sprintf("PLUGO_PIPE_WRITE=%d", cw))
	cmd.SysProcAttr = &syscall.SysProcAttr{
		AdditionalInheritedHandles: []syscall.Handle{cr, cw},
	}

	// 4. Start the plugin subprocess
	if err := cmd.Start(); err != nil {
		_ = syscall.CloseHandle(hr)
		_ = syscall.CloseHandle(cw)
		_ = syscall.CloseHandle(cr)
		_ = syscall.CloseHandle(hw)
		return nil, fmt.Errorf("failed to start plugin subprocess: %w", err)
	}

	// 5. Critical: Host must close its copies of the child's handles
	_ = syscall.CloseHandle(cr)
	_ = syscall.CloseHandle(cw)

	// Wrap host handles in os.File
	hostReadFile := os.NewFile(uintptr(hr), "host_read")
	hostWriteFile := os.NewFile(uintptr(hw), "host_write")

	// Create a combined ReadWriteCloser for the host
	hostConn := &pipeConn{
		r: hostReadFile,
		w: hostWriteFile,
	}

	// 6. Negotiate codec
	msgConn, err := negotiateFromHostToPlugin(ctx, hostConn, o.codecs...)
	if err != nil {
		_ = hostConn.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("failed to negotiate codec with plugin: %w", err)
	}

	return NewPlugin(cmd, msgConn), nil
}

// pipeConn combines two os.File pipes into a single io.ReadWriteCloser
type pipeConn struct {
	r *os.File
	w *os.File
}

func (p *pipeConn) Read(b []byte) (n int, err error) {
	return p.r.Read(b)
}

func (p *pipeConn) Write(b []byte) (n int, err error) {
	return p.w.Write(b)
}

func (p *pipeConn) Close() error {
	err1 := p.r.Close()
	err2 := p.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
