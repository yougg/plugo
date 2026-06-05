package plugo

import (
	"errors"
	"os/exec"
)

// Option is a function that configures the plugin initialization.
type Option func(*options)

type options struct {
	args   []string
	env    []string
	codecs []Codec
}

// WithArgs specifies command-line arguments to pass to the plugin.
func WithArgs(args ...string) Option {
	return func(o *options) {
		o.args = append(o.args, args...)
	}
}

// WithEnv specifies environment variables to pass to the plugin.
// Each entry should be in the form "KEY=VALUE".
func WithEnv(env ...string) Option {
	return func(o *options) {
		o.env = append(o.env, env...)
	}
}

// WithCodec specifies codecs to use for negotiating with the plugin.
func WithCodec(codecs ...Codec) Option {
	return func(o *options) {
		o.codecs = append(o.codecs, codecs...)
	}
}

// Plugin represents a running instance of a plugin subprocess.
// It holds the communication connection on the host side and the exec.Cmd used to launch the plugin,
// coordinating lifecycle management and resource cleanup.
type Plugin struct {
	cmd  *exec.Cmd
	conn *MessageConn
}

// NewPlugin constructs a Plugin management instance.
func NewPlugin(cmd *exec.Cmd, conn *MessageConn) *Plugin {
	return &Plugin{
		cmd:  cmd,
		conn: conn,
	}
}

// Conn returns the communication channel *MessageConn for this plugin instance.
func (p *Plugin) Conn() *MessageConn {
	return p.conn
}

// Wait blocks waiting for the plugin subprocess to exit, returning exit errors if any.
func (p *Plugin) Wait() error {
	return p.cmd.Wait()
}

// Close closes the communication channel and cleanly shuts down or kills the plugin subprocess,
// releasing all system resources.
func (p *Plugin) Close() error {
	var errs []error

	// 1. Close communication stream. Subprocess should shut down gracefully on EOF.
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// 2. Kill the subprocess if it's still running to prevent leaks
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}

	// 3. Reap the subprocess to release the system process slot completely
	if p.cmd != nil {
		if err := p.cmd.Wait(); err != nil {
			if _, ok := errors.AsType[*exec.ExitError](err); !ok {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Cmd returns the underlying *exec.Cmd handle, useful for process ID queries or status checks.
func (p *Plugin) Cmd() *exec.Cmd {
	return p.cmd
}
