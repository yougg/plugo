package plugo

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"
)

type (
	// handshakeRequest is the negotiation request sent from host to plugin.
	handshakeRequest struct {
		Supported []string `json:"supported"` // List of supported codecs by host
	}

	// handshakeResponse is the negotiation response sent from plugin to host.
	handshakeResponse struct {
		Selected string `json:"selected"` // Selected codec name
	}
)

// withContext wraps a blocking connection action with cancellation and timeout via context.
func withContext(ctx context.Context, rwc io.ReadWriteCloser, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dl, hasDeadline := rwc.(deadliner)
	if hasDeadline {
		// Reset deadline first to avoid leftover deadlines from previous calls affecting this operation
		_ = dl.SetDeadline(time.Time{})
		if d, ok := ctx.Deadline(); ok {
			_ = dl.SetDeadline(d)
		}
	}

	if ctx.Done() == nil {
		defer func() {
			if hasDeadline {
				_ = dl.SetDeadline(time.Time{})
			}
		}()
		return fn()
	}

	done := make(chan struct{})
	// exited is used to ensure the goroutine exits before clearing the deadline, avoiding race conditions
	exited := make(chan struct{})

	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			if hasDeadline {
				_ = dl.SetDeadline(time.Now())
			} else {
				_ = rwc.Close()
			}
		case <-done:
		}
	}()

	err := fn()
	close(done)
	// Wait for the goroutine to exit, ensuring its SetDeadline call has completed, before clearing the deadline
	<-exited
	if hasDeadline {
		_ = dl.SetDeadline(time.Time{})
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

// negotiateFromHostToPlugin performs codec negotiation on the host side.
// It sends the supported codecs to the plugin and waits for confirmation.
// Upon successful negotiation, it returns a MessageConn instance configured with the selected codec.
func negotiateFromHostToPlugin(ctx context.Context, rwc io.ReadWriteCloser, codecs ...Codec) (*MessageConn, error) {
	if len(codecs) == 0 {
		return nil, fmt.Errorf("host supported codec list cannot be empty")
	}

	// 1. Build supported codec names and look-up map
	supportedNames := make([]string, 0, len(codecs))
	codecMap := make(map[string]Codec)
	for _, c := range codecs {
		supportedNames = append(supportedNames, c.Name())
		codecMap[c.Name()] = c
	}

	// 2. Send handshake request (4-byte big-endian length + JSON payload)
	req := handshakeRequest{Supported: supportedNames}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal handshake request: %w", err)
	}

	reqLen := uint32(len(reqData))
	buf := make([]byte, 4+reqLen)
	binary.BigEndian.PutUint32(buf[0:4], reqLen)
	copy(buf[4:], reqData)

	if err = withContext(ctx, rwc, func() error {
		_, err = rwc.Write(buf)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to send handshake request: %w", err)
	}

	// 3. Read handshake response (4-byte big-endian length + JSON payload)
	lenBuf := make([]byte, 4)
	if err = withContext(ctx, rwc, func() error {
		_, err = io.ReadFull(rwc, lenBuf)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read handshake response length: %w", err)
	}

	respLen := binary.BigEndian.Uint32(lenBuf)
	if respLen == 0 {
		return nil, fmt.Errorf("received invalid zero-length handshake response frame")
	}

	respData := make([]byte, respLen)
	if err = withContext(ctx, rwc, func() error {
		_, err = io.ReadFull(rwc, respData)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read handshake response payload: %w", err)
	}

	var resp handshakeResponse
	if err = json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal handshake response: %w", err)
	}

	// 4. Match and verify the selected codec
	selectedCodec, exists := codecMap[resp.Selected]
	if !exists {
		return nil, fmt.Errorf("plugin selected an unsupported codec: %s", resp.Selected)
	}

	msgConn := NewMessageConn(rwc, selectedCodec, true)
	msgConn.startReadLoop()
	return msgConn, nil
}

// negotiateFromPluginToHost performs codec negotiation on the plugin side.
// It blocks to read the supported codecs from the host, matches the best mutual codec,
// responds with confirmation, and returns the upgraded MessageConn.
func negotiateFromPluginToHost(ctx context.Context, rwc io.ReadWriteCloser, codecs ...Codec) (*MessageConn, error) {
	if len(codecs) == 0 {
		return nil, fmt.Errorf("plugin supported codec list cannot be empty")
	}

	// 1. Read handshake request (4-byte big-endian length + JSON payload)
	lenBuf := make([]byte, 4)
	if err := withContext(ctx, rwc, func() error {
		_, err := io.ReadFull(rwc, lenBuf)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read handshake request length: %w", err)
	}

	reqLen := binary.BigEndian.Uint32(lenBuf)
	if reqLen == 0 {
		return nil, fmt.Errorf("received invalid zero-length handshake request frame")
	}

	reqData := make([]byte, reqLen)
	if err := withContext(ctx, rwc, func() error {
		_, err := io.ReadFull(rwc, reqData)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read handshake request payload: %w", err)
	}

	var req handshakeRequest
	if err := json.Unmarshal(reqData, &req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal handshake request: %w", err)
	}

	// 2. Match the best mutual codec (based on plugin precedence)
	var selectedCodec Codec
	for _, clientCodec := range codecs {
		if slices.Contains(req.Supported, clientCodec.Name()) {
			selectedCodec = clientCodec
		}
		if selectedCodec != nil {
			break
		}
	}

	if selectedCodec == nil {
		return nil, fmt.Errorf("no mutual protocol codec: host supports %v, plugin supports %v", req.Supported, codecs)
	}

	// 3. Send response confirmation to the host
	resp := handshakeResponse{Selected: selectedCodec.Name()}
	respData, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal handshake response: %w", err)
	}

	respLen := uint32(len(respData))
	buf := make([]byte, 4+respLen)
	binary.BigEndian.PutUint32(buf[0:4], respLen)
	copy(buf[4:], respData)

	if err = withContext(ctx, rwc, func() error {
		_, err = rwc.Write(buf)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to send handshake response: %w", err)
	}

	msgConn := NewMessageConn(rwc, selectedCodec, false)
	msgConn.startReadLoop()
	return msgConn, nil
}
