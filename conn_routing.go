package plugo

import (
	"context"
	"fmt"
	"io"
)

// HandleSingleMulti registers a handler for the SingleMulti stream pattern on the specified route.
func (m *MessageConn) HandleSingleMulti(route string, handler func(req []byte, sender func([]byte) error) error) {
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	m.singleMultiHandlers[route] = handler
}

// HandleMultiSingle registers a handler for the MultiSingle stream pattern on the specified route.
func (m *MessageConn) HandleMultiSingle(route string, handler func(reqs <-chan []byte) ([]byte, error)) {
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	m.multiSingleHandlers[route] = handler
}

// HandleBidi registers a handler for raw full-duplex stream on the specified route.
func (m *MessageConn) HandleBidi(route string, handler func(s *Stream)) {
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	m.bidiHandlers[route] = handler
}

// startStream creates a new stream and sends the StreamFlagStart frame with the encoded header.
func (m *MessageConn) startStream(ctx context.Context, route string, mode StreamMode) (*Stream, error) {
	// Auto-increment by 2 to maintain odd/even IDs, preventing collision between endpoints
	m.streamsMu.Lock()
	if m.readLoopErr != nil {
		err := m.readLoopErr
		m.streamsMu.Unlock()
		return nil, fmt.Errorf("read loop is not running due to error: %w", err)
	}
	streamID := m.nextStreamID
	m.nextStreamID += 2
	m.streamsMu.Unlock()
	payload := make([]byte, 1+len(route))
	payload[0] = byte(mode)
	copy(payload[1:], route)
	m.streamsMu.Lock()
	s := &Stream{
		id:     streamID,
		conn:   m,
		recvCh: make(chan *StreamFrame, 100),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	m.streams[streamID] = s
	m.streamsMu.Unlock()

	frame := StreamFrame{
		StreamID: streamID,
		Flags:    StreamFlagStart,
		Payload:  payload,
	}

	if err := m.writeStreamFrame(ctx, frame); err != nil {
		m.removeStream(streamID)
		return nil, err
	}
	return s, nil
}

// CallSingleMulti calls a SingleMulti route. It sends a single request and returns a channel that yields multiple responses.
func (m *MessageConn) CallSingleMulti(ctx context.Context, route string, req []byte) (<-chan []byte, error) {
	s, err := m.startStream(ctx, route, StreamModeSingleMulti)
	if err != nil {
		return nil, err
	}

	if err := s.Send(ctx, req); err != nil {
		s.Close()
		return nil, err
	}
	if err := s.CloseWrite(); err != nil {
		s.Close()
		return nil, err
	}

	respCh := make(chan []byte, 10)
	go func() {
		defer s.Close()
		defer close(respCh)
		for {
			resp, err := s.Recv(ctx)
			if err != nil {
				return
			}
			select {
			case respCh <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()
	return respCh, nil
}

// CallMultiSingle calls a MultiSingle route. It sends multiple requests from the provided channel and waits for a single response.
func (m *MessageConn) CallMultiSingle(ctx context.Context, route string, reqs <-chan []byte) ([]byte, error) {
	s, err := m.startStream(ctx, route, StreamModeMultiSingle)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case req, ok := <-reqs:
				if !ok {
					_ = s.CloseWrite()
					return
				}
				if err := s.Send(ctx, req); err != nil {
					s.Close()
					return
				}
			case <-ctx.Done():
				s.Close()
				return
			}
		}
	}()

	defer s.Close()
	resp, err := s.Recv(ctx)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return resp, nil
}

// CallBidi calls a bidirectional route and returns the raw stream.
func (m *MessageConn) CallBidi(ctx context.Context, route string) (*Stream, error) {
	return m.startStream(ctx, route, StreamModeBidi)
}
