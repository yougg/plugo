package plugo

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

type deadliner interface {
	SetDeadline(t time.Time) error
}

// MessageConn encapsulates the underlying communication channel, providing message-based
// framed reads and writes, resolving packet sticking and fragmentation automatically.
type MessageConn struct {
	rwc   io.ReadWriteCloser
	codec Codec
	wMu   sync.Mutex // Protects write stream from interleaved data

	// Stream multiplexing fields
	streamsMu       sync.RWMutex
	streams         map[uint32]*Stream
	acceptCh        chan *Stream
	rawMsgCh        chan []byte
	readLoopOnce    sync.Once
	readLoopStarted bool
	readLoopErr     error
	readLoopDone    chan struct{}

	nextStreamID uint32

	routeMu             sync.RWMutex
	singleMultiHandlers map[string]func(req []byte, sender func([]byte) error) error
	multiSingleHandlers map[string]func(reqs <-chan []byte) ([]byte, error)
	bidiHandlers        map[string]func(s *Stream)
}

// NewMessageConn wraps an underlying io.ReadWriteCloser with a specified codec.
func NewMessageConn(rwc io.ReadWriteCloser, codec Codec, isHost bool) *MessageConn {
	initialID := uint32(2)
	if isHost {
		initialID = 1
	}
	return &MessageConn{
		rwc:                 rwc,
		codec:               codec,
		streams:             make(map[uint32]*Stream),
		acceptCh:            make(chan *Stream, 100),
		rawMsgCh:            make(chan []byte, 100),
		readLoopDone:        make(chan struct{}),
		nextStreamID:        initialID,
		singleMultiHandlers: make(map[string]func(req []byte, sender func([]byte) error) error),
		multiSingleHandlers: make(map[string]func(reqs <-chan []byte) ([]byte, error)),
		bidiHandlers:        make(map[string]func(s *Stream)),
	}
}

// Close closes the underlying connection channel.
func (m *MessageConn) Close() error {
	m.streamsMu.Lock()
	if m.readLoopStarted {
		for _, s := range m.streams {
			s.cancel()
		}
	}
	m.streamsMu.Unlock()
	return m.rwc.Close()
}

// withContext handles context cancellation and timeout for blocking read/write calls.
func (m *MessageConn) withContext(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dl, hasDeadline := m.rwc.(deadliner)
	if hasDeadline {
		if d, ok := ctx.Deadline(); ok {
			_ = dl.SetDeadline(d)
			defer dl.SetDeadline(time.Time{})
		}
	}

	if ctx.Done() == nil {
		return fn()
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			if hasDeadline {
				_ = dl.SetDeadline(time.Now())
			} else {
				_ = m.rwc.Close()
			}
		case <-done:
		}
	}()

	err := fn()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

// WriteMessage sends a structured message.
// It serializes the message using the specified codec, prefixes it with a 12-byte big-endian
// header (Length, StreamID=0, Flags=0), and writes it atomically to the underlying channel.
func (m *MessageConn) WriteMessage(ctx context.Context, v any) error {
	data, err := m.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	return m.WriteData(ctx, data)
}

// WriteData sends raw byte data directly.
// It prefixes the data with a 12-byte big-endian header (Length, StreamID=0, Flags=0)
// and writes it atomically to the underlying channel without codec serialization.
func (m *MessageConn) WriteData(ctx context.Context, data []byte) error {
	length := uint32(len(data))
	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], 0)  // StreamID = 0
	binary.BigEndian.PutUint32(buf[8:12], 0) // Flags = 0
	copy(buf[12:], data)

	return m.withContext(ctx, func() error {
		m.wMu.Lock()
		defer m.wMu.Unlock()

		_, err := m.rwc.Write(buf)
		if err != nil {
			return fmt.Errorf("failed to write to connection: %w", err)
		}
		return nil
	})
}

// writeStreamFrame directly transmits a multiplexed stream frame with the 12-byte header.
func (m *MessageConn) writeStreamFrame(ctx context.Context, frame StreamFrame) error {
	length := uint32(len(frame.Payload))
	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], frame.StreamID)
	binary.BigEndian.PutUint32(buf[8:12], frame.Flags)
	copy(buf[12:], frame.Payload)

	return m.withContext(ctx, func() error {
		m.wMu.Lock()
		defer m.wMu.Unlock()

		_, err := m.rwc.Write(buf)
		if err != nil {
			return fmt.Errorf("failed to write stream frame: %w", err)
		}
		return nil
	})
}

// ReadMessage reads a complete message frame and deserializes it.
// If the multiplexed read loop is running, it consumes from the rawMsgCh; otherwise, it reads synchronously.
func (m *MessageConn) ReadMessage(ctx context.Context, v any) error {
	payload, err := m.ReadData(ctx)
	if err != nil {
		return err
	}
	if err := m.codec.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return nil
}

// ReadData reads a complete raw data frame.
// If the multiplexed read loop is running, it consumes from the rawMsgCh; otherwise, it reads synchronously.
func (m *MessageConn) ReadData(ctx context.Context) ([]byte, error) {
	m.streamsMu.RLock()
	loopStarted := m.readLoopStarted
	m.streamsMu.RUnlock()

	if loopStarted {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case data, ok := <-m.rawMsgCh:
			if !ok {
				m.streamsMu.RLock()
				err := m.readLoopErr
				m.streamsMu.RUnlock()
				if err != nil {
					return nil, err
				}
				return nil, io.EOF
			}
			return data, nil
		}
	}

	var result []byte
	err := m.withContext(ctx, func() error {
		header := make([]byte, 12)
		if _, err := io.ReadFull(m.rwc, header); err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(header[0:4])
		streamID := binary.BigEndian.Uint32(header[4:8])
		flags := binary.BigEndian.Uint32(header[8:12])

		if streamID != 0 {
			if length > 0 {
				discard := make([]byte, length)
				_, _ = io.ReadFull(m.rwc, discard)
			}
			return fmt.Errorf("received stream frame (ID %d) in non-multiplexed synchronous mode", streamID)
		}

		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(m.rwc, payload); err != nil {
				return fmt.Errorf("failed to read payload (expected %d bytes): %w", length, err)
			}
		}

		if flags&StreamFlagError != 0 {
			return fmt.Errorf("connection error frame received: %s", string(payload))
		}

		result = payload
		return nil
	})
	return result, err
}

// CreateStream creates a raw unrouted multiplexed bidirectional stream.
// It is recommended to use CallBidi, CallSingleMulti, or CallMultiSingle for routed streams.
func (m *MessageConn) CreateStream(ctx context.Context) (*Stream, error) {
	return m.startStream(ctx, "", StreamModeBidi)
}

// AcceptStream accepts an incoming multiplexed stream.
func (m *MessageConn) AcceptStream(ctx context.Context) (*Stream, error) {
	m.startReadLoop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s, ok := <-m.acceptCh:
		if !ok {
			m.streamsMu.RLock()
			err := m.readLoopErr
			m.streamsMu.RUnlock()
			if err != nil {
				return nil, fmt.Errorf("read loop error: %w", err)
			}
			return nil, io.EOF
		}
		return s, nil
	}
}

// startReadLoop safely starts the background read loop.
func (m *MessageConn) startReadLoop() {
	m.readLoopOnce.Do(func() {
		m.streamsMu.Lock()
		m.readLoopStarted = true
		m.streamsMu.Unlock()
		go m.readLoop()
	})
}

// readLoop is the background routine for receiving and dispatching multiplexed stream frames.
func (m *MessageConn) readLoop() {
	defer func() {
		close(m.readLoopDone)
		close(m.rawMsgCh)
	}()

	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(m.rwc, header)
		if err != nil {
			m.handleReadLoopError(err)
			return
		}

		length := binary.BigEndian.Uint32(header[0:4])
		streamID := binary.BigEndian.Uint32(header[4:8])
		flags := binary.BigEndian.Uint32(header[8:12])

		payload := make([]byte, length)
		if length > 0 {
			_, err = io.ReadFull(m.rwc, payload)
			if err != nil {
				m.handleReadLoopError(fmt.Errorf("failed to read stream frame payload: %w", err))
				return
			}
		}

		if streamID == 0 {
			select {
			case <-m.readLoopDone:
				return
			case m.rawMsgCh <- payload:
			}
			continue
		}

		m.streamsMu.Lock()
		s, exists := m.streams[streamID]
		if !exists {
			if flags&StreamFlagStart != 0 { // Start frame
				var mode StreamMode
				var route string
				if len(payload) >= 1 {
					mode = StreamMode(payload[0])
					route = string(payload[1:])
				}

				s = &Stream{
					id:     streamID,
					conn:   m,
					recvCh: make(chan *StreamFrame, 100),
				}
				s.ctx, s.cancel = context.WithCancel(context.Background())
				m.streams[streamID] = s
				m.streamsMu.Unlock()

				routed := false
				m.routeMu.RLock()
				switch mode {
				case StreamModeSingleMulti:
					if handler, ok := m.singleMultiHandlers[route]; ok {
						routed = true
						go func(s *Stream, h func(req []byte, sender func([]byte) error) error) {
							defer s.Close()
							req, err := s.Recv(s.ctx)
							if err != nil {
								return
							}
							_ = h(req, func(resp []byte) error {
								return s.Send(s.ctx, resp)
							})
						}(s, handler)
					}
				case StreamModeMultiSingle:
					if handler, ok := m.multiSingleHandlers[route]; ok {
						routed = true
						go func(s *Stream, h func(reqs <-chan []byte) ([]byte, error)) {
							defer s.Close()
							reqsCh := make(chan []byte, 10)
							go func() {
								defer close(reqsCh)
								for {
									req, err := s.Recv(s.ctx)
									if err != nil {
										return
									}
									reqsCh <- req
								}
							}()
							resp, err := h(reqsCh)
							if err == nil {
								_ = s.Send(s.ctx, resp)
								_ = s.CloseWrite()
							}
						}(s, handler)
					}
				case StreamModeBidi:
					if handler, ok := m.bidiHandlers[route]; ok {
						routed = true
						go func(s *Stream, h func(s *Stream)) {
							h(s)
						}(s, handler)
					}
				}
				m.routeMu.RUnlock()

				// If no route matched, send to acceptCh (for raw unrouted streams like CreateStream "")
				if !routed {
					select {
					case <-m.readLoopDone:
					case m.acceptCh <- s:
					}
				}
				m.streamsMu.Lock()
			} else {
				m.streamsMu.Unlock()
				// Send error back: stream not found
				errFrame := StreamFrame{
					StreamID: streamID,
					Flags:    StreamFlagError,
					Payload:  []byte("stream not found"),
				}
				_ = m.writeStreamFrame(context.Background(), errFrame)
				m.streamsMu.Lock()
			}
		} else {
			frame := &StreamFrame{
				StreamID: streamID,
				Flags:    flags,
				Payload:  payload,
			}
			if flags&StreamFlagClose != 0 { // Close frame
				s.cancel()
				close(s.recvCh)
				delete(m.streams, streamID)
			} else if flags&StreamFlagError != 0 { // Error frame
				s.cancel()
				m.streamsMu.Unlock()
				select {
				case <-s.ctx.Done():
				case s.recvCh <- frame:
				}
				m.streamsMu.Lock()
				delete(m.streams, streamID)
			} else {
				m.streamsMu.Unlock()
				select {
				case <-s.ctx.Done():
				case s.recvCh <- frame:
				}
				m.streamsMu.Lock()
			}
		}
		m.streamsMu.Unlock()
	}
}

func (m *MessageConn) handleReadLoopError(err error) {
	m.streamsMu.Lock()
	m.readLoopErr = err
	for _, s := range m.streams {
		s.cancel()
		close(s.recvCh)
	}
	m.streams = make(map[uint32]*Stream)
	m.streamsMu.Unlock()
	close(m.acceptCh)
}

// removeStream deletes a stream from active streams.
func (m *MessageConn) removeStream(id uint32) {
	m.streamsMu.Lock()
	delete(m.streams, id)
	m.streamsMu.Unlock()
}

// UnderlyingConn returns the wrapped io.ReadWriteCloser connection handle.
func (m *MessageConn) UnderlyingConn() io.ReadWriteCloser {
	return m.rwc
}

// Codec returns the current codec used by this connection.
func (m *MessageConn) Codec() Codec {
	return m.codec
}
