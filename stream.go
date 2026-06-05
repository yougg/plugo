package plugo

import (
	"context"
	"fmt"
	"io"
	"time"
)

type (
	// StreamMode defines the interaction pattern of a stream
	StreamMode uint8

	// Stream represents an active bidirectional message stream over the physical connection.
	Stream struct {
		id     uint32
		conn   *MessageConn
		recvCh chan *StreamFrame
		ctx    context.Context
		cancel context.CancelFunc
	}

	// StreamFrame represents a multiplexed message stream frame.
	StreamFrame struct {
		StreamID uint32 `json:"stream_id" gob:"stream_id"`
		Flags    uint32 `json:"flags" gob:"flags"` // Flags: 1=Start, 2=Data, 4=Close, 8=Error, 16=CloseWrite
		Payload  []byte `json:"payload,omitempty" gob:"payload,omitempty"`
	}
)

const (
	StreamFlagStart uint32 = 1 << iota
	StreamFlagData
	StreamFlagClose
	StreamFlagError
	StreamFlagCloseWrite
)

const (
	StreamModeBidi        StreamMode = iota // Raw full-duplex stream
	StreamModeSingleMulti                   // Single request, multiple responses
	StreamModeMultiSingle                   // Multiple requests, single response
)

// Send sends a payload over the stream.
func (s *Stream) Send(ctx context.Context, payload []byte) error {
	select {
	case <-s.ctx.Done():
		return fmt.Errorf("stream is closed: %w", s.ctx.Err())
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	frame := StreamFrame{
		StreamID: s.id,
		Flags:    StreamFlagData,
		Payload:  payload,
	}
	return s.conn.writeStreamFrame(ctx, frame)
}

// Recv receives a payload from the stream.
func (s *Stream) Recv(ctx context.Context) ([]byte, error) {
	// Prioritize reading pending data from recvCh to prevent data loss
	// when the stream is quickly closed by the peer and both ctx.Done and recvCh are ready.
	select {
	case frame, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		if frame.Flags&StreamFlagClose != 0 || frame.Flags&StreamFlagCloseWrite != 0 {
			if frame.Flags&StreamFlagClose != 0 {
				s.cancel()
			}
			return nil, io.EOF
		}
		if frame.Flags&StreamFlagError != 0 {
			s.cancel()
			return nil, fmt.Errorf("stream error from peer: %s", string(frame.Payload))
		}
		return frame.Payload, nil
	default:
	}

	select {
	case <-s.ctx.Done():
		// Even if stream context is canceled, check if there's any remaining data
		select {
		case frame, ok := <-s.recvCh:
			if !ok {
				return nil, io.EOF
			}
			if frame.Flags&StreamFlagClose != 0 || frame.Flags&StreamFlagCloseWrite != 0 {
				return nil, io.EOF
			}
			if frame.Flags&StreamFlagError != 0 {
				return nil, fmt.Errorf("stream error from peer: %s", string(frame.Payload))
			}
			return frame.Payload, nil
		default:
			return nil, io.EOF
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		if frame.Flags&StreamFlagClose != 0 || frame.Flags&StreamFlagCloseWrite != 0 {
			if frame.Flags&StreamFlagClose != 0 {
				s.cancel()
			}
			return nil, io.EOF
		}
		if frame.Flags&StreamFlagError != 0 { // Error frame
			s.cancel()
			return nil, fmt.Errorf("stream error from peer: %s", string(frame.Payload))
		}
		return frame.Payload, nil
	}
}

// CloseWrite gracefully closes the write side of the stream, notifying the peer
// that no more data will be sent, but allowing the local read side to remain open.
func (s *Stream) CloseWrite() error {
	frame := StreamFrame{
		StreamID: s.id,
		Flags:    StreamFlagCloseWrite,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.conn.writeStreamFrame(ctx, frame)
}

// Close closes the stream, notifying the peer.
func (s *Stream) Close() error {
	s.cancel()
	frame := StreamFrame{
		StreamID: s.id,
		Flags:    StreamFlagClose,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.conn.writeStreamFrame(ctx, frame)
	s.conn.removeStream(s.id)
	return nil
}

// ID returns the stream ID.
func (s *Stream) ID() uint32 {
	return s.id
}
