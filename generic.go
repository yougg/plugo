package plugo

import (
	"context"
	"fmt"
)

// ReadMessage reads a complete message frame from the connection and unmarshals it into a new instance of T.
func ReadMessage[T any](ctx context.Context, conn *MessageConn) (T, error) {
	var v T
	err := conn.ReadMessage(ctx, &v)
	return v, err
}

// WriteMessage serializes a value of type T and writes it to the connection.
func WriteMessage[T any](ctx context.Context, conn *MessageConn, msg T) error {
	return conn.WriteMessage(ctx, &msg)
}

// TypedStream is a generic wrapper around Stream that provides strongly typed Send and Recv methods.
type TypedStream[Req, Resp any] struct {
	stream *Stream
	conn   *MessageConn
}

// Send sends a typed payload over the stream.
func (s *TypedStream[Req, Resp]) Send(ctx context.Context, req Req) error {
	data, err := s.conn.Codec().Marshal(&req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}
	return s.stream.Send(ctx, data)
}

// Recv receives a typed payload from the stream.
func (s *TypedStream[Req, Resp]) Recv(ctx context.Context) (Resp, error) {
	var resp Resp
	data, err := s.stream.Recv(ctx)
	if err != nil {
		return resp, err
	}
	if err := s.conn.Codec().Unmarshal(data, &resp); err != nil {
		return resp, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return resp, nil
}

// CloseWrite gracefully closes the write side of the stream.
func (s *TypedStream[Req, Resp]) CloseWrite() error {
	return s.stream.CloseWrite()
}

// Close closes the stream, notifying the peer.
func (s *TypedStream[Req, Resp]) Close() error {
	return s.stream.Close()
}

// ID returns the stream ID.
func (s *TypedStream[Req, Resp]) ID() uint32 {
	return s.stream.ID()
}

// Route defines a strongly-typed routing endpoint bound to a specific MessageConn.
type Route[Req, Resp any] struct {
	conn  *MessageConn
	route string
}

// NewRoute creates a new typed route.
func NewRoute[Req, Resp any](conn *MessageConn, route string) *Route[Req, Resp] {
	return &Route[Req, Resp]{
		conn:  conn,
		route: route,
	}
}

// CallSingleMulti calls a SingleMulti route. It sends a single request and returns a channel that yields multiple responses.
func (r *Route[Req, Resp]) CallSingleMulti(ctx context.Context, req Req) (<-chan Resp, error) {
	reqBytes, err := r.conn.Codec().Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	rawRespCh, err := r.conn.CallSingleMulti(ctx, r.route, reqBytes)
	if err != nil {
		return nil, err
	}

	respCh := make(chan Resp, 10)
	go func() {
		defer close(respCh)
		for rawResp := range rawRespCh {
			var resp Resp
			if err := r.conn.Codec().Unmarshal(rawResp, &resp); err != nil {
				// We drop messages that fail to unmarshal.
				// In a full implementation, error reporting mechanisms can be extended.
				continue
			}
			respCh <- resp
		}
	}()
	return respCh, nil
}

// CallMultiSingle calls a MultiSingle route. It sends multiple requests from the provided channel and waits for a single response.
func (r *Route[Req, Resp]) CallMultiSingle(ctx context.Context, reqs <-chan Req) (Resp, error) {
	var zero Resp
	rawReqsCh := make(chan []byte, 10)

	go func() {
		defer close(rawReqsCh)
		for req := range reqs {
			reqBytes, err := r.conn.Codec().Marshal(&req)
			if err != nil {
				// Failed to marshal a request. We can either skip or close.
				continue
			}
			rawReqsCh <- reqBytes
		}
	}()

	rawResp, err := r.conn.CallMultiSingle(ctx, r.route, rawReqsCh)
	if err != nil {
		return zero, err
	}

	var resp Resp
	if err := r.conn.Codec().Unmarshal(rawResp, &resp); err != nil {
		return zero, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return resp, nil
}

// CallBidi calls a bidirectional route and returns a strongly-typed stream.
func (r *Route[Req, Resp]) CallBidi(ctx context.Context) (*TypedStream[Req, Resp], error) {
	stream, err := r.conn.CallBidi(ctx, r.route)
	if err != nil {
		return nil, err
	}
	return &TypedStream[Req, Resp]{
		stream: stream,
		conn:   r.conn,
	}, nil
}

// HandleSingleMulti registers a handler for the SingleMulti stream pattern on the specified route.
func (r *Route[Req, Resp]) HandleSingleMulti(handler func(req Req, sender func(Resp) error) error) {
	r.conn.HandleSingleMulti(r.route, func(rawReq []byte, sender func([]byte) error) error {
		var req Req
		if err := r.conn.Codec().Unmarshal(rawReq, &req); err != nil {
			return fmt.Errorf("failed to unmarshal request: %w", err)
		}

		return handler(req, func(resp Resp) error {
			respBytes, err := r.conn.Codec().Marshal(&resp)
			if err != nil {
				return fmt.Errorf("failed to marshal response: %w", err)
			}
			return sender(respBytes)
		})
	})
}

// HandleMultiSingle registers a handler for the MultiSingle stream pattern on the specified route.
func (r *Route[Req, Resp]) HandleMultiSingle(handler func(reqs <-chan Req) (Resp, error)) {
	r.conn.HandleMultiSingle(r.route, func(rawReqs <-chan []byte) ([]byte, error) {
		reqsCh := make(chan Req, 10)
		go func() {
			defer close(reqsCh)
			for rawReq := range rawReqs {
				var req Req
				if err := r.conn.Codec().Unmarshal(rawReq, &req); err == nil {
					reqsCh <- req
				}
			}
		}()

		resp, err := handler(reqsCh)
		if err != nil {
			return nil, err
		}

		respBytes, marshalErr := r.conn.Codec().Marshal(&resp)
		if marshalErr != nil {
			return nil, fmt.Errorf("failed to marshal response: %w", marshalErr)
		}
		return respBytes, nil
	})
}

// HandleBidi registers a handler for a strongly-typed full-duplex stream on the specified route.
func (r *Route[Req, Resp]) HandleBidi(handler func(s *TypedStream[Resp, Req])) {
	r.conn.HandleBidi(r.route, func(s *Stream) {
		ts := &TypedStream[Resp, Req]{
			stream: s,
			conn:   r.conn,
		}
		handler(ts)
	})
}
