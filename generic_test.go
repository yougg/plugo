package plugo

import (
	"context"
	"net"
	"testing"
	"time"
)

type TestReq struct {
	Msg string
}

type TestResp struct {
	Reply string
}

func TestGenericReadWriteMessage(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	conn1 := NewMessageConn(c1, JSONCodec{}, true)
	conn2 := NewMessageConn(c2, JSONCodec{}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		err := WriteMessage(ctx, conn1, TestReq{Msg: "hello"})
		if err != nil {
			t.Errorf("WriteMessage failed: %v", err)
		}
	}()

	req, err := ReadMessage[TestReq](ctx, conn2)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if req.Msg != "hello" {
		t.Errorf("expected 'hello', got '%s'", req.Msg)
	}
}

func TestGenericRouteSingleMulti(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	conn1 := NewMessageConn(c1, JSONCodec{}, true)
	conn2 := NewMessageConn(c2, JSONCodec{}, false)

	route1 := NewRoute[TestReq, TestResp](conn1, "test_route")
	route2 := NewRoute[TestReq, TestResp](conn2, "test_route")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	route2.HandleSingleMulti(func(req TestReq, sender func(TestResp) error) error {
		if req.Msg != "req1" {
			t.Errorf("expected 'req1', got '%s'", req.Msg)
		}
		sender(TestResp{Reply: "resp1"})
		sender(TestResp{Reply: "resp2"})
		return nil
	})

	go conn1.AcceptStream(ctx) // start read loop on caller
	go conn2.AcceptStream(ctx) // start read loop on peer

	respCh, err := route1.CallSingleMulti(ctx, TestReq{Msg: "req1"})
	if err != nil {
		t.Fatalf("CallSingleMulti failed: %v", err)
	}

	resp1 := <-respCh
	if resp1.Reply != "resp1" {
		t.Errorf("expected 'resp1', got '%s'", resp1.Reply)
	}
	resp2 := <-respCh
	if resp2.Reply != "resp2" {
		t.Errorf("expected 'resp2', got '%s'", resp2.Reply)
	}
}

func TestGenericRouteMultiSingle(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	conn1 := NewMessageConn(c1, JSONCodec{}, true)
	conn2 := NewMessageConn(c2, JSONCodec{}, false)

	route1 := NewRoute[TestReq, TestResp](conn1, "test_route_ms")
	route2 := NewRoute[TestReq, TestResp](conn2, "test_route_ms")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	route2.HandleMultiSingle(func(reqs <-chan TestReq) (TestResp, error) {
		var count int
		for range reqs {
			count++
		}
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}
		return TestResp{Reply: "done"}, nil
	})

	go conn1.AcceptStream(ctx)
	go conn2.AcceptStream(ctx)

	reqsCh := make(chan TestReq, 2)
	reqsCh <- TestReq{Msg: "1"}
	reqsCh <- TestReq{Msg: "2"}
	close(reqsCh)

	resp, err := route1.CallMultiSingle(ctx, reqsCh)
	if err != nil {
		t.Fatalf("CallMultiSingle failed: %v", err)
	}
	if resp.Reply != "done" {
		t.Errorf("expected 'done', got '%s'", resp.Reply)
	}
}

func TestGenericRouteBidi(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	conn1 := NewMessageConn(c1, JSONCodec{}, true)
	conn2 := NewMessageConn(c2, JSONCodec{}, false)

	route1 := NewRoute[TestReq, TestResp](conn1, "test_route_bidi")
	route2 := NewRoute[TestReq, TestResp](conn2, "test_route_bidi")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	route2.HandleBidi(func(s *TypedStream[TestResp, TestReq]) {
		req, err := s.Recv(ctx)
		if err != nil {
			t.Errorf("Recv failed: %v", err)
		}
		if req.Msg != "ping" {
			t.Errorf("expected 'ping', got '%s'", req.Msg)
		}
		err = s.Send(ctx, TestResp{Reply: "pong"})
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
		s.Close()
	})

	go conn1.AcceptStream(ctx)
	go conn2.AcceptStream(ctx)

	s, err := route1.CallBidi(ctx)
	if err != nil {
		t.Fatalf("CallBidi failed: %v", err)
	}

	err = s.Send(ctx, TestReq{Msg: "ping"})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	resp, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if resp.Reply != "pong" {
		t.Errorf("expected 'pong', got '%s'", resp.Reply)
	}
	s.Close()
}
