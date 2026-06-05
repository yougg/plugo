package plugo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Define parameters for starting the current test binary recursively as a subprocess plugin
var runPlugin = flag.Bool("run-test-plugin", false, "internal flag to run the test binary as a subprocess plugin")
var runRawPlugin = flag.Bool("run-raw-plugin", false, "internal flag to run the test binary as a subprocess plugin for raw data")

// TestMain acts as the entry point, executing the plugin runner if flagged, or standard testing suite
func TestMain(m *testing.M) {
	flag.Parse()
	if *runPlugin {
		runTestPluginClient()
		os.Exit(0)
	}
	if *runRawPlugin {
		runTestRawPluginClient()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// IPCMessage represents the message contract exchanged between host and plugin during verification
type IPCMessage struct {
	ID      uint32 `json:"id" gob:"id"`
	Payload string `json:"payload" gob:"payload"`
}

// runTestPluginClient simulates the plugin client process execution loop
func runTestPluginClient() {
	ctx := context.Background()

	// 1. Initialize client connection channel and negotiate
	msgConn, err := Attaching(ctx, GobCodec{}, JSONCodec{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Plugin] Failed to initialize connection and negotiate: %v\n", err)
		os.Exit(1)
	}

	// 2. Start background AcceptStream goroutine to natively handle multiplexed streams in parallel.
	// Thanks to rawMsgCh, this runs beautifully in parallel with ReadMessage!
	go func() {
		for {
			stream, err := msgConn.AcceptStream(ctx)
			if err != nil {
				break
			}
			go func(s *Stream) {
				defer s.Close()

				// Read the first payload to determine mode
				firstPayload, err := s.Recv(ctx)
				if err != nil {
					return
				}

				mode := string(firstPayload)
				if mode == "CMD:SINGLE_MULTI" {
					// Single-to-Multi: received 1 command, send 3 responses
					for i := 1; i <= 3; i++ {
						resp := fmt.Sprintf("MultiResp-%d", i)
						if err := s.Send(ctx, []byte(resp)); err != nil {
							break
						}
					}
				} else if mode == "CMD:MULTI_SINGLE" {
					// Multi-to-Single: receive until "END", send 1 summary
					count := 0
					for {
						payload, err := s.Recv(ctx)
						if err != nil {
							break
						}
						if string(payload) == "END" {
							break
						}
						count++
					}
					summary := fmt.Sprintf("Summary:%d", count)
					_ = s.Send(ctx, []byte(summary))
				} else {
					// Default Echo behavior (also used for Multi-to-Multi)
					// Process the first payload
					processed := append([]byte("Stream-Processed: "), firstPayload...)
					if err := s.Send(ctx, processed); err != nil {
						return
					}
					// Continue echoing
					for {
						payload, err := s.Recv(ctx)
						if err != nil {
							break
						}
						processed := append([]byte("Stream-Processed: "), payload...)
						if err := s.Send(ctx, processed); err != nil {
							break
						}
					}
				}
			}(stream)
		}
	}()

	// 4. Main framed message loop: block, process, and write back standard request-response messages.
	for {
		req, err := ReadMessage[IPCMessage](ctx, msgConn)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "[Plugin] ReadMessage failed: %v\n", err)
			break
		}

		req.Payload = "Processed: " + req.Payload

		if err := WriteMessage(ctx, msgConn, req); err != nil {
			fmt.Fprintf(os.Stderr, "[Plugin] WriteMessage failed: %v\n", err)
			break
		}
	}
}

// runTestRawPluginClient simulates a plugin that uses ReadData and WriteData directly
func runTestRawPluginClient() {
	ctx := context.Background()

	msgConn, err := Attaching(ctx, GobCodec{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[RawPlugin] Failed to initialize connection and negotiate: %v\n", err)
		os.Exit(1)
	}

	for {
		data, err := msgConn.ReadData(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "[RawPlugin] ReadData failed: %v\n", err)
			break
		}

		processed := append([]byte("Raw-Processed: "), data...)
		if err := msgConn.WriteData(ctx, processed); err != nil {
			fmt.Fprintf(os.Stderr, "[RawPlugin] WriteData failed: %v\n", err)
			break
		}
	}
}

// TestPlugoIntegration verifies the complete context-aware concurrent IPC communication
func TestPlugoIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to resolve current test executable path: %v", err)
	}

	plugin, err := Open(ctx, exePath, WithArgs("-run-test-plugin"), WithCodec(GobCodec{}, JSONCodec{}))
	if err != nil {
		t.Fatalf("Failed to start plugin subprocess: %v", err)
	}
	defer plugin.Close()

	msgConn := plugin.Conn()

	if msgConn.Codec().Name() != "gob" {
		t.Errorf("Unexpected codec selection. Expected: gob, Got: %s", msgConn.Codec().Name())
	}

	var pendingResponses sync.Map
	var active uint32 = 1

	// Launch background dispatcher
	go func() {
		for {
			resp, err := ReadMessage[IPCMessage](ctx, msgConn)
			if err != nil {
				return
			}
			if chVal, ok := pendingResponses.Load(resp.ID); ok {
				ch := chVal.(chan string)
				select {
				case ch <- resp.Payload:
				default:
				}
			}
		}
	}()

	// Concurrently invoke 20 standard requests
	var wg sync.WaitGroup
	var errorCount int32

	for i := range 20 {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			reqID := atomic.AddUint32(&active, 1)
			ch := make(chan string, 1)
			pendingResponses.Store(reqID, ch)
			defer pendingResponses.Delete(reqID)

			req := IPCMessage{
				ID:      reqID,
				Payload: fmt.Sprintf("Data-%d", seq),
			}

			if err := WriteMessage(ctx, msgConn, req); err != nil {
				t.Errorf("WriteMessage failed in concurrency test: %v", err)
				atomic.AddInt32(&errorCount, 1)
				return
			}

			select {
			case result := <-ch:
				expected := "Processed: Data-" + fmt.Sprintf("%d", seq)
				if result != expected {
					t.Errorf("Payload mismatch. Expected: %s, Got: %s", expected, result)
					atomic.AddInt32(&errorCount, 1)
				}
			case <-ctx.Done():
				t.Errorf("Request canceled or timed out, ID: %d", reqID)
				atomic.AddInt32(&errorCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("Integration verification failed with %d errors", errorCount)
	}
}

// TestPlugoStreamIntegration verifies multiplexed bidirectional streaming over a single connection
func TestPlugoStreamIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to resolve current test executable path: %v", err)
	}

	plugin, err := Open(ctx, exePath, WithArgs("-run-test-plugin"), WithCodec(GobCodec{}))
	if err != nil {
		t.Fatalf("Failed to start plugin subprocess for stream test: %v", err)
	}
	defer plugin.Close()

	msgConn := plugin.Conn()

	var wg sync.WaitGroup
	var errorCount int32

	// Concurrently establish 3 independent streams over the same physical channel
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(streamID uint32) {
			defer wg.Done()

			stream, err := msgConn.CreateStream(ctx)
			if err != nil {
				t.Errorf("Failed to create Stream ID %d: %v", streamID, err)
				atomic.AddInt32(&errorCount, 1)
				return
			}
			defer stream.Close()

			for seq := 1; seq <= 5; seq++ {
				payload := fmt.Sprintf("StreamMsg-%d-%d", streamID, seq)
				if err := stream.Send(ctx, []byte(payload)); err != nil {
					t.Errorf("Stream %d failed to Send at seq %d: %v", streamID, seq, err)
					atomic.AddInt32(&errorCount, 1)
					return
				}

				resp, err := stream.Recv(ctx)
				if err != nil {
					t.Errorf("Stream %d failed to Recv at seq %d: %v", streamID, seq, err)
					atomic.AddInt32(&errorCount, 1)
					return
				}

				expected := fmt.Sprintf("Stream-Processed: StreamMsg-%d-%d", streamID, seq)
				if string(resp) != expected {
					t.Errorf("Stream %d payload mismatch. Expected: %s, Got: %s", streamID, expected, string(resp))
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}(uint32(i * 100))
	}

	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("Stream integration verification failed with %d errors", errorCount)
	}
}

// TestPlugoWriteDataReadData verifies raw data write/read using MessageConn
func TestPlugoWriteDataReadData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to resolve current test executable path: %v", err)
	}

	plugin, err := Open(ctx, exePath, WithArgs("-run-raw-plugin"), WithCodec(GobCodec{}))
	if err != nil {
		t.Fatalf("Failed to start plugin subprocess for raw data test: %v", err)
	}
	defer plugin.Close()

	msgConn := plugin.Conn()

	expectedPayload := []byte("hello raw data test payload")
	if err := msgConn.WriteData(ctx, expectedPayload); err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	resp, err := msgConn.ReadData(ctx)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	expectedResp := append([]byte("Raw-Processed: "), expectedPayload...)
	if string(resp) != string(expectedResp) {
		t.Errorf("Raw data mismatch. Expected: %s, Got: %s", string(expectedResp), string(resp))
	}
}

// TestCodecs tests normal and edge cases of various codecs
func TestCodecs(t *testing.T) {
	// 1. Test global Marshal and Unmarshal functions when Codec is nil
	_, err := Marshal[string](nil, "test")
	if err == nil {
		t.Error("Expected error when marshaling with nil codec, got nil")
	}

	_, err = Unmarshal[string](nil, []byte("test"))
	if err == nil {
		t.Error("Expected error when unmarshaling with nil codec, got nil")
	}

	// 2. Test JSON codec
	jsonCodec := JSONCodec{}
	if name := jsonCodec.Name(); name != "json" {
		t.Errorf("Expected codec name 'json', got %s", name)
	}

	// Normal serialization and deserialization
	type testStruct struct {
		Name string `json:"name"`
	}
	encodedJSON, err := jsonCodec.Marshal(testStruct{Name: "plugo"})
	if err != nil {
		t.Fatalf("JSON marshal failed: %v", err)
	}
	var decodedJSON testStruct
	if err := jsonCodec.Unmarshal(encodedJSON, &decodedJSON); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if decodedJSON.Name != "plugo" {
		t.Errorf("Expected name 'plugo', got %s", decodedJSON.Name)
	}

	// Abnormal serialization: passing types that cannot be serialized to JSON
	_, err = jsonCodec.Marshal(make(chan int))
	if err == nil {
		t.Error("Expected JSON marshal to fail for channel type, but it succeeded")
	}

	// Abnormal deserialization: passing invalid JSON bytes
	var destJSON testStruct
	err = jsonCodec.Unmarshal([]byte("{invalid json}"), &destJSON)
	if err == nil {
		t.Error("Expected JSON unmarshal to fail for invalid json, but it succeeded")
	}

	// 3. Test Gob codec
	gobCodec := GobCodec{}
	if name := gobCodec.Name(); name != "gob" {
		t.Errorf("Expected codec name 'gob', got %s", name)
	}

	// Normal serialization and deserialization
	encodedGob, err := gobCodec.Marshal("test gob string")
	if err != nil {
		t.Fatalf("Gob marshal failed: %v", err)
	}
	var decodedGob string
	if err := gobCodec.Unmarshal(encodedGob, &decodedGob); err != nil {
		t.Fatalf("Gob unmarshal failed: %v", err)
	}
	if decodedGob != "test gob string" {
		t.Errorf("Expected 'test gob string', got %s", decodedGob)
	}

	// Abnormal serialization: passing types that cannot be encoded by Gob
	_, err = gobCodec.Marshal(make(chan int))
	if err == nil {
		t.Error("Expected Gob marshal to fail for channel type, but it succeeded")
	}

	// Abnormal deserialization: passing invalid Gob data
	var destGob string
	err = gobCodec.Unmarshal([]byte("invalid gob bytes"), &destGob)
	if err == nil {
		t.Error("Expected Gob unmarshal to fail for invalid bytes, but it succeeded")
	}
}

// customRWC combines io.Reader and io.Writer into an io.ReadWriteCloser for unit testing
type customRWC struct {
	io.Reader
	io.Writer
	closeFunc func() error
}

func (c *customRWC) Close() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

// TestClientInitAndPipeConn tests Attaching and its behavior under simulated Windows pipe environment variables
func TestClientInitAndPipeConn(t *testing.T) {
	// Backup and clean environment variables
	origRead := os.Getenv("PLUGO_PIPE_READ")
	origWrite := os.Getenv("PLUGO_PIPE_WRITE")
	defer func() {
		os.Setenv("PLUGO_PIPE_READ", origRead)
		os.Setenv("PLUGO_PIPE_WRITE", origWrite)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Test setting only one environment variable, should error
	os.Setenv("PLUGO_PIPE_READ", "123")
	os.Unsetenv("PLUGO_PIPE_WRITE")
	_, err := Attaching(ctx, GobCodec{})
	if err == nil || err.Error() != "missing one of the pipe handles, both read and write handles are required" {
		t.Errorf("Expected missing handle error, got %v", err)
	}

	// 2. Test error without environment variables (Unix default FD 3 branch)
	os.Unsetenv("PLUGO_PIPE_READ")
	os.Unsetenv("PLUGO_PIPE_WRITE")
	// Calling directly here will use FD 3. Usually FD 3 is empty in test environment, returning FD 3 not found
	// If not empty, it will also error during subsequent negotiation, so it should definitely error
	_, err = Attaching(ctx, GobCodec{})
	if err == nil {
		t.Error("Expected InitPluginClient to fail when FD 3 is missing, but it succeeded")
	}

	// 3. Simulate complete Windows pipe negotiation and initialization process
	// Create two pairs of pipes: r1/w1 for Host -> Client, r2/w2 for Client -> Host
	r1, w1, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe 1: %v", err)
	}
	defer r1.Close()
	defer w1.Close()

	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe 2: %v", err)
	}
	defer r2.Close()
	defer w2.Close()

	// Client reads r1, Client writes w2
	os.Setenv("PLUGO_PIPE_READ", fmt.Sprintf("%d", r1.Fd()))
	os.Setenv("PLUGO_PIPE_WRITE", fmt.Sprintf("%d", w2.Fd()))

	// Host reads r2, Host writes w1
	hostRWC := &customRWC{
		Reader: r2,
		Writer: w1,
		closeFunc: func() error {
			_ = r2.Close()
			_ = w1.Close()
			return nil
		},
	}

	errCh := make(chan error, 1)
	go func() {
		// Host performs handshake negotiation concurrently
		hostConn, err := negotiateFromHostToPlugin(ctx, hostRWC, GobCodec{})
		if err != nil {
			errCh <- err
			return
		}
		// After successful negotiation, write a little test data to verify Read/Write of the connection
		if err := hostConn.WriteData(ctx, []byte("hello-from-host")); err != nil {
			errCh <- err
			return
		}
		_ = hostConn.Close()
		close(errCh)
	}()

	// Client side starts initialization
	clientConn, err := Attaching(ctx, GobCodec{})
	if err != nil {
		t.Fatalf("InitPluginClient failed: %v", err)
	}
	defer clientConn.Close()

	// Verify whether the client successfully received data written by Host
	data, err := clientConn.ReadData(ctx)
	if err != nil {
		t.Fatalf("Failed to read from client connection: %v", err)
	}
	if string(data) != "hello-from-host" {
		t.Errorf("Expected 'hello-from-host', got %q", string(data))
	}

	// Wait for Host goroutine to end and check for errors
	if err := <-errCh; err != nil {
		t.Errorf("Host negotiation or writing failed: %v", err)
	}
}

// errorConn simulates various read/write exceptions and deadline settings that occur in testing
type errorConn struct {
	readErr         error
	writeErr        error
	readBuf         *bytes.Buffer
	writeBuf        *bytes.Buffer
	deadlineTime    time.Time
	lastSetDeadline time.Time
	closed          bool
}

func (c *errorConn) Read(p []byte) (n int, err error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	if c.readBuf != nil {
		return c.readBuf.Read(p)
	}
	return 0, io.EOF
}

func (c *errorConn) Write(p []byte) (n int, err error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.writeBuf != nil {
		return c.writeBuf.Write(p)
	}
	return len(p), nil
}

func (c *errorConn) Close() error {
	c.closed = true
	return nil
}

func (c *errorConn) SetDeadline(t time.Time) error {
	c.deadlineTime = t
	if !t.IsZero() {
		c.lastSetDeadline = t
	}
	return nil
}

// TestNegotiationEdgeCases covers all exceptional branches in negotiate.go and withContext features
func TestNegotiationEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Host negotiation: empty codecs error
	_, err := negotiateFromHostToPlugin(ctx, &errorConn{}, nil...)
	if err == nil {
		t.Error("Expected NegotiateHost to fail on empty codecs, got nil")
	}

	// 2. Host negotiation: error occurred while writing handshake request
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{writeErr: fmt.Errorf("write error")}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on write error, got nil")
	}

	// 3. Host negotiation: error/EOF occurred reading handshake response length
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{readErr: fmt.Errorf("read length error")}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on read length error, got nil")
	}

	// 4. Host negotiation: received invalid handshake response with length 0
	rBuf := bytes.NewBuffer([]byte{0, 0, 0, 0}) // Length = 0
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on 0 length response, got nil")
	}

	// 5. Host negotiation: incomplete data/EOF occurred reading handshake response payload
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 10, 1, 2, 3}) // Length = 10, but only 3 bytes data
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on incomplete response payload, got nil")
	}

	// 6. Host negotiation: failed to parse handshake response JSON
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 8, '{', 'i', 'n', 'v', 'a', 'l', 'i', 'd'}) // json error
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on invalid JSON response, got nil")
	}

	// 7. Host negotiation: plugin selected a Codec not supported by Host
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 24, '{', '"', 's', 'e', 'l', 'e', 'c', 't', 'e', 'd', '"', ':', '"', 'x', 'm', 'l', '"', '}'}) // selected "xml"
	_, err = negotiateFromHostToPlugin(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateHost to fail on unsupported codec selection, got nil")
	}

	// 8. Client negotiation: empty codecs error
	_, err = negotiateFromPluginToHost(ctx, &errorConn{}, nil...)
	if err == nil {
		t.Error("Expected NegotiateClient to fail on empty codecs, got nil")
	}

	// 9. Client negotiation: error occurred reading handshake request length
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readErr: fmt.Errorf("read req length error")}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on read length error, got nil")
	}

	// 10. Client negotiation: received invalid handshake request with length 0
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 0})
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on 0 length request, got nil")
	}

	// 11. Client negotiation: incomplete data/EOF occurred reading handshake request payload
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 10, 1, 2})
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on incomplete request payload, got nil")
	}

	// 12. Client negotiation: failed to parse handshake request JSON
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 5, 'n', 'o', 'j', 's', 'n'})
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on invalid JSON request, got nil")
	}

	// 13. Client negotiation: no mutual Codec error
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 22, '{', '"', 's', 'u', 'p', 'p', 'o', 'r', 't', 'e', 'd', '"', ':', '[', '"', 'j', 's', 'o', 'n', '"', ']', '}'})
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readBuf: rBuf}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on no mutual codec, got nil")
	}

	// 14. Client negotiation: failed to send response
	rBuf = bytes.NewBuffer([]byte{0, 0, 0, 21, '{', '"', 's', 'u', 'p', 'p', 'o', 'r', 't', 'e', 'd', '"', ':', '[', '"', 'g', 'o', 'b', '"', ']', '}'})
	_, err = negotiateFromPluginToHost(ctx, &errorConn{readBuf: rBuf, writeErr: fmt.Errorf("write response error")}, GobCodec{})
	if err == nil {
		t.Error("Expected NegotiateClient to fail on write response error, got nil")
	}

	// 15. withContext related: ctx already timed out/canceled
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = negotiateFromHostToPlugin(canceledCtx, &errorConn{}, GobCodec{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled wrap, got %v", err)
	}

	// 16. withContext related: Deadline propagation test
	ec := &errorConn{
		readBuf: bytes.NewBuffer([]byte{0, 0, 0, 1}), // Let read at least continue, but intentionally stall due to incompleteness
	}
	timeCtx, cancelTime := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelTime()

	_, _ = negotiateFromHostToPlugin(timeCtx, ec, GobCodec{})
	if ec.lastSetDeadline.IsZero() {
		t.Error("Expected SetDeadline to be called with context deadline, but lastSetDeadline was zero")
	}
}

// TestConnectionAndStreamEdgeCases covers all edge cases and exceptions of Stream and MessageConn in conn.go
func TestConnectionAndStreamEdgeCases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ==========================================
	// 1. Error path test in synchronous read mode (non-multiplexed)
	// ==========================================
	c1, c2 := net.Pipe()
	mcSync := NewMessageConn(c1, GobCodec{}, true)
	mcMux := NewMessageConn(c2, GobCodec{}, false)

	// 1.1 Test receiving non-zero stream frame, sync read should error and discard the frame
	go func() {
		header := make([]byte, 12)
		binary.BigEndian.PutUint32(header[0:4], 5)   // Length = 5
		binary.BigEndian.PutUint32(header[4:8], 100) // StreamID = 100
		binary.BigEndian.PutUint32(header[8:12], 0)  // Flags = 0
		_, _ = c2.Write(header)
		_, _ = c2.Write([]byte("hello"))
	}()
	_, err := mcSync.ReadData(ctx)
	if err == nil || !strings.Contains(err.Error(), "received stream frame (ID 100) in non-multiplexed synchronous mode") {
		t.Errorf("Expected received stream frame in sync mode error, got %v", err)
	}

	// 1.2 Test error when receiving Error frame (Flags & 8 != 0)
	go func() {
		header := make([]byte, 12)
		binary.BigEndian.PutUint32(header[0:4], 8) // Length = 8
		binary.BigEndian.PutUint32(header[4:8], 0) // StreamID = 0
		binary.BigEndian.PutUint32(header[8:12], StreamFlagError)
		_, _ = c2.Write(header)
		_, _ = c2.Write([]byte("sync-err"))
	}()
	_, err = mcSync.ReadData(ctx)
	if err == nil || !strings.Contains(err.Error(), "connection error frame received: sync-err") {
		t.Errorf("Expected connection error frame error, got %v", err)
	}

	// 1.3 Test EOF/error caused by incomplete Payload read
	go func() {
		header := make([]byte, 12)
		binary.BigEndian.PutUint32(header[0:4], 50) // Length = 50, but we only write 5 bytes
		binary.BigEndian.PutUint32(header[4:8], 0)
		binary.BigEndian.PutUint32(header[8:12], 0)
		_, _ = c2.Write(header)
		_, _ = c2.Write([]byte("short"))
		_ = c2.Close() // Intentionally close to trigger EOF
	}()
	_, err = mcSync.ReadData(ctx)
	if err == nil || !strings.Contains(err.Error(), "failed to read payload") {
		t.Errorf("Expected payload read error, got %v", err)
	}
	_ = mcSync.Close()

	// ==========================================
	// 2. Error path test of multiplexed mode and Stream
	// ==========================================
	c3, c4 := net.Pipe()
	mcMux = NewMessageConn(c3, GobCodec{}, true)
	defer mcMux.Close()
	mcMux2 := NewMessageConn(c4, GobCodec{}, false)
	_ = mcMux2

	// 2.1 Verify UnderlyingConn
	if mcMux.UnderlyingConn() != c3 {
		t.Error("UnderlyingConn did not return the correct connection")
	}

	// 2.2 Start a stream on the Host side
	// To not block writeStreamFrame in CreateStream, stream start frame must be read asynchronously from c4
	streamStartCh := make(chan []byte, 1)
	go func() {
		hdr := make([]byte, 13) // 12 bytes header + 1 byte (mode) payload
		_, _ = io.ReadFull(c4, hdr)
		streamStartCh <- hdr
	}()

	s1, err := mcMux.CreateStream(ctx)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	<-streamStartCh

	// Verify ID
	if s1.ID() != 1 { // Auto-increment starts at 1 for host
		t.Errorf("Expected stream ID 1, got %d", s1.ID())
	}

	// 2.3 Verify Stream.Send error
	// Send error when parameter context is canceled
	canceledCtx, cancelSend := context.WithCancel(context.Background())
	cancelSend()
	if err := s1.Send(canceledCtx, []byte("test")); err == nil {
		t.Error("Expected Send to fail with canceled context, got nil")
	}

	// Send error after stream itself is closed
	go func() {
		hdr := make([]byte, 12)
		_, _ = io.ReadFull(c4, hdr)
	}()
	_ = s1.Close()
	if err := s1.Send(ctx, []byte("test")); err == nil || !strings.Contains(err.Error(), "stream is closed") {
		t.Errorf("Expected 'stream is closed' error, got %v", err)
	}

	// 2.4 Verify Stream.Recv handling Close frame and Error frame
	// First, trigger mcMux side to accept new stream by writing a Start frame to c4
	go func() {
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint32(hdr[0:4], 0)
		binary.BigEndian.PutUint32(hdr[4:8], 300) // StreamID = 300
		binary.BigEndian.PutUint32(hdr[8:12], StreamFlagStart)
		_, _ = c4.Write(hdr)
	}()

	s300, err := mcMux.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("Failed to accept stream: %v", err)
	}

	// Write Close frame to c4 (Flags = 4)
	go func() {
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint32(hdr[0:4], 0)
		binary.BigEndian.PutUint32(hdr[4:8], 300)
		binary.BigEndian.PutUint32(hdr[8:12], StreamFlagClose)
		_, _ = c4.Write(hdr)
	}()

	_, err = s300.Recv(ctx)
	if err != io.EOF {
		t.Errorf("Expected Recv to return EOF on peer close, got %v", err)
	}

	// Accept new stream 400 again, and simulate sending Error frame (Flags = 8)
	go func() {
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint32(hdr[0:4], 0)
		binary.BigEndian.PutUint32(hdr[4:8], 400) // StreamID = 400
		binary.BigEndian.PutUint32(hdr[8:12], StreamFlagStart)
		_, _ = c4.Write(hdr)
	}()

	s400, err := mcMux.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("Failed to accept stream: %v", err)
	}

	go func() {
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint32(hdr[0:4], 9)   // Length = 9
		binary.BigEndian.PutUint32(hdr[4:8], 400) // StreamID = 400
		binary.BigEndian.PutUint32(hdr[8:12], StreamFlagError)
		_, _ = c4.Write(hdr)
		_, _ = c4.Write([]byte("fatal-err"))
	}()

	_, err = s400.Recv(ctx)
	if err == nil {
		t.Error("Expected stream error, got nil")
	}

	// 2.5 Verify behavior after readLoop error
	// Forcibly close c4, causing readLoop of mcMux to die
	_ = c4.Close()

	// Wait a short while for readLoop to finish cleanup
	time.Sleep(50 * time.Millisecond)

	// After readLoop crashes, CreateStream should return readLoopErr
	_, err = mcMux.CreateStream(ctx)
	if err == nil || !strings.Contains(err.Error(), "read loop is not running due to error") {
		t.Errorf("Expected read loop error when creating stream, got %v", err)
	}

	// After readLoop crashes, AcceptStream should return readLoopErr
	_, err = mcMux.AcceptStream(ctx)
	if err == nil || !strings.Contains(err.Error(), "read loop error") {
		t.Errorf("Expected read loop error when accepting stream, got %v", err)
	}

	// After readLoop crashes, ReadData should return readLoopErr
	_, err = mcMux.ReadData(ctx)
	if err == nil {
		t.Error("Expected error when reading from broken multiplexed connection, got nil")
	}
}

// dummyCodec is used to simulate an invalid Codec rejected by subprocess
type dummyCodec struct{}

func (dummyCodec) Marshal(v any) ([]byte, error)      { return nil, nil }
func (dummyCodec) Unmarshal(data []byte, v any) error { return nil }
func (dummyCodec) Name() string                       { return "dummy" }

// TestPluginLifecycleAndConcurrency tests plugin lifecycle management, failure cleanup, and concurrent startup
func TestPluginLifecycleAndConcurrency(t *testing.T) {
	// Temporarily redirect os.Stderr to os.DevNull to prevent expected subprocess error logs from polluting console output
	oldStderr := os.Stderr
	nullFile, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		os.Stderr = nullFile
		defer func() {
			os.Stderr = oldStderr
			_ = nullFile.Close()
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to resolve current test executable path: %v", err)
	}

	// 1. Test error when StartPlugin starts a non-existent binary
	_, err = Open(ctx, "/nonexistent/binary/path", WithCodec(GobCodec{}))
	if err == nil || !strings.Contains(err.Error(), "failed to start plugin subprocess") {
		t.Errorf("Expected cmd start error, got %v", err)
	}

	// 2. Test resource cleanup and error when StartPlugin negotiation fails (unsupported Codec)
	_, err = Open(ctx, exePath, WithArgs("-run-test-plugin"), WithCodec(dummyCodec{}))
	if err == nil || !strings.Contains(err.Error(), "failed to negotiate codec with plugin") {
		t.Errorf("Expected negotiation fail error, got %v", err)
	}

	// 4. Normally start a plugin, verify calls to Cmd() and Wait()
	plugin, err := Open(ctx, exePath, WithArgs("-run-test-plugin"), WithCodec(GobCodec{}))
	if err != nil {
		t.Fatalf("Failed to start valid plugin: %v", err)
	}

	if cmd := plugin.Cmd(); cmd == nil {
		t.Error("Expected plugin.Cmd() to return non-nil value")
	}

	// Close plugin to release resources
	if err := plugin.Close(); err != nil {
		t.Errorf("plugin.Close() failed: %v", err)
	}

	// Calling Close again should be safe
	_ = plugin.Close()

	// Wait should return (may return exec: Wait was already called error, but still successful error return)
	_ = plugin.Wait()
}

// TestPlugoStreamAdvancedModes verifies Single-to-Multi, Multi-to-Single, and Multi-to-Multi patterns
func TestPlugoStreamAdvancedModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to resolve current test executable path: %v", err)
	}

	plugin, err := Open(ctx, exePath, WithArgs("-run-test-plugin"), WithCodec(GobCodec{}))
	if err != nil {
		t.Fatalf("Failed to start plugin subprocess for advanced stream test: %v", err)
	}
	defer plugin.Close()

	msgConn := plugin.Conn()
	var wg sync.WaitGroup
	var errorCount int32

	// Mode 1: Single-to-Multi (Host sends 1 request, reads 3 responses)
	wg.Go(func() {
		s, err := msgConn.CreateStream(ctx)
		if err != nil {
			t.Errorf("Failed to create Stream 1001: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}
		defer s.Close()

		if err := s.Send(ctx, []byte("CMD:SINGLE_MULTI")); err != nil {
			t.Errorf("Send failed: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}

		for i := 1; i <= 3; i++ {
			resp, err := s.Recv(ctx)
			if err != nil {
				t.Errorf("Recv failed: %v", err)
				atomic.AddInt32(&errorCount, 1)
				return
			}
			expected := fmt.Sprintf("MultiResp-%d", i)
			if string(resp) != expected {
				t.Errorf("Expected %s, got %s", expected, string(resp))
				atomic.AddInt32(&errorCount, 1)
			}
		}
	})

	// Mode 2: Multi-to-Single (Host sends 3 data + END, reads 1 response)
	wg.Go(func() {
		s, err := msgConn.CreateStream(ctx)
		if err != nil {
			t.Errorf("Failed to create Stream 1002: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}
		defer s.Close()

		if err := s.Send(ctx, []byte("CMD:MULTI_SINGLE")); err != nil {
			t.Errorf("Send failed: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}

		for i := 1; i <= 3; i++ {
			_ = s.Send(ctx, fmt.Appendf(nil, "Data-%d", i))
		}
		_ = s.Send(ctx, []byte("END"))

		resp, err := s.Recv(ctx)
		if err != nil {
			t.Errorf("Recv failed: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}
		if string(resp) != "Summary:3" {
			t.Errorf("Expected Summary:3, got %s", string(resp))
			atomic.AddInt32(&errorCount, 1)
		}
	})

	// Mode 3: Multi-to-Multi (Host sends independently, reads independently)
	wg.Go(func() {
		s, err := msgConn.CreateStream(ctx)
		if err != nil {
			t.Errorf("Failed to create Stream 1003: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}
		defer s.Close()

		if err := s.Send(ctx, []byte("CMD:ECHO")); err != nil {
			t.Errorf("Send failed: %v", err)
			atomic.AddInt32(&errorCount, 1)
			return
		}

		var duplexWg sync.WaitGroup
		duplexWg.Add(2)

		// Sender goroutine
		go func() {
			defer duplexWg.Done()
			for i := 1; i <= 3; i++ {
				_ = s.Send(ctx, fmt.Appendf(nil, "Duplex-%d", i))
			}
		}()

		// Receiver goroutine
		go func() {
			defer duplexWg.Done()
			// Expecting 1 echo for CMD:ECHO + 3 echos for Duplex-x
			for i := 0; i <= 3; i++ {
				resp, err := s.Recv(ctx)
				if err != nil {
					t.Errorf("Duplex Recv failed: %v", err)
					atomic.AddInt32(&errorCount, 1)
					return
				}
				if !strings.HasPrefix(string(resp), "Stream-Processed:") {
					t.Errorf("Invalid duplex echo: %s", string(resp))
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}()

		duplexWg.Wait()
	})

	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("Advanced modes verification failed with %d errors", errorCount)
	}
}
