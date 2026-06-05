package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yougg/plugo"
	"github.com/yougg/plugo/examples/shared"
)

// ipcClient encapsulates the multiplexing dispatcher for each plugin process
type ipcClient struct {
	name      string
	plugin    *plugo.Plugin
	msgConn   *plugo.MessageConn
	callbacks *sync.Map
	activeID  *atomic.Uint32
	ctx       context.Context
	cancel    context.CancelFunc
}

func newIPCClient(name string, plugin *plugo.Plugin, msgConn *plugo.MessageConn) *ipcClient {
	ctx, cancel := context.WithCancel(context.Background())
	cli := &ipcClient{
		name:      name,
		plugin:    plugin,
		msgConn:   msgConn,
		callbacks: &sync.Map{},
		activeID:  &atomic.Uint32{},
		ctx:       ctx,
		cancel:    cancel,
	}

	// Start background read loop for multiplexing responses to awaiting Goroutines
	go cli.readLoop()

	return cli
}

// readLoop continuously reads framed messages and dispatches them via ID
func (c *ipcClient) readLoop() {
	for {
		resp, err := plugo.ReadMessage[shared.IPCRespMessage](c.ctx, c.msgConn)
		if err != nil {
			if err == io.EOF {
				slog.Info("[Host] Plugin connection closed", "plugin", c.name)
				c.cancel()
				break
			}
			select {
			case <-c.ctx.Done():
				// Closed intentionally
				return
			default:
			}
			slog.Error("[Host] Failed to read message from plugin", "plugin", c.name, "error", err)
			c.cancel()
			break
		}

		if val, ok := c.callbacks.Load(resp.ID); ok {
			ch := val.(chan shared.ResponsePayload)
			select {
			case ch <- resp.Payload:
			default:
			}
		}
	}
}

// Call safely sends a request concurrently and blocks awaiting the corresponding response with context timeout
func (c *ipcClient) Call(ctx context.Context, cmd string, payload shared.RequestPayload) (shared.ResponsePayload, error) {
	reqID := c.activeID.Add(1)

	ch := make(chan shared.ResponsePayload, 1)
	c.callbacks.Store(reqID, ch)
	defer c.callbacks.Delete(reqID)

	req := shared.IPCReqMessage{
		ID:      reqID,
		Command: cmd,
		Payload: payload,
	}

	if err := plugo.WriteMessage(ctx, c.msgConn, req); err != nil {
		return shared.ResponsePayload{}, fmt.Errorf("failed to write message: %w", err)
	}

	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		return shared.ResponsePayload{}, ctx.Err()
	case <-c.ctx.Done():
		return shared.ResponsePayload{}, fmt.Errorf("plugin connection closed")
	case <-time.After(3 * time.Second):
		return shared.ResponsePayload{}, fmt.Errorf("request timed out")
	}
}

// Close gracefully closes the connection and kills the plugin process
func (c *ipcClient) Close() error {
	c.cancel()
	return c.plugin.Close()
}

func main() {
	slog.Info("[Host] Host service started. Beginning STAGE 1: Standard Concurrency Verification...")

	// Locate plugin binaries in the same directory based on the host executable path
	exePath, err := os.Executable()
	if err != nil {
		slog.Error("[Host] Failed to get executable path", "error", err)
		os.Exit(1)
	}
	pluginDir := filepath.Dir(exePath)

	var ext string
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}

	type localPluginConfig struct {
		Name    string
		BinPath string
		Codecs  []plugo.Codec
	}

	// 1. Configure the plugins for STAGE 1
	pluginConfigs := []localPluginConfig{
		{
			Name:    "Go-JSON-Plugin",
			BinPath: filepath.Join(pluginDir, "plugin_go_json"+ext),
			Codecs:  []plugo.Codec{plugo.JSONCodec{}},
		},
		{
			Name:    "Go-Msgpack-Plugin",
			BinPath: filepath.Join(pluginDir, "plugin_go_msgp"+ext),
			Codecs:  []plugo.Codec{shared.MsgpackCodec{}, plugo.JSONCodec{}},
		},
		{
			Name:    "Zig-Msgpack-Plugin",
			BinPath: filepath.Join(pluginDir, "plugin_zig_msgp"+ext),
			Codecs:  []plugo.Codec{shared.MsgpackCodec{}},
		},
		{
			Name:    "Zig-JSON-Plugin",
			BinPath: filepath.Join(pluginDir, "plugin_zig_json"+ext),
			Codecs:  []plugo.Codec{plugo.JSONCodec{}},
		},
		{
			Name:    "Go-Abnormal-Plugin",
			BinPath: filepath.Join(pluginDir, "plugin_go_abnormal"+ext),
			Codecs:  []plugo.Codec{plugo.JSONCodec{}},
		},
	}

	// Filter out plugin configurations where the binary file does not exist
	var activeConfigs []localPluginConfig
	for _, config := range pluginConfigs {
		if _, err = os.Stat(config.BinPath); os.IsNotExist(err) {
			slog.Warn("[Host] Plugin binary not found, skipping plugin loading", "name", config.Name, "path", config.BinPath)
			continue
		}
		activeConfigs = append(activeConfigs, config)
	}

	var activeClients []*ipcClient
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, config := range activeConfigs {
		wg.Add(1)
		go func(cfg localPluginConfig) {
			defer wg.Done()
			p, err := plugo.Open(ctx, cfg.BinPath, plugo.WithCodec(cfg.Codecs...))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("[Host] Plugin failed to start, ignoring", "name", cfg.Name, "error", err)
				return
			}
			slog.Info("[Host] Plugin initialized successfully", "name", cfg.Name, "codec", p.Conn().Codec().Name())
			cli := newIPCClient(cfg.Name, p, p.Conn())
			activeClients = append(activeClients, cli)
		}(config)
	}

	wg.Wait()

	slog.Info("[Host] All STAGE 1 plugins ready! Dispatching concurrent requests...")

	var wgTasks sync.WaitGroup
	for _, cli := range activeClients {
		for i := 1; i <= 3; i++ {
			wgTasks.Add(1)
			go func(c *ipcClient, taskID int) {
				defer wgTasks.Done()

				payload := shared.RequestPayload{
					TaskName: fmt.Sprintf("Task-%d", taskID),
					TaskDetail: shared.TaskInfo{
						Step:    taskID,
						Options: "A,B,C",
					},
				}
				slog.Info("[Host] Dispatching call request", "plugin", c.name, "task_id", taskID, "payload", payload)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				begin := time.Now()
				result, err := c.Call(ctx, "execute", payload)
				if err != nil {
					slog.Error("[Host] Dispatch call failed", "plugin", c.name, "task_id", taskID, "error", err)
					return
				}

				slog.Info("[Host] Received call response", "plugin", c.name, "task_id", taskID, "result_msg", result.Result.Message, "latency", time.Since(begin))
			}(cli, i)
		}
	}

	wgTasks.Wait()

	slog.Info("[Host] STAGE 1 concurrency test completed! Cleaning up Stage 1 plugins...")
	for _, cli := range activeClients {
		_ = cli.Close()
	}

	slog.Info("[Host] STAGE 1 finished successfully!")
	slog.Info("==========================================================================================")
	slog.Info("STAGE 2: Multiplexed Long-running Bidirectional Message Streaming Showcase")
	slog.Info("==========================================================================================")

	// 2. Launch Go Msgpack Plugin to demonstrate long-running multiplexed streams over a single connection
	goMsgpBin := filepath.Join(pluginDir, "plugin_go_msgp"+ext)
	slog.Info("[Host] Launching Go Msgpack Plugin for streaming demonstration...", "path", goMsgpBin)

	cmd := exec.Command(goMsgpBin)
	cmd.Stderr = os.Stderr

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer streamCancel()

	streamPlugin, err := plugo.Open(streamCtx, goMsgpBin, plugo.WithCodec(shared.MsgpackCodec{}))
	if err != nil {
		slog.Error("[Host] Failed to start plugin for Stage 2", "error", err)
		os.Exit(1)
	}
	defer streamPlugin.Close()

	// ---------------------------------------------------------
	// STAGE 2: High-Level RPC Streaming Routing
	// ---------------------------------------------------------
	slog.Info("==========================================================================================")
	slog.Info("STAGE 2: High-Level Multiplexed RPC Streams (Single-Multi, Multi-Single, Bidi) Showcase")
	slog.Info("==========================================================================================")

	slog.Info("[Host] Launching stream routes on plugin...", "path", goMsgpBin)
	msgConn := streamPlugin.Conn()

	ctxStream, cancelStream := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStream()

	var wgStream sync.WaitGroup
	wgStream.Add(3)

	// Stream Route 1: Single-to-Multi
	go func() {
		defer wgStream.Done()
		slog.Info("[Host] Route: query_data (Single-to-Multi) starting...")
		route := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "query_data")
		respsChan, err := route.CallSingleMulti(ctxStream, shared.RequestPayload{TaskName: "Get Metrics"})
		if err != nil {
			slog.Error("[Host] CallSingleMulti failed", "error", err)
			return
		}
		for resp := range respsChan {
			slog.Info("[Host] query_data received multi-response", "payload", resp.Result.Message)
		}
	}()

	// Stream Route 2: Multi-to-Single
	go func() {
		defer wgStream.Done()
		slog.Info("[Host] Route: upload_logs (Multi-to-Single) starting...")

		route := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "upload_logs")
		reqsCh := make(chan shared.RequestPayload)
		go func() {
			for i := 1; i <= 3; i++ {
				reqsCh <- shared.RequestPayload{TaskName: fmt.Sprintf("LogChunk-%d", i)}
				time.Sleep(100 * time.Millisecond)
			}
			close(reqsCh)
		}()

		resp, err := route.CallMultiSingle(ctxStream, reqsCh)
		if err != nil {
			slog.Error("[Host] CallMultiSingle failed", "error", err)
			return
		}
		slog.Info("[Host] upload_logs received summary", "payload", resp.Result.Message)
	}()

	// Stream Route 3: Bidi
	go func() {
		defer wgStream.Done()
		slog.Info("[Host] Route: realtime_sync (Bidi) starting...")
		route := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "realtime_sync")
		s3, err := route.CallBidi(ctxStream)
		if err != nil {
			slog.Error("[Host] CallBidi failed", "error", err)
			return
		}
		defer s3.Close()

		var duplexWg sync.WaitGroup
		duplexWg.Add(2)

		go func() {
			defer duplexWg.Done()
			for i := 1; i <= 2; i++ {
				data := shared.RequestPayload{TaskName: fmt.Sprintf("SyncData-%d", i)}
				if err := s3.Send(ctxStream, data); err != nil {
					return
				}
				time.Sleep(150 * time.Millisecond)
			}
		}()

		go func() {
			defer duplexWg.Done()
			for i := 1; i <= 2; i++ {
				resp, err := s3.Recv(ctxStream)
				if err != nil {
					return
				}
				slog.Info("[Host] realtime_sync received echo", "payload", resp.Result.Message)
			}
		}()
		duplexWg.Wait()
		slog.Info("[Host] realtime_sync completed.")
	}()

	wgStream.Wait()
	slog.Info("[Host] STAGE 2 streaming showcase completed perfectly! Releasing Stage 2 resources...")
	slog.Info("[Host] All Plugo operations and showcases completed successfully!")
}
