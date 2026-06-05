package plugingo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/yougg/plugo"
	"github.com/yougg/plugo/examples/shared"
)

type PluginConfig struct {
	Name          string
	Codec         plugo.Codec
	StreamPrefix  string
	MessagePrefix string
	NodeID        string
}

func Run(cfg PluginConfig) {
	ctx := context.Background()

	msgConn, err := plugo.Attaching(ctx, cfg.Codec)
	if err != nil {
		slog.Error("["+cfg.Name+"] Failed to initialize client and negotiate codec", "error", err)
		os.Exit(1)
	}
	defer msgConn.Close()

	slog.Info("["+cfg.Name+"] Negotiation successful, channel upgraded", "codec", msgConn.Codec().Name())

	// ---------------------------------------------------------
	// STAGE 2: Register Stream Routes (Option 2 - fully abstract)
	// ---------------------------------------------------------

	// Route 1: query_data (Single-to-Multi)
	r1 := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "query_data")
	r1.HandleSingleMulti(func(req shared.RequestPayload, send func(shared.ResponsePayload) error) error {
		slog.Info("["+cfg.Name+"] query_data route called", "req", req.TaskName)
		for i := 1; i <= 3; i++ {
			resp := shared.ResponsePayload{Result: shared.TaskResult{Message: cfg.StreamPrefix + fmt.Sprintf("MultiResp-%d", i)}}
			if err := send(resp); err != nil {
				return err
			}
		}
		return nil
	})

	// Route 2: upload_logs (Multi-to-Single)
	r2 := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "upload_logs")
	r2.HandleMultiSingle(func(reqs <-chan shared.RequestPayload) (shared.ResponsePayload, error) {
		count := 0
		for req := range reqs {
			_ = req // process part
			count++
		}
		slog.Info("["+cfg.Name+"] upload_logs route processed stream chunks", "count", count)
		summary := shared.ResponsePayload{Result: shared.TaskResult{Message: cfg.StreamPrefix + fmt.Sprintf("Summary:%d", count)}}
		return summary, nil
	})

	// Route 3: realtime_sync (Bidi)
	r3 := plugo.NewRoute[shared.RequestPayload, shared.ResponsePayload](msgConn, "realtime_sync")
	r3.HandleBidi(func(s *plugo.TypedStream[shared.ResponsePayload, shared.RequestPayload]) {
		defer s.Close()
		slog.Info("[" + cfg.Name + "] realtime_sync bidirectional route started")
		for {
			payload, err := s.Recv(ctx)
			if err != nil {
				break
			}
			processed := shared.ResponsePayload{Result: shared.TaskResult{Message: cfg.StreamPrefix + payload.TaskName}}
			if err := s.Send(ctx, processed); err != nil {
				break
			}
		}
	})

	// ---------------------------------------------------------
	// STAGE 1: Standard IPC Messages (Request/Response)
	// ---------------------------------------------------------
	// The plugin now blocks, serving IPC requests via ReadMessage and routed streams in the background.
	for {
		req, err := plugo.ReadMessage[shared.IPCReqMessage](ctx, msgConn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("[" + cfg.Name + "] Host disconnected, plugin exiting cleanly")
				break
			}
			slog.Error("["+cfg.Name+"] Failed to read message", "error", err)
			break
		}

		slog.Info("["+cfg.Name+"] Received request", "id", req.ID, "cmd", req.Command, "task", req.Payload.TaskName)

		resp := shared.IPCRespMessage{
			ID:      req.ID,
			Command: req.Command,
			Payload: shared.ResponsePayload{
				Code: 200,
				Result: shared.TaskResult{
					Message: cfg.MessagePrefix + req.Payload.TaskName,
					Extra: shared.ExtraResult{
						NodeID: cfg.NodeID,
						Status: "SUCCESS",
					},
				},
			},
		}

		if err := plugo.WriteMessage(ctx, msgConn, resp); err != nil {
			slog.Error("["+cfg.Name+"] Failed to write back response", "error", err)
			break
		}
	}

	slog.Info("[" + cfg.Name + "] Exiting gracefully")
}
