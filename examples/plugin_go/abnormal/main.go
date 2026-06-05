package main

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/yougg/plugo"
	"github.com/yougg/plugo/examples/shared"
)

func main() {
	ctx := context.Background()
	msgConn, err := plugo.Attaching(ctx, plugo.JSONCodec{})
	if err != nil {
		slog.Error("[Abnormal] Handshake failed", "error", err)
		os.Exit(1)
	}
	defer msgConn.Close()

	slog.Info("[Abnormal] Handshake successful, preparing to simulate abnormal behavior")

	for {
		req, err := plugo.ReadMessage[shared.IPCReqMessage](ctx, msgConn)
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("[Abnormal] Failed to read message", "error", err)
			break
		}

		slog.Info("[Abnormal] Received request, preparing to simulate panic", "id", req.ID)

		// mock exception
		panic("Crash occurred while simulating request processing")
	}
}
