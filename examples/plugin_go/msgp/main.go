package main

import (
	plugingo "github.com/yougg/plugo/examples/plugin_go"
	"github.com/yougg/plugo/examples/shared"
)

func main() {
	plugingo.Run(plugingo.PluginConfig{
		Name:          "Go Msgpack Plugin",
		Codec:         shared.MsgpackCodec{},
		StreamPrefix:  "Go-Msgpack-Stream-Processed: ",
		MessagePrefix: "Processed by Go Msgpack: ",
		NodeID:        "go-msgp-node-1",
	})
}
