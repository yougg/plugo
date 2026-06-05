package main

import (
	"github.com/yougg/plugo"
	plugingo "github.com/yougg/plugo/examples/plugin_go"
)

func main() {
	plugingo.Run(plugingo.PluginConfig{
		Name:          "Go JSON Plugin",
		Codec:         plugo.JSONCodec{},
		StreamPrefix:  "Go-JSON-Stream-Processed: ",
		MessagePrefix: "Processed by Go JSON: ",
		NodeID:        "go-json-node-1",
	})
}
