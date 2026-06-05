package shared

import (
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

// MsgpackCodec implements plugo.Codec using github.com/tinylib/msgp
// for zero-reflection, high-performance MessagePack encoding/decoding.
type MsgpackCodec struct{}

// Marshal serializes an object implementing msgp.Marshaler into MessagePack bytes.
func (MsgpackCodec) Marshal(v any) ([]byte, error) {
	marshaler, ok := v.(msgp.Marshaler)
	if !ok {
		return nil, fmt.Errorf("type %T does not implement msgp.Marshaler interface", v)
	}
	// nil allows msgp to automatically allocate underlying buffer capacity
	data, err := marshaler.MarshalMsg(nil)
	if err != nil {
		return nil, fmt.Errorf("msgp marshal failed: %w", err)
	}
	return data, nil
}

// Unmarshal deserializes MessagePack bytes into an object implementing msgp.Unmarshaler.
func (MsgpackCodec) Unmarshal(data []byte, v any) error {
	unmarshaler, ok := v.(msgp.Unmarshaler)
	if !ok {
		return fmt.Errorf("type %T does not implement msgp.Unmarshaler interface", v)
	}
	_, err := unmarshaler.UnmarshalMsg(data)
	if err != nil {
		return fmt.Errorf("msgp unmarshal failed: %w", err)
	}
	return nil
}

// Name returns the protocol negotiation name "msgp".
func (MsgpackCodec) Name() string {
	return "msgp"
}
