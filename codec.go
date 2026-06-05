package plugo

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
)

// Codec defines a unified interface for plugin data serialization and deserialization.
type Codec interface {
	// Marshal serializes an object into a byte slice.
	Marshal(v any) ([]byte, error)
	// Unmarshal deserializes a byte slice into a target object.
	Unmarshal(data []byte, v any) error
	// Name returns the name of the codec, used for capability negotiation between host and plugins.
	Name() string
}

// Marshal serialize any value of type t using codec
func Marshal[T any](c Codec, v T) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("codec cannot be nil")
	}
	return c.Marshal(v)
}

// Unmarshal Use codec to deserialize to type T and directly return the T value
func Unmarshal[T any](c Codec, data []byte) (T, error) {
	var v T
	if c == nil {
		return v, fmt.Errorf("codec cannot be nil")
	}
	if err := c.Unmarshal(data, &v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// JSONCodec implements the Codec interface using standard library json.
type JSONCodec struct{}

// Marshal serializes an object to JSON bytes.
func (JSONCodec) Marshal(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed: %w", err)
	}
	return data, nil
}

// Unmarshal deserializes JSON bytes into a target object.
func (JSONCodec) Unmarshal(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return fmt.Errorf("json unmarshal failed: %w", err)
	}
	return nil
}

// Name returns the codec name "json".
func (JSONCodec) Name() string {
	return "json"
}

// GobCodec implements the Codec interface using standard library gob.
type GobCodec struct{}

// Marshal serializes an object to Gob bytes.
func (GobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("gob encode failed: %w", err)
	}
	return buf.Bytes(), nil
}

// Unmarshal deserializes Gob bytes into a target object.
func (GobCodec) Unmarshal(data []byte, v any) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("gob decode failed: %w", err)
	}
	return nil
}

// Name returns the codec name "gob".
func (GobCodec) Name() string {
	return "gob"
}
