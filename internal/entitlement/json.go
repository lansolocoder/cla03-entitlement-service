package entitlement

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// errTrailingJSON is returned when a body carries data after its first value.
var errTrailingJSON = errors.New("invalid JSON body: unexpected trailing content")

// newJSONDecoder returns a strict decoder: unknown fields are rejected and
// numbers are kept as literals so integer fields can be validated exactly.
func newJSONDecoder(raw []byte) *json.Decoder {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	return dec
}

// decodeStrict decodes a single JSON value into v, rejecting unknown fields
// and any data following the first value.
func decodeStrict(raw []byte, v any) error {
	dec := newJSONDecoder(raw)
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return errTrailingJSON
		}
		return err
	}
	return nil
}
