package endpoints

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"

	"encoding/json"

	"github.com/alice-lg/birdwatcher/bird"
)

func makeTestParsed() bird.Parsed {
	return bird.Parsed{
		"protocols": bird.Parsed{
			"bgp1": bird.Parsed{
				"state": "up",
				"since": "2024-01-01",
			},
		},
		"ttl": "300",
	}
}

func TestWriteResponse_JSON(t *testing.T) {
	res := makeTestParsed()

	req := httptest.NewRequest(http.MethodGet, "/protocols", nil)
	// No Accept header — should default to JSON

	w := httptest.NewRecorder()
	writeResponse(w, req, res)

	resp := w.Result()

	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v\nbody: %s", err, body)
	}

	if _, ok := decoded["protocols"]; !ok {
		t.Errorf("expected 'protocols' key in decoded JSON, got keys: %v", decoded)
	}
}

func TestWriteResponse_CBOR(t *testing.T) {
	res := makeTestParsed()

	req := httptest.NewRequest(http.MethodGet, "/protocols", nil)
	req.Header.Set("Accept", "application/cbor")

	w := httptest.NewRecorder()
	writeResponse(w, req, res)

	resp := w.Result()

	if ct := resp.Header.Get("Content-Type"); ct != "application/cbor" {
		t.Errorf("expected Content-Type application/cbor, got %q", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}

	var decoded map[string]interface{}
	if err := cbor.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid CBOR: %v", err)
	}

	if _, ok := decoded["protocols"]; !ok {
		t.Errorf("expected 'protocols' key in decoded CBOR, got keys: %v", decoded)
	}
}

func TestWriteResponse_CBOR_Gzip(t *testing.T) {
	res := makeTestParsed()

	req := httptest.NewRequest(http.MethodGet, "/protocols", nil)
	req.Header.Set("Accept", "application/cbor")
	req.Header.Set("Accept-Encoding", "gzip")

	w := httptest.NewRecorder()
	writeResponse(w, req, res)

	resp := w.Result()

	if ct := resp.Header.Get("Content-Type"); ct != "application/cbor" {
		t.Errorf("expected Content-Type application/cbor, got %q", ct)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "gzip" {
		t.Errorf("expected Content-Encoding gzip, got %q", ce)
	}

	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("creating gzip reader: %v", err)
	}
	defer gr.Close()

	body, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompressing body: %v", err)
	}

	var decoded map[string]interface{}
	if err := cbor.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid CBOR after decompression: %v", err)
	}

	if _, ok := decoded["protocols"]; !ok {
		t.Errorf("expected 'protocols' key in decoded CBOR, got keys: %v", decoded)
	}
}
