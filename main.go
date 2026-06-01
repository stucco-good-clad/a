package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	url := "http://slc.rpc.orbitflare.com"
	key := os.Getenv("KEY_1")
	if key == "" {
		fmt.Println("missing KEY_1")
		os.Exit(1)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getSlot",
		"params":  []interface{}{},
	})

	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", key)

	client := &http.Client{Timeout: 15 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("status=%d time=%s body=%s\n", resp.StatusCode, elapsed, string(b))
}
