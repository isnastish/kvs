package kvs

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/isnastish/kvs/pkg/version"
)

func performHttpRequest(t *testing.T, ctx context.Context, method string, endpoint string, body *bytes.Buffer) {
	var req *http.Request
	var err error
	var resp *http.Response
	httpClient := http.Client{}
	// We should always pass untyped nil to http.NewRequest/NewRequestWithContext,
	// that's why the code is a bit convoluted here, otherwise we get nil-pointer dereference
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, endpoint, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, endpoint, body)
	}

	done := make(chan struct{}, 1)
	go func() {
		resp, err = httpClient.Do(req)
		done <- struct{}{}
	}()

	for {
		select {
		case <-done:
			assert.True(t, err == nil)
			assert.True(t, resp.StatusCode == http.StatusOK)
			resp.Body.Close()
			return
		case <-ctx.Done():
			assert.True(t, false)
			return
		}
	}
}

func TestTrailingSlash(t *testing.T) {
	settings := Settings{
		Endpoint:           ":5000",
		TransactionLogFile: "test_transactions.bin",
	}

	defer func() {
		err := os.Remove(settings.TransactionLogFile)
		assert.True(t, err == nil)
	}()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	defer waitGroup.Wait()

	go func() {
		defer waitGroup.Done()
		RunServer(&settings)
	}()

	<-time.After(200 * time.Millisecond)

	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/api/%s", settings.Endpoint, version.GetServiceVersion()))
	withTrailingSlashURL := baseURL.JoinPath("/echo/")
	withoutTrailingSlashURL := baseURL.JoinPath("/echo")
	killServerURL := baseURL.JoinPath("/kill")

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancel()

	performHttpRequest(t, timeoutCtx, http.MethodGet, withTrailingSlashURL.String(), bytes.NewBufferString("echo ECHO echo"))
	performHttpRequest(t, timeoutCtx, http.MethodGet, withoutTrailingSlashURL.String(), bytes.NewBufferString("ECHO ECHo ECho Echo echo"))
	performHttpRequest(t, timeoutCtx, http.MethodGet, killServerURL.String(), nil)
}
