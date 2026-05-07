package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
)

func TestCreateExportJobReturnsErrorOn400(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message": "invalid request"}`))
	}))
	defer srv.Close()

	_, err := createExportJob(context.Background(), logger.NewTestLogger(), srv.URL, "test-api-key", exportJobCreateRequest{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid request")
}

func TestCreateExportJobReturnsJobIDOnSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true, "data": {"jobId": "job-abc-123"}}`))
	}))
	defer srv.Close()

	jobID, err := createExportJob(context.Background(), logger.NewTestLogger(), srv.URL, "test-api-key", exportJobCreateRequest{})

	assert.NoError(t, err)
	assert.Equal(t, "job-abc-123", jobID)
}
