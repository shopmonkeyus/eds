//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/api"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type ShutdownFunc func()

func setupServer(logger logger.Logger, creds string) (int, ShutdownFunc) {
	p, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/v3/eds/internal/enroll/{code}", func(w http.ResponseWriter, r *http.Request) {
		var resp api.EnrollResponse
		resp.Success = true
		resp.Data = api.EnrollTokenData{
			Token:    enrollToken,
			ServerID: serverID,
		}
		logger.Info("enroll request received: %s", r.PathValue("code"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/eds/internal", func(w http.ResponseWriter, r *http.Request) {
		var resp api.SessionStartResponse
		resp.Success = true
		usercreds := base64.StdEncoding.EncodeToString([]byte(creds))
		resp.Data = api.EdsSession{
			SessionId:  sessionId,
			Credential: &usercreds,
		}
		logger.Info("session start")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/eds/internal/log", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/v3/eds/internal/{sessionid}", func(w http.ResponseWriter, r *http.Request) {
		var resp api.SessionEndResponse
		resp.Success = true
		resp.Data.URL = fmt.Sprintf("http://127.0.0.1:%d/v3/eds/internal/log", p)
		resp.Data.ErrorURL = fmt.Sprintf("http://127.0.0.1:%d/v3/eds/internal/log", p)
		logger.Info("session send")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/schema/{object}/{version}", func(w http.ResponseWriter, r *http.Request) {
		var resp internal.Schema
		object := r.PathValue("object")
		version := r.PathValue("version")
		logger.Info("schema request received for %s %s", object, version)
		switch object {
		case "order":
			resp = internal.Schema{
				Table:        "order",
				ModelVersion: version,
				Properties: map[string]internal.SchemaProperty{
					"id": {
						Type: "string",
					},
					"name": {
						Type: "string",
					},
					"age": {
						Type: "number",
					},
					"updatedDate": {
						Type:   "string",
						Format: "date-time",
					},
				},
				PrimaryKeys: []string{"id"},
			}
		case "customer":
			resp = internal.Schema{
				Table:        "customer",
				ModelVersion: version,
				Properties: map[string]internal.SchemaProperty{
					"id": {
						Type: "string",
					},
					"name": {
						Type: "string",
					},
				},
				PrimaryKeys: []string{"id"},
			}
		}
		logger.Info("schema fetched for %s %s", object, version)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/schema", func(w http.ResponseWriter, r *http.Request) {
		var resp = make(internal.SchemaMap)
		resp["order"] = &internal.Schema{
			Table:        "order",
			ModelVersion: modelVersion,
			Properties: map[string]internal.SchemaProperty{
				"id": {
					Type: "string",
				},
				"name": {
					Type: "string",
				},
				"updatedDate": {
					Type:   "string",
					Format: "date-time",
				},
			},
			PrimaryKeys: []string{"id"},
		}
		logger.Info("schema fetched")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	s := &http.Server{
		Addr: fmt.Sprintf(":%d", p),
	}

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return p, func() {
		if err := s.Shutdown(context.Background()); err != nil {
			logger.Error("error shutting down http server: %s", err)
		}
	}
}
