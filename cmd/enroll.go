package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/spf13/cobra"
)

type enrollTokenData struct {
	Token    string `json:"token" toml:"token"`
	ServerID string `json:"serverId" toml:"server_id"`
}

type enrollResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    enrollTokenData `json:"data"`
}

func getAPIURL(firstLetter string) (*string, error) {
	apiUrls := map[string]string{
		"P": "https://api.shopmonkey.cloud",
		"S": "https://sandbox-api.shopmonkey.cloud",
		"E": "https://edge-api.shopmonkey.cloud",
		"L": "http://localhost:3101",
	}

	if url, exists := apiUrls[firstLetter]; exists {
		return &url, nil
	}
	return nil, errors.New("invalid code")
}

var enrollCmd = &cobra.Command{
	Use:   "enroll [code]",
	Short: "Enroll a new server and get api key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[enroll]")
		code := args[0]
		apiURL := mustFlagString(cmd, "api-url", false)
		dataDir := getDataDir(cmd, logger)

		if apiURL == "" {
			logger.Debug("Getting api from prefix")
			firstLetter := code[0:1]
			maybeApiURL, err := getAPIURL(firstLetter)
			if err != nil {
				logger.Fatal("error getting api url: %s", err)
			}
			apiURL = *maybeApiURL
		}

		req, err := http.NewRequest("GET", apiURL+"/v3/eds/internal/enroll/"+code, nil)
		if err != nil {
			logger.Fatal("error creating request: %s", err)
		}

		retry := util.NewHTTPRetry(req)
		resp, err := retry.Do()
		if err != nil {
			logger.Fatal("failed to enroll server: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				logger.Fatal("invalid enrollment code or it has already been used")
			}
			logger.Fatal("%s", handleAPIError(resp, "enroll"))
		}

		var enrollResp enrollResponse
		if err := json.NewDecoder(resp.Body).Decode(&enrollResp); err != nil {
			logger.Fatal("failed to decode response: %w", err)
		}
		if !enrollResp.Success {
			logger.Fatal("failed to start enroll: %s", enrollResp.Message)
		}

		var buf bytes.Buffer
		if err := toml.NewEncoder(&buf).Encode(enrollResp.Data); err != nil {
			logger.Fatal("failed to encode response: %w", err)
		}

		tokenFile := filepath.Join(dataDir, "config.toml")
		file, err := os.Create(tokenFile)
		if err != nil {
			logger.Fatal("failed to create token file: %w", err)
		}
		defer file.Close()
		if err := os.WriteFile(tokenFile, buf.Bytes(), 0644); err != nil {
			logger.Fatal("failed to write to token file: %w", err)
		}
		logger.Info("Enrollment successful!")
		logger.Info("run %s to start the server", getCommandExample("server"))
	},
}

func init() {
	rootCmd.AddCommand(enrollCmd)
	enrollCmd.Flags().String("api-url", "", "the for testing again preview environment")
	enrollCmd.Flags().MarkHidden("api-url")
}
