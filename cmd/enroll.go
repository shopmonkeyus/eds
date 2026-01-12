package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/shopmonkeyus/eds/internal/api"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/spf13/cobra"
)

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

		logger.Info("what uuuuuuuup")

		if apiURL == "" {
			logger.Debug("Getting api from prefix")
			firstLetter := code[0:1]
			maybeApiURL, err := api.GetAPIURL(firstLetter)
			if err != nil {
				logger.Fatal("error getting api url: %s", err)
			}
			apiURL = *maybeApiURL
		}

		req, err := http.NewRequest("GET", apiURL+"/v3/eds/internal/enroll/"+code, nil)
		if err != nil {
			logger.Fatal("error creating request: %s", err)
		}

		retry := util.NewHTTPRetry(req, util.WithLogger(logger))
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

		var enrollResp api.EnrollResponse
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
