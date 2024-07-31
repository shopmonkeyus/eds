package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

type enrollTokenData struct {
	Token string `json:"token"`
}

type enrollResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    enrollTokenData `json:"data"`
}

func getApiUrl(firstLetter string) (*string, error) {
	apiUrls := map[string]string{
		"P": "https://api.shopmonkey.cloud/",
		"S": "https://sandbox-api.shopmonkey.cloud/",
		"E": "https://edge-api.shopmonkey.cloud/",
		"L": "http://localhost:3101",
	}

	if url, exists := apiUrls[firstLetter]; exists {
		return &url, nil
	}
	return nil, errors.New("invalid first letter")
}

var enrollCmd = &cobra.Command{
	Use:   "enroll code",
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
			maybeApiURL, err := getApiUrl(firstLetter)
			if err != nil {
				logger.Error("error getting api url: %s", err)
				os.Exit(1)
			}
			apiURL = *maybeApiURL
		}

		req, err := http.NewRequest("GET", apiURL+"/v3/eds/internal/enroll/"+code, nil)
		if err != nil {
			logger.Error("error creating request: %s", err)
			os.Exit(1)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			logger.Error("failed to enroll server: %w", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			buf, _ := io.ReadAll(resp.Body)
			logger.Error("failed to enroll server. status code=%d. %s", resp.StatusCode, string(buf))
			os.Exit(1)
		}

		var enrollResp enrollResponse
		if err := json.NewDecoder(resp.Body).Decode(&enrollResp); err != nil {
			logger.Error("failed to decode response: %w", err)
			os.Exit(1)
		}
		if !enrollResp.Success {
			logger.Error("failed to start enroll: %s", enrollResp.Message)
		}

		tokenFile := filepath.Join(dataDir, "token.json")
		file, err := os.Create(tokenFile)
		if err != nil {
			logger.Error("failed to create token file: %w", err)
			os.Exit(1)
		}
		_, err = file.WriteString(enrollResp.Data.Token)
		if err != nil {
			logger.Error("failed to write to token file: %w", err)
			os.Exit(1)
		}
		defer file.Close()

	},
}

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("couldn't get current working directory: ", err)
		os.Exit(1)
	}
	rootCmd.AddCommand(enrollCmd)
	enrollCmd.Flags().String("api-url", "", "the for testing again preview environment")
	enrollCmd.Flags().String("data-dir", cwd, "the data directory for storing logs and other data")
}
