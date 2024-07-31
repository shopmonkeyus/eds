package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

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
	return nil, fmt.Errorf("Invalid first letter")
}

var enrollCmd = &cobra.Command{
	Use:   "enroll",
	Short: "Enroll a new server and get api key",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[enroll]")
		code := mustFlagString(cmd, "code", true)

		firstLetter := code[0:1]
		apiURL, err := getApiUrl(firstLetter)

		if err != nil {
			logger.Error("error getting api url: %s", err)
			os.Exit(1)
		}

		req, err := http.NewRequest("GET", *apiURL+"/v3/eds/internal/enroll/"+code, nil)
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
		if _, err := os.Stat("dataDir"); os.IsNotExist(err) {
			os.Mkdir("dataDir", 0755)
		}

		tokenFile := "dataDir/token.json"
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
	rootCmd.AddCommand(enrollCmd)
	enrollCmd.Flags().String("code", "", "the enrollment code from HQ")

}
