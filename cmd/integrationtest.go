package cmd

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/integrationtest"
	"github.com/shopmonkeyus/eds/internal/registry"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/slice"
	"github.com/spf13/cobra"
)

func RunWithLogAndRecover(fn func(cmd *cobra.Command, args []string, log logger.Logger)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		log := newLogger(cmd)
		defer func() {
			if err := recover(); err != nil {
				log.Error("error running integration test: %s", err)
			}
		}()
		fn(cmd, args, log)
	}
}

var integrationtestCmd = &cobra.Command{
	Use:   "integrationtest",
	Short: "Run integration tests",
	Long:  "Run various integration tests for EDS components",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		integrationtest.GlobalConnectionHandler.DisconnectAll()
	},
}

var updateDatamodelCmd = &cobra.Command{
	Use:   "update-datamodel",
	Short: "update the datamodel",
	Long:  "update the datamodel from the shopmonkey api",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		log.Info("Starting update-datamodel")

		registery, err := registry.NewAPIRegistryPrivate(context.Background(), log, "http://api.shopmonkey.cloud", "test", nil)
		if err != nil {
			log.Fatal("Failed to create registry: %s", err)
		}
		schema, err := registery.GetLatestSchema()
		if err != nil {
			log.Fatal("Failed to get latest schema: %s", err)
		}

		const (
			RWX_RX_RX = 0755
			_RW_R_R   = 0644
		)

		os.MkdirAll("datamodel", RWX_RX_RX)

		mergedEnums := make(enumsMap)
		for _, s := range schema {
			// if s.Table != "customer" {
			// 	continue
			// }
			model, enums := goStructFromSchema(*s)
			for name, enum := range enums {
				if existingEnum, ok := mergedEnums[name]; ok {
					if !reflect.DeepEqual(enum, existingEnum) {
						mergedEnums[name+s.Table] = enum
						continue
					}
				}
				mergedEnums[name] = enum
			}
			os.WriteFile(fmt.Sprintf("datamodel/%s.go", s.Table), []byte(model), _RW_R_R)
		}
		generatedEnums := generateEnums(mergedEnums)
		os.WriteFile("datamodel/enums.go", []byte(generatedEnums), _RW_R_R)
	}),
}

func toPublic(name string) string {
	return strings.ToUpper(string(name[0])) + name[1:]
}

func hyphensToUnderscores(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

func generateEnums(enums enumsMap) string {
	lines := []string{"package datamodel", ""}
	lines = append(lines, "//this file is machine generated", "//if you edit this file, demons will pour down your wifi connection and infest your harddrive with snakes", "//@jdavenport on slack with questions", "")
	for name, enum := range enums {
		lines = append(lines, fmt.Sprintf("type %s string", toPublic(name)), "const (")
		for _, enum := range enum {
			lines = append(lines, fmt.Sprintf(`	%s %s = "%s"`, name+toPublic(hyphensToUnderscores(enum)), toPublic(name), enum))
		}
		lines = append(lines, ")", "")
	}
	return strings.Join(lines, "\n")
}

type enumsMap = map[string][]string
type goModel = string

func goStructFromSchema(schema internal.Schema) (goModel, enumsMap) {
	Sprintf := fmt.Sprintf

	typeMap := map[string]string{
		"string":  "string",
		"number":  "float64",
		"integer": "int",
		"boolean": "bool",
		"array":   "[]string",
		"object":  "[]byte",
	}

	enums := make(enumsMap)

	lines := []string{"package datamodel", ""}
	lines = append(lines, "//this file is machine generated", "//if you edit this file, demons will pour down your wifi connection and infest your harddrive with snakes", "//@jdavenport on slack with questions", "")
	lines = append(lines, "//model version: "+schema.ModelVersion)
	lines = append(lines, Sprintf("type %s struct {", toPublic(schema.Table)), "")

	maxFieldWidth := 0
	for name := range schema.Properties {
		if len(toPublic(name)) > maxFieldWidth {
			maxFieldWidth = len(toPublic(name))
		}
	}

	for name, property := range schema.Properties {
		dataType := typeMap[property.Type]

		if property.Items != nil && property.Items.Type == "enum" {
			enumType := toPublic(name + "Enum")
			enums[enumType] = property.Items.Enum
			dataType = enumType
		}

		if property.Nullable || !slice.Contains(schema.Required, name) {
			dataType = "*" + dataType
		}
		lines = append(lines, Sprintf("	%-*s %s", maxFieldWidth, toPublic(name), dataType))
	}

	lines = append(lines, "}", "")
	for _, line := range lines {
		fmt.Println(line)
	}

	model := strings.Join(lines, "\n")
	// Create datamodel directory if it doesn't exist

	return model, enums
}

var loadTestRandomCmd = &cobra.Command{
	Use:   "loadtest-random",
	Short: "Send random messages to nats",
	Long:  "Send random messages to nats for integration testing. Currently sends customer update messages",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		log.Info("Starting load-random integration test")
		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

		log.Info("Sending %d customer messages", count)

		delivered := integrationtest.PublishRandomMessages(js, count, log)

		log.Info("Completed sending %d customer messages to NATS", delivered)
	}),
}

var publishFileDataCmd = &cobra.Command{
	Use:   "publish-file-data",
	Short: "Publish test data to NATS from file",
	Long:  "Publishes dbchange events from JSON file to NATS\nTest data should be at path '..eds_test_data/output.jsonl.tar.gz'\n@jdavenport for details",
	Run: RunWithLogAndRecover(func(cmd *cobra.Command, args []string, log logger.Logger) {
		log.Info("Starting publish-file-data integration test")

		count, _ := cmd.Flags().GetInt("count")

		js := integrationtest.NewConnection(&integrationtest.JetstreamConnection{})

		path := "../eds_test_data/output.jsonl.tar.gz"

		delivered := integrationtest.PublishTestData(path, count, js, log)
		log.Info("completed sending %d messages", delivered)
	}),
}

func init() {
	loadTestRandomCmd.Flags().String("nats-url", "", "NATS server URL (default: nats://localhost:4222)")
	loadTestRandomCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")

	publishFileDataCmd.Flags().Int("count", 1, "Number of messages to send (default: 1)")

	integrationtestCmd.AddCommand(loadTestRandomCmd)
	integrationtestCmd.AddCommand(updateDatamodelCmd)
	integrationtestCmd.AddCommand(publishFileDataCmd)

	rootCmd.AddCommand(integrationtestCmd)
}
