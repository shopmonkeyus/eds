package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/spf13/cobra"
)

var integrationtestCmd = &cobra.Command{
	Use:   "integrationtest",
	Short: "Run integration tests",
	Long:  "Run various integration tests for EDS components",
}

// CustomerType represents the type of customer
type CustomerType string

const (
	CustomerTypeIndividual CustomerType = "Individual"
	CustomerTypeFleet      CustomerType = "Fleet"
)

// Country represents a country code
type Country string

const (
	CountryUS Country = "US"
	CountryCA Country = "CA"
	CountryMX Country = "MX"
)

// ContactMethods represents preferred contact methods
type ContactMethods string

const (
	ContactMethodsEmail ContactMethods = "Email"
	ContactMethodsPhone ContactMethods = "Phone"
	ContactMethodsSMS   ContactMethods = "SMS"
)

// LanguageCode represents language codes
type LanguageCode string

const (
	LanguageCodeEnUS LanguageCode = "en_US"
	LanguageCodeEsUS LanguageCode = "es_US"
	LanguageCodeFrCA LanguageCode = "fr_CA"
)

// generateRandomCustomer generates a random customer object based on the provided schema
func generateRandomCustomer(companyID, locationID string, customerID string) map[string]interface{} {
	now := time.Now()
	publicID := uuid.New().String()

	// Random customer type
	customerTypes := []CustomerType{CustomerTypeIndividual, CustomerTypeFleet}
	customerType := customerTypes[rand.Intn(len(customerTypes))]

	// Random country
	countries := []Country{CountryUS, CountryCA, CountryMX}
	country := countries[rand.Intn(len(countries))]

	// Random contact method
	contactMethods := []ContactMethods{ContactMethodsEmail, ContactMethodsPhone, ContactMethodsSMS}
	preferredContactMethod := contactMethods[rand.Intn(len(contactMethods))]

	// Random language
	languages := []LanguageCode{LanguageCodeEnUS, LanguageCodeEsUS, LanguageCodeFrCA}
	preferredLanguage := languages[rand.Intn(len(languages))]

	// Generate random strings for names and addresses
	firstName := fmt.Sprintf("FirstName%d", rand.Intn(1000))
	lastName := fmt.Sprintf("LastName%d", rand.Intn(1000))
	companyName := fmt.Sprintf("Company%d", rand.Intn(1000))
	address1 := fmt.Sprintf("%d Random St", rand.Intn(9999)+1)
	city := fmt.Sprintf("City%d", rand.Intn(1000))
	state := fmt.Sprintf("ST%d", rand.Intn(100))
	postalCode := fmt.Sprintf("%05d", rand.Intn(100000))
	phone := fmt.Sprintf("555-%03d-%04d", rand.Intn(1000), rand.Intn(10000))
	email := fmt.Sprintf("user%d@example.com", rand.Intn(10000))
	website := fmt.Sprintf("www.example%d.com", rand.Intn(1000))

	// Generate random DOT number (for fleet customers)
	var dotNumber *string
	if customerType == CustomerTypeFleet {
		dot := fmt.Sprintf("%06d", rand.Intn(1000000))
		dotNumber = &dot
	}

	// Generate random IDs for optional fields
	paymentTermID := uuid.New().String()
	laborRateID := uuid.New().String()
	laborMatrixID := uuid.New().String()
	pricingMatrixID := uuid.New().String()
	fleetID := uuid.New().String()
	referralSourceID := uuid.New().String()
	finixIdentityID := uuid.New().String()
	externalID := uuid.New().String()
	originLocationID := locationID

	// Generate random counts
	statementCount := rand.Intn(100)
	transactionCount := rand.Intn(500)
	deferredServiceCount := rand.Intn(50)
	appointmentCount := rand.Intn(200)
	messageCount := rand.Intn(300)
	vehicleCount := rand.Intn(20)
	orderCount := rand.Intn(100)

	// Generate random discount percentage
	discountPercent := rand.Float64() * 25.0 // 0-25%

	// Generate random timestamps
	createdDate := now.Add(-time.Duration(rand.Intn(365*24*60*60)) * time.Second) // Random date within last year
	updatedDate := time.Now()

	// Generate random last time order worked (nullable)
	var lastTimeOrderWorked *time.Time
	if rand.Float32() > 0.3 { // 70% chance of having a last order time
		lastOrderTime := createdDate.Add(time.Duration(rand.Intn(int(time.Since(createdDate).Seconds()))) * time.Second)
		lastTimeOrderWorked = &lastOrderTime
	}

	// Generate random tax exempt flags
	taxExempt := rand.Float32() > 0.8 // 20% chance of being tax exempt
	gstExempt := rand.Float32() > 0.8 // 20% chance of being GST exempt
	hstExempt := rand.Float32() > 0.8 // 20% chance of being HST exempt
	pstExempt := rand.Float32() > 0.8 // 20% chance of being PST exempt

	// Generate random marketing opt-in
	marketingOptIn := rand.Float32() > 0.7 // 30% chance of marketing opt-in

	// Generate random note
	note := fmt.Sprintf("Note%d", rand.Intn(1000))

	// Generate random location IDs array
	locationIDs := []string{locationID}
	if rand.Float32() > 0.5 { // 50% chance of having additional locations
		additionalLocations := rand.Intn(3) + 1
		for i := 0; i < additionalLocations; i++ {
			locationIDs = append(locationIDs, uuid.New().String())
		}
	}

	// Generate random meta and metadata
	meta := map[string]interface{}{
		"source":    "integration-test",
		"generated": true,
		"timestamp": now.Unix(),
	}

	var metadata map[string]interface{}
	if rand.Float32() > 0.5 { // 50% chance of having metadata
		metadata = map[string]interface{}{
			"customField1": fmt.Sprintf("value%d", rand.Intn(1000)),
			"customField2": rand.Intn(1000),
			"notes":        fmt.Sprintf("Test metadata %d", rand.Intn(1000)),
		}
	}

	// Generate random custom fields
	var customFields map[string]interface{}
	if rand.Float32() > 0.6 { // 40% chance of having custom fields
		customFields = map[string]interface{}{
			"favoriteColor":       fmt.Sprintf("color%d", rand.Intn(1000)),
			"preferredContact":    fmt.Sprintf("contact%d", rand.Intn(1000)),
			"specialInstructions": fmt.Sprintf("Instructions %d", rand.Intn(1000)),
		}
	}

	// Generate random labels
	labels := []map[string]interface{}{
		{"name": fmt.Sprintf("Label%d", rand.Intn(1000)), "color": "#FFD700"},
	}

	// Build the customer object
	customer := map[string]interface{}{
		"id":                     customerID,
		"meta":                   meta,
		"createdDate":            createdDate.Format(time.RFC3339),
		"updatedDate":            updatedDate.Format(time.RFC3339),
		"companyId":              companyID,
		"locationIds":            locationIDs,
		"customerType":           string(customerType),
		"firstName":              firstName,
		"lastName":               lastName,
		"companyName":            companyName,
		"address1":               address1,
		"city":                   city,
		"state":                  state,
		"country":                string(country),
		"postalCode":             postalCode,
		"paymentTermId":          paymentTermID,
		"publicId":               publicID,
		"note":                   note,
		"marketingOptIn":         marketingOptIn,
		"preferredContactMethod": string(preferredContactMethod),
		"preferredLanguage":      string(preferredLanguage),
		"taxExempt":              taxExempt,
		"gstExempt":              gstExempt,
		"hstExempt":              hstExempt,
		"pstExempt":              pstExempt,
		"discountPercent":        discountPercent,
		"laborRateId":            laborRateID,
		"laborMatrixId":          laborMatrixID,
		"pricingMatrixId":        pricingMatrixID,
		"fleetId":                fleetID,
		"statementCount":         statementCount,
		"transactionCount":       transactionCount,
		"deferredServiceCount":   deferredServiceCount,
		"appointmentCount":       appointmentCount,
		"messageCount":           messageCount,
		"website":                website,
		"vehicleCount":           vehicleCount,
		"orderCount":             orderCount,
		"finixIdentityId":        finixIdentityID,
		"labels":                 labels,
		"externalId":             externalID,
		"originLocationId":       originLocationID,
		"imported":               false,
		"phone":                  phone,
		"email":                  email,
	}

	// Add optional fields conditionally
	if dotNumber != nil {
		customer["dotNumber"] = *dotNumber
	}
	if referralSourceID != "" {
		customer["referralSourceId"] = referralSourceID
	}
	if lastTimeOrderWorked != nil {
		customer["lastTimeOrderWorked"] = lastTimeOrderWorked.Format(time.RFC3339)
	}
	if metadata != nil {
		customer["metadata"] = metadata
	}
	if customFields != nil {
		customer["customFields"] = customFields
	}

	return customer
}

var loadTestSnowflakeCmd = &cobra.Command{
	Use:   "loadtest-snowflake",
	Short: "Load test data into Snowflake",
	Long:  "Load test data into Snowflake for integration testing",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger.Info("Starting load-snowflake integration test")

		natsURL := mustFlagString(cmd, "nats-url", false)
		if natsURL == "" {
			natsURL = "nats://localhost:4222"
		}
		credsFile := mustFlagString(cmd, "creds", false)
		count := mustFlagInt(cmd, "count", false)
		if count <= 0 {
			count = 1
		}
		seed := mustFlagInt(cmd, "seed", false)
		if seed > 0 {
			rand.Seed(int64(seed))
			logger.Info("Using random seed: %d", seed)
		}

		logger.Info("Connecting to NATS at: %s", natsURL)
		var nc *nats.Conn
		var err error

		if credsFile != "" {
			nc, err = nats.Connect(natsURL, nats.UserCredentials(credsFile))
		} else {
			nc, err = nats.Connect(natsURL)
		}
		if err != nil {
			logger.Fatal("Failed to connect to NATS: %s", err)
		}
		defer nc.Close()
		logger.Info("Connected to NATS successfully")

		js, err := jetstream.New(nc)
		if err != nil {
			logger.Fatal("Failed to create JetStream context: %s", err)
		}
		cid := "28a6712e-83a0-4ede-97cb-c3f5201068dc"
		lid := "5b7a05d6-c971-4f77-8792-9c12744a811d"
		uid := "test-user-789"

		logger.Info("Sending %d customer messages", count)

		for i := 0; i < count; i++ {
			customerID := "customer" + strconv.Itoa(i)
			// Generate random customer data
			customerData := generateRandomCustomer(cid, lid, customerID)

			// Convert customer data to JSON
			customerJSON, err := json.Marshal(customerData)
			if err != nil {
				logger.Fatal("Failed to marshal customer data: %s", err)
			}

			event := internal.DBChangeEvent{
				ID:            util.Hash(time.Now()) + customerID,
				Operation:     "UPDATE",
				Table:         "customer",
				Key:           []string{customerID},
				ModelVersion:  "547388d6b0a76f85",
				CompanyID:     &cid,
				LocationID:    &lid,
				UserID:        &uid,
				Timestamp:     time.Now().UnixMilli(),
				MVCCTimestamp: fmt.Sprintf("%d", time.Now().Nanosecond()),
				After:         json.RawMessage(customerJSON),
				Diff:          []string{"firstName", "lastName", "email"},
				Imported:      false,
			}

			buf, err := json.Marshal(event)
			if err != nil {
				logger.Fatal("Failed to marshal event: %s", err)
			}

			subject := fmt.Sprintf("dbchange.customer.UPDATE.%s.1.PUBLIC.2", *event.CompanyID)
			logger.Info("Publishing event %d/%d to subject: %s", i+1, count, subject)

			msgID := util.Hash(event)
			_, err = js.Publish(context.Background(), subject, buf, jetstream.WithMsgID(msgID))
			if err != nil {
				logger.Fatal("Failed to publish message: %s", err)
			}

			logger.Info("Successfully published customer message %d/%d", i+1, count)
			logger.Info("Message ID: %s", msgID)

			// Small delay between messages to avoid overwhelming the system
			if i < count-1 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		logger.Info("Completed sending %d customer messages to NATS", count)
	},
}

func init() {
	loadTestSnowflakeCmd.Flags().String("nats-url", "", "NATS server URL (default: nats://localhost:4222)")
	loadTestSnowflakeCmd.Flags().String("creds", "", "NATS credentials file path")
	loadTestSnowflakeCmd.Flags().Int("count", 1, "Number of customer messages to send (default: 1)")
	loadTestSnowflakeCmd.Flags().Int("seed", 0, "Seed for random number generation (default: 0)")

	integrationtestCmd.AddCommand(loadTestSnowflakeCmd)
	rootCmd.AddCommand(integrationtestCmd)
}
