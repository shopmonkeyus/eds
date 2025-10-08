package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

func generateRandomCustomer(companyID, locationID string, customerID string) map[string]any {
	now := time.Now()
	publicID := uuid.New().String()

	customerTypes := []string{"Individual", "Fleet"}
	customerType := customerTypes[rand.Intn(len(customerTypes))]

	countries := []string{"US", "CA", "MX"}
	country := countries[rand.Intn(len(countries))]

	contactMethods := []string{"Email", "Phone", "SMS"}
	preferredContactMethod := contactMethods[rand.Intn(len(contactMethods))]

	languages := []string{"en_US", "es_US", "fr_CA"}
	preferredLanguage := languages[rand.Intn(len(languages))]

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

	var dotNumber *string
	if customerType == "Fleet" {
		dot := fmt.Sprintf("%06d", rand.Intn(1000000))
		dotNumber = &dot
	}

	paymentTermID := uuid.New().String()
	laborRateID := uuid.New().String()
	laborMatrixID := uuid.New().String()
	pricingMatrixID := uuid.New().String()
	fleetID := uuid.New().String()
	referralSourceID := uuid.New().String()
	finixIdentityID := uuid.New().String()
	externalID := uuid.New().String()
	originLocationID := locationID

	statementCount := rand.Intn(100)
	transactionCount := rand.Intn(500)
	deferredServiceCount := rand.Intn(50)
	appointmentCount := rand.Intn(200)
	messageCount := rand.Intn(300)
	vehicleCount := rand.Intn(20)
	orderCount := rand.Intn(100)

	discountPercent := rand.Float64() * 25.0 // 0-25%

	createdDate := now.Add(-time.Duration(rand.Intn(365*24*60*60)) * time.Second) // Random date within last year
	updatedDate := time.Now()

	var lastTimeOrderWorked *time.Time
	if rand.Float32() > 0.3 { // 70% chance of having a last order time
		lastOrderTime := createdDate.Add(time.Duration(rand.Intn(int(time.Since(createdDate).Seconds()))) * time.Second)
		lastTimeOrderWorked = &lastOrderTime
	}

	taxExempt := rand.Float32() > 0.8 // 20% chance of being tax exempt
	gstExempt := rand.Float32() > 0.8 // 20% chance of being GST exempt
	hstExempt := rand.Float32() > 0.8 // 20% chance of being HST exempt
	pstExempt := rand.Float32() > 0.8 // 20% chance of being PST exempt

	marketingOptIn := rand.Float32() > 0.7 // 30% chance of marketing opt-in

	note := fmt.Sprintf("Note%d", rand.Intn(1000))

	locationIDs := []string{locationID}
	if rand.Float32() > 0.5 { // 50% chance of having additional locations
		additionalLocations := rand.Intn(3) + 1
		for i := 0; i < additionalLocations; i++ {
			locationIDs = append(locationIDs, uuid.New().String())
		}
	}

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

	var customFields map[string]interface{}
	if rand.Float32() > 0.6 { // 40% chance of having custom fields
		customFields = map[string]interface{}{
			"favoriteColor":       fmt.Sprintf("color%d", rand.Intn(1000)),
			"preferredContact":    fmt.Sprintf("contact%d", rand.Intn(1000)),
			"specialInstructions": fmt.Sprintf("Instructions %d", rand.Intn(1000)),
		}
	}

	labels := []map[string]interface{}{
		{"name": fmt.Sprintf("Label%d", rand.Intn(1000)), "color": "#FFD700"},
	}

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

func PublishRandomMessages(js jetstream.JetStream, count int, log logger.Logger) int {
	cid := "28a6712e-83a0-4ede-97cb-c3f5201068dc"
	lid := "5b7a05d6-c971-4f77-8792-9c12744a811d"
	uid := "test-user-789"

	delivered := 0
	for i := 0; i < count; i++ {

		customerID := "customer" + strconv.Itoa(i)

		customerData := generateRandomCustomer(cid, lid, customerID)

		customerJSON, err := json.Marshal(customerData)
		if err != nil {
			log.Fatal("Failed to marshal customer data: %s", err)
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
			MVCCTimestamp: fmt.Sprintf("%d", time.Now().UnixNano()),
			After:         json.RawMessage(customerJSON),
			Diff:          []string{"firstName", "lastName", "email"},
			Imported:      false,
		}

		buf, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}
		msgID := util.Hash(event)

		subject := fmt.Sprintf("dbchange.customer.UPDATE.%s.1.PUBLIC.2", *event.CompanyID)
		if _, err := js.Publish(context.Background(), subject, buf, jetstream.WithMsgID(msgID)); err != nil {
			panic(err)
		}

		log.Info("Publishing event %d/%d to subject: %s", i+1, count, subject)

		log.Info("Successfully published customer message %d/%d", i+1, count)

		if i < count-1 {
			time.Sleep(100 * time.Millisecond)
		}
		delivered++
	}
	return delivered
}
