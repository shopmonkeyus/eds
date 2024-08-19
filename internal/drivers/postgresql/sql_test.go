package postgresql

import (
	"encoding/json"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/registry"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestDBChanges(t *testing.T) {
	registery, err := registry.NewAPIRegistry("http://api.shopmonkey.cloud")
	assert.NoError(t, err)

	var payload = `{"operation":"UPDATE","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["gcp-us-west1","zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"before":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"diff":["updatedDate"]}`
	var dbChange internal.DBChangeEvent
	err = json.Unmarshal([]byte(payload), &dbChange)
	assert.NoError(t, err)
	schema, err := registery.GetLatestSchema()
	assert.NoError(t, err)
	sql, err := toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, "41bd0b37d05eab4c", util.Hash(sql))

	payload = `{"operation":"DELETE","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["gcp-us-west1","zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"before":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"diff":["updatedDate"]}`
	err = json.Unmarshal([]byte(payload), &dbChange)
	assert.NoError(t, err)
	sql, err = toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, "DELETE FROM \"order\" WHERE id='zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae';\n", sql)
}

func TestDBConnectionString(t *testing.T) {
	val, err := getConnectionStringFromURL("postgres://localhost")
	assert.NoError(t, err)
	assert.Equal(t, "postgresql://localhost:5432?application_name=eds", val)
	val, err = getConnectionStringFromURL("postgres://localhost:15432")
	assert.NoError(t, err)
	assert.Equal(t, "postgresql://localhost:15432?application_name=eds", val)
	val, err = getConnectionStringFromURL("postgres://localhost:15432?application_name=foo")
	assert.NoError(t, err)
	assert.Equal(t, "postgresql://localhost:15432?application_name=foo", val)
}

func TestValidate(t *testing.T) {
	var driver postgresqlDriver
	url, err := driver.Validate(map[string]any{
		"Database": "db",
		"Hostname": "hostname",
	})
	assert.Empty(t, err)
	assert.Equal(t, "postgres://hostname:5432/db", url)

	url, err = driver.Validate(map[string]any{
		"Database": "db",
		"Hostname": "hostname",
		"Port":     1234,
	})
	assert.Empty(t, err)
	assert.Equal(t, "postgres://hostname:1234/db", url)

	url, err = driver.Validate(map[string]any{
		"Database": "db",
		"Hostname": "hostname",
		"Port":     1234,
		"Username": "user",
	})
	assert.Empty(t, err)
	assert.Equal(t, "postgres://user:@hostname:1234/db", url)

	url, err = driver.Validate(map[string]any{
		"Database": "db",
		"Hostname": "hostname",
		"Port":     1234,
		"Username": "user",
		"Password": "pass",
	})
	assert.Empty(t, err)
	assert.Equal(t, "postgres://user:pass@hostname:1234/db", url)
}
