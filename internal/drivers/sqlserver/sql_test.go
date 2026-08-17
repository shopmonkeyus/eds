package sqlserver

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
)

func TestQuoteValue(t *testing.T) {
	assert.Equal(t, "'test'", quoteValue("test"))
	assert.Equal(t, "'test with a ''hi'''", quoteValue("test with a 'hi'"))
	assert.Equal(t, "1", quoteValue(1))
	assert.Equal(t, "1.1", quoteValue(1.1))
	assert.Equal(t, "1", quoteValue(true))
	assert.Equal(t, "0", quoteValue(false))
	assert.Equal(t, "NULL", quoteValue(nil))
	assert.Equal(t, `'{\"a\":\"b\"}'`, quoteValue(map[string]any{"a": "b"}))
	assert.Equal(t, `'[{\"a\":\"b\"}]'`, quoteValue([]interface{}{map[string]any{"a": "b"}}))
	tv := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, `'2021-01-01 00:00:00'`, quoteValue(tv))
	assert.Equal(t, `'2021-01-01 00:00:00'`, quoteValue(&tv))
	assert.Equal(t, `'2024-07-09 18:28:03.69708'`, quoteValue("2024-07-09T18:28:03.69708Z"))
	assert.Equal(t, `'2024-07-09 18:28:03'`, quoteValue("2024-07-09T18:28:03Z"))
}

func TestQuoteIdentifier(t *testing.T) {
	assert.Equal(t, "[test]", quoteIdentifier("test"))
	assert.Equal(t, "[order]", quoteIdentifier("order"))
	assert.Equal(t, "[current]", quoteIdentifier("current"))
}

func TestQuoteIdentifierAlwaysQuotes(t *testing.T) {
	assert.Equal(t, "[test]", quoteIdentifier("test"))
	assert.Equal(t, "[id]", quoteIdentifier("id"))
	assert.Equal(t, "[name]", quoteIdentifier("name"))
	assert.Equal(t, "[updatedDate]", quoteIdentifier("updatedDate"))
	assert.Equal(t, "[companyId]", quoteIdentifier("companyId"))
	assert.Equal(t, "[order]", quoteIdentifier("order"))
	assert.Equal(t, "[current]", quoteIdentifier("current"))
	assert.Equal(t, "[select]", quoteIdentifier("select"))
	assert.Equal(t, "[number]", quoteIdentifier("number"))
	assert.Equal(t, "[archived]", quoteIdentifier("archived"))
	assert.Equal(t, "[status]", quoteIdentifier("status"))
}

func getOrderSchema() *internal.Schema {
	buf, err := os.ReadFile("../testdata/order_schema_b041c12fbf8d1103.json")
	if err != nil {
		panic(err)
	}
	var schema internal.Schema
	if err := json.Unmarshal(buf, &schema); err != nil {
		panic(err)
	}
	return &schema
}

func TestDBChanges(t *testing.T) {
	schema := getOrderSchema()
	// every non-delete event writes the same full-row merge, regardless of operation
	// or the diff the event carried
	const mergeHash = "64372ece6f3bcb8e"
	var payload = `{"operation":"UPDATE","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"before":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"diff":["updatedDate"]}`
	var dbChange internal.DBChangeEvent
	assert.NoError(t, json.Unmarshal([]byte(payload), &dbChange))
	sql, err := toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, mergeHash, util.Hash(sql))

	payload = `{"operation":"DELETE","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"before":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"diff":["updatedDate"]}`
	err = json.Unmarshal([]byte(payload), &dbChange)
	assert.NoError(t, err)
	sql, err = toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, "DELETE FROM [order] WHERE [id]='zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae';\n", sql)

	payload = `{"operation":"INSERT","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3}}`
	dbChange.Diff = nil
	err = json.Unmarshal([]byte(payload), &dbChange)
	assert.NoError(t, err)
	sql, err = toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, mergeHash, util.Hash(sql))

	payload = `{"operation":"UPDATE","region":"dev","id":"53d366bd86032a5a","timestamp":1720732611708,"mvccTimestamp":"1720732611708587506.0000000000","table":"order","key":["zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae"],"modelVersion":"b041c12fbf8d1103","companyId":"6287a4154d1a72cc5ce091bb","locationId":"6287a4044d1a723b10eff1b0","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162,"before":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-09T18:28:45.162Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3},"after":{"allowCollectPayment":false,"allowCustomerAuthorization":true,"allowCustomerESign":true,"allowCustomerViewActivity":true,"allowCustomerViewAuthorizations":true,"allowCustomerViewInspections":true,"allowCustomerViewMessages":true,"appointmentDates":[],"archived":false,"assignedTechnicianIds":[],"authorized":true,"authorizedDate":null,"coalescedName":"Fuel Pump Replacement","companyId":"6287a4154d1a72cc5ce091bb","complaint":"Car was towed in, it's not starting. ","completedAuthorizedLaborHours":0,"completedDate":null,"completedLaborHours":0,"conversationId":null,"crdb_region":"gcp-us-west1","createdDate":"2024-07-09T18:28:03.69708Z","customFields":null,"customerId":"6287a4384d1a722f13e091ec","deferredServiceCount":0,"deleted":false,"deletedDate":null,"deletedReason":null,"deletedUserId":null,"discountCents":0,"discountPercent":0,"dueDate":null,"emailId":null,"epaCents":0,"externalNumber":null,"feesCents":0,"fullyPaidDate":null,"generatedCustomerName":"Tim Candy","generatedName":null,"generatedVehicleName":"2005 Toyota Tacoma","gstCents":0,"hstCents":0,"id":"zzdb46f9-b4d1-4d53-9a1e-f9a878ff03ae","imported":false,"inspectionCount":0,"inspectionStatus":"None","internalNumber":1004,"invoiced":false,"invoicedDate":null,"labels":[],"laborCents":0,"locationId":"6287a4044d1a723b10eff1b0","messageCount":0,"messagedDate":null,"meta":{"modelVersion":"b041c12fbf8d1103","sessionId":"999","userId":"6287a4044d1a723b10e091b9","version":1720549725162},"metadata":null,"mileageIn":null,"mileageOut":null,"name":"Fuel Pump Replacement","number":"1004","orderCreatedDate":"2024-07-09T18:28:03.69708Z","paid":false,"paidCostCents":46700,"partsCents":0,"paymentDueDate":null,"paymentTermId":"280d1021-90db-4f98-aa7a-e1b95f78ffa2","phoneNumberId":null,"profitability":{"labor":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"parts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"subcontracts":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"tires":{"discountCents":0,"discountPercent":0,"profitCents":0,"profitPercent":0,"retailCents":0,"wholesaleCents":0},"totalDiscountCents":0,"totalDiscountPercent":0,"totalProfitCents":0,"totalProfitPercent":0,"totalRetailCents":0,"totalWholesaleCents":0},"pstCents":0,"publicId":"7d3fc9c2-5c72-47ef-bb2f-d83d9453c3c3","purchaseOrderNumber":null,"readOnly":false,"readOnlyReason":null,"recommendation":null,"remainingCostCents":0,"repairOrderDate":null,"requestedDepositCents":0,"requireESignatureOnAuthorization":false,"requireESignatureOnInvoice":false,"sentToCarfax":false,"serviceWriterId":null,"shopSuppliesCents":0,"shopUnreadMessageCount":0,"statementId":null,"status":"Estimate","subcontractsCents":0,"surchargingEnabled":false,"taxCents":0,"taxConfigId":"205bdb43-6a25-4c55-a7de-21428f463c03","tiresCents":0,"totalAuthorizedLaborHours":0,"totalCostCents":0,"totalLaborHours":0,"transactionFeeConfigId":null,"transactionalFeeSubtotalCents":0,"transactionalFeeTotalCents":0,"updatedDate":"2024-07-11T21:16:51.70856Z","updatedSinceSignedInvoice":false,"vehicleId":"6287a4384d1a72a512e091f9","workflowStatusDate":"2024-07-09T18:28:03.69708Z","workflowStatusId":"35a3ab48-1a54-4633-9da4-947c80177a45","workflowStatusPosition":1E+3}}`
	err = json.Unmarshal([]byte(payload), &dbChange)
	assert.NoError(t, err)
	dbChange.Diff = nil
	sql, err = toSQL(dbChange, schema)
	assert.NoError(t, err)
	t.Log(sql)
	assert.Equal(t, mergeHash, util.Hash(sql))
}

func TestParseDSN(t *testing.T) {
	dsn, err := ParseURLToDSN("sqlserver://root:password@localhost:3306/eds")
	assert.NoError(t, err)
	assert.Equal(t, "sqlserver://root:password@localhost:3306?app+name=eds&database=eds&encrypt=disable", dsn)
	dsn, err = ParseURLToDSN("sqlserver://root:password@localhost:3306/eds?encrypt=enable")
	assert.NoError(t, err)
	assert.Equal(t, "sqlserver://root:password@localhost:3306?app+name=eds&database=eds&encrypt=enable", dsn)
	dsn, err = ParseURLToDSN("sqlserver://root:password@foo.microsoft.com:3306/eds")
	assert.NoError(t, err)
	assert.Equal(t, "sqlserver://root:password@foo.microsoft.com:3306?app+name=eds&database=eds", dsn)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]any
		expectedURL string
		expectError bool
	}{
		{
			name:        "minimal config",
			config:      map[string]any{"Database": "db", "Hostname": "hostname"},
			expectedURL: "sqlserver://hostname:1433/db",
		},
		{
			name:        "missing required field Hostname",
			config:      map[string]any{"Database": "db", "Port": 1433},
			expectError: true,
		},
	}

	var driver sqlserverDriver
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, errs := driver.Validate(tt.config)
			if tt.expectError {
				assert.GreaterOrEqual(t, len(errs), 1)
				assert.Equal(t, "", url)
			} else {
				assert.Empty(t, errs)
				assert.Equal(t, tt.expectedURL, url)
			}
		})
	}
}

func TestAddNewColumnsSQL(t *testing.T) {
	logger := logger.NewConsoleLogger()
	detail := getOrderSchema()
	sql := addNewColumnsSQL(logger, []string{"number", "internalNumber", "externalNumber"}, detail, make(internal.DatabaseSchema))
	assert.Equal(t, []string{"ALTER TABLE [order] ADD [number] NVARCHAR(MAX) NULL;", "ALTER TABLE [order] ADD [internalNumber] NVARCHAR(MAX) NULL;", "ALTER TABLE [order] ADD [externalNumber] NVARCHAR(MAX) NULL;"}, sql)
}

func TestAddNewColumnsSQLSkip(t *testing.T) {
	logger := logger.NewConsoleLogger()
	detail := getOrderSchema()
	sql := addNewColumnsSQL(logger, []string{"number", "internalNumber", "externalNumber"}, detail, internal.DatabaseSchema{"order": {"number": "NVARCHAR(MAX)"}})
	assert.Equal(t, []string{"ALTER TABLE [order] ADD [internalNumber] NVARCHAR(MAX) NULL;", "ALTER TABLE [order] ADD [externalNumber] NVARCHAR(MAX) NULL;"}, sql)
}

func getSmallSchema() *internal.Schema {
	return &internal.Schema{
		Table:       "order",
		PrimaryKeys: []string{"id"},
		Required:    []string{"id", "name"},
		Properties: map[string]internal.SchemaProperty{
			"id":               {Type: "string"},
			"name":             {Type: "string"},
			"appointmentDates": {Type: "array", Items: &internal.ItemsType{Type: "string"}},
			"labels":           {Type: "object"},
			"metadata":         {Type: "object", Nullable: true},
			"updatedDate":      {Type: "string", Format: "date-time"},
		},
	}
}

// every column is written on every merge, whether or not the event carried a value
// for it, and a matched row is only overwritten by a newer updatedDate
func TestMergeWritesAllColumns(t *testing.T) {
	sql := toSQLFromObject(getSmallSchema(), "order", map[string]any{"id": "1", "updatedDate": "2024-07-11T21:16:51.70856Z"})
	assert.Equal(t, `MERGE [order] AS target USING (VALUES('1','2024-07-11 21:16:51.70856')) AS source ([id],[updatedDate]) ON target.[id]=source.[id]`+
		` WHEN MATCHED AND (target.[updatedDate] IS NULL OR source.[updatedDate]>target.[updatedDate]) THEN UPDATE SET`+
		` [appointmentDates]='[]',[labels]='{}',[metadata]=NULL,[name]=NULL,[updatedDate]='2024-07-11 21:16:51.70856'`+
		` WHEN NOT MATCHED THEN INSERT ([id],[appointmentDates],[labels],[metadata],[name],[updatedDate])`+
		` VALUES ('1','[]','{}',NULL,NULL,'2024-07-11 21:16:51.70856');`, sql)
}

// a model without an updatedDate column has no way to detect stale events, so the
// merge must fall back to an unguarded update rather than emit invalid SQL
func TestMergeWithoutUpdatedDate(t *testing.T) {
	schema := &internal.Schema{
		Table:       "customer",
		PrimaryKeys: []string{"id"},
		Properties: map[string]internal.SchemaProperty{
			"id":   {Type: "string"},
			"name": {Type: "string"},
		},
	}
	sql := toSQLFromObject(schema, "customer", map[string]any{"id": "1", "name": "test"})
	assert.Equal(t, `MERGE [customer] AS target USING (VALUES('1')) AS source ([id]) ON target.[id]=source.[id] WHEN MATCHED THEN UPDATE SET [name]='test' WHEN NOT MATCHED THEN INSERT ([id],[name]) VALUES ('1','test');`, sql)
}

// non-key columns are always nullable, even the ones the schema marks as required
func TestCreateSQLIsNullable(t *testing.T) {
	expected := "DROP TABLE IF EXISTS [order];\n" +
		"CREATE TABLE [order] (\n" +
		"\t[id] VARCHAR(64),\n" +
		"\t[appointmentDates] NVARCHAR(MAX) NULL,\n" +
		"\t[labels] NVARCHAR(MAX) NULL,\n" +
		"\t[metadata] NVARCHAR(MAX) NULL,\n" +
		"\t[name] NVARCHAR(MAX) NULL,\n" +
		"\t[updatedDate] NVARCHAR(MAX) NULL,\n" +
		"\tPRIMARY KEY ([id])\n)"
	assert.Equal(t, expected, createSQL(getSmallSchema()))
}
