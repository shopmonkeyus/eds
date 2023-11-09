package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	"github.com/shopmonkeyus/eds-server/internal/migrator"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type SqlServerProvider struct {
	logger            logger.Logger
	url               string
	db                *sql.DB
	ctx               context.Context
	opts              *ProviderOpts
	schema            string
	modelVersionCache map[string]bool
}

var _ internal.Provider = (*SqlServerProvider)(nil)

func NewSqlServerProvider(plogger logger.Logger, connString string, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix("[sqlserver]")
	logger.Info("starting mssql plugin with connection: %s", connString)
	ctx := context.Background()
	return &SqlServerProvider{
		logger: logger,
		url:    connString,
		ctx:    ctx,
		opts:   opts,
		schema: "dbo",
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *SqlServerProvider) Start() error {
	p.logger.Info("start")

	db, err := sql.Open("sqlserver", p.url)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	p.db = db

	// ensure _migration table
	sql := `
		IF OBJECT_ID(N'_migration', N'U') IS NULL
		CREATE TABLE _migration (
			model_version_id varchar(500) primary key
		);
	`

	_, err = p.db.Exec(sql)
	if err != nil {
		return fmt.Errorf("unable to create _migration table: %w", err)
	}
	// fetch all the applied model version ids
	// and we'll use this to decide whether or not to run a diff
	query := `SELECT model_version_id from _migration;`
	rows, err := p.db.Query(query)
	if err != nil {
		return fmt.Errorf("unable to fetch modelVersionIds from _migration table: %w", err)
	}
	p.modelVersionCache = make(map[string]bool, 0)

	defer rows.Close()

	for rows.Next() {
		var modelVersionId string
		err := rows.Scan(&modelVersionId)
		if err != nil {
			return fmt.Errorf("unable to fetch modelVersionId from _migration table: %w", err)
		}
		p.modelVersionCache[modelVersionId] = true
	}

	return nil
}

// Stop the provider and return an error or nil if ok
func (p *SqlServerProvider) Stop() error {
	p.logger.Info("stop")
	p.db.Close()
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *SqlServerProvider) Process(data datatypes.ChangeEventPayload, schema dm.Model) error {
	if p.opts != nil && p.opts.DryRun {
		p.logger.Info("[dry-run] would write: %v %v", data, schema)
		return nil
	}

	err := p.ensureTableSchema(schema)
	if err != nil {
		return err
	}

	err = p.upsertData(data, schema)
	if err != nil {
		return err
	}
	return nil
}

func (p *SqlServerProvider) populateSchemaMap() (map[string]dm.Model, error) {
	userSchemaJson := []byte(`{"data":{"name":"Payment","table":"payment","fields":[{"line":1747,"name":"id","table":"id","type":"String","primary_key":true,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"gen_random_uuid()::STRING","comment":"","attributes":[{"name":"id","arg":""},{"name":"default","arg":"uuid()"}],"api_schema":{"omit":false,"calculated":false}},{"line":1748,"name":"crdb_region","table":"crdb_region","type":"crdb_internal_region","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":true,"list":false,"enum":true,"scalar":false,"upgrade":false,"default":"default_to_database_primary_region(gateway_region())::dev.public.crdb_internal_region","comment":"","annotations":[{"name":"api_schema","arg":"omit"},{"name":"private","arg":""}],"api_schema":{"omit":true,"calculated":false}},{"line":1749,"name":"meta","table":"meta","type":"Json","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1750,"name":"metadata","table":"metadata","type":"Json","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"metadata reserved for customers to control","api_schema":{"omit":false,"calculated":false}},{"line":1751,"name":"createdDate","table":"createdDate","type":"DateTime","primary_key":false,"unique":false,"optional":false,"timestampz":true,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"current_timestamp():::TIMESTAMPTZ","comment":"","annotations":[{"name":"entity_change","arg":""}],"attributes":[{"name":"default","arg":"now()"},{"name":"db.Timestamptz","arg":"6"}],"api_schema":{"omit":false,"calculated":false}},{"line":1752,"name":"updatedDate","table":"updatedDate","type":"DateTime","primary_key":false,"unique":false,"optional":true,"timestampz":true,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"current_timestamp():::TIMESTAMPTZ","comment":"","annotations":[{"name":"entity_change","arg":""}],"attributes":[{"name":"default","arg":"now()"},{"name":"db.Timestamptz","arg":"6"}],"api_schema":{"omit":false,"calculated":false}},{"line":1753,"name":"companyId","table":"companyId","type":"String","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1754,"name":"locationId","table":"locationId","type":"String","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1755,"name":"orderId","table":"orderId","type":"String","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1756,"name":"payerId","table":"payerId","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1757,"name":"statementId","table":"statementId","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1758,"name":"recordedDate","table":"recordedDate","type":"DateTime","primary_key":false,"unique":false,"optional":true,"timestampz":true,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"the date that the payment was recorded","attributes":[{"name":"db.Timestamptz","arg":"6"}],"api_schema":{"omit":false,"calculated":false}},{"line":1759,"name":"note","table":"note","type":"String","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"'':::STRING","comment":"","attributes":[{"name":"default","arg":"\"\""}],"api_schema":{"omit":false,"calculated":false}},{"line":1760,"name":"userData","table":"userData","type":"Json","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1761,"name":"status","table":"status","type":"PaymentStatus","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1762,"name":"paymentType","table":"paymentType","type":"PaymentType","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1763,"name":"paymentMode","table":"paymentMode","type":"PaymentMode","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1764,"name":"transactionType","table":"transactionType","type":"PaymentTransactionType","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1765,"name":"amountCents","table":"amountCents","type":"Int","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"amount charged or refuned","api_schema":{"omit":false,"calculated":false}},{"line":1766,"name":"refundedAmountCents","table":"refundedAmountCents","type":"Int","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"amount refunded for charge transactions","api_schema":{"omit":false,"calculated":false}},{"line":1767,"name":"refunded","table":"refunded","type":"Boolean","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1768,"name":"refundReason","table":"refundReason","type":"RefundReason","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1769,"name":"chargeId","table":"chargeId","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"reference for original payment if transaction type is different from charge","api_schema":{"omit":false,"calculated":false}},{"line":1770,"name":"bulk","table":"bulk","type":"Boolean","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"false","comment":"","attributes":[{"name":"default","arg":"false"}],"api_schema":{"omit":false,"calculated":false}},{"line":1771,"name":"deposit","table":"deposit","type":"Boolean","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"false","comment":"","attributes":[{"name":"default","arg":"false"}],"api_schema":{"omit":false,"calculated":false}},{"line":1772,"name":"checkNumber","table":"checkNumber","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1773,"name":"cardType","table":"cardType","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1774,"name":"cardDigits","table":"cardDigits","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1775,"name":"cardName","table":"cardName","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1776,"name":"cardConfirmation","table":"cardConfirmation","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1777,"name":"debitCard","table":"debitCard","type":"Boolean","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"false","comment":"","attributes":[{"name":"default","arg":"false"}],"api_schema":{"omit":false,"calculated":false}},{"line":1778,"name":"chargeFromPublicPage","table":"chargeFromPublicPage","type":"Boolean","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"default":"false","comment":"","attributes":[{"name":"default","arg":"false"}],"api_schema":{"omit":false,"calculated":false}},{"line":1779,"name":"provider","table":"provider","type":"PaymentProvider","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1780,"name":"providerData","table":"providerData","type":"Json","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1781,"name":"receiptNumber","table":"receiptNumber","type":"Int","primary_key":false,"unique":false,"optional":false,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1782,"name":"providerFee","table":"providerFee","type":"Int","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"SM payments fee amount in cents on top of Stripe fee","api_schema":{"omit":false,"calculated":false}},{"line":1783,"name":"transactionalFeeAmountCents","table":"transactionalFeeAmountCents","type":"Int","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"amount charged as SM transactional fee (service fee)","api_schema":{"omit":false,"calculated":false}},{"line":1784,"name":"disputedPaymentId","table":"disputedPaymentId","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"if a payment is a deduction/correction of a disputed payment, here we store link to that original disputed payment","api_schema":{"omit":false,"calculated":false}},{"line":1785,"name":"disputedStatus","table":"disputedStatus","type":"PaymentDisputedStatus","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1786,"name":"disputedReason","table":"disputedReason","type":"String","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":false,"scalar":true,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}},{"line":1787,"name":"disputedType","table":"disputedType","type":"PaymentDisputedType","primary_key":false,"unique":false,"optional":true,"timestampz":false,"private":false,"list":false,"enum":true,"scalar":false,"upgrade":false,"comment":"","api_schema":{"omit":false,"calculated":false}}],"comment":"","annotations":[{"name":"dao","arg":"omit=createMany"},{"name":"public","arg":""},{"name":"changefeed","arg":"group=payment"}],"public":true,"soft_delete":false,"generated":false,"cockroach":{"locality":"regional_by_row"},"changefeed":{"group":"payment"},"entity_change":{"create":true,"delete":true,"update":{"updatedDate":true}},"cache":{"ttl":1800000},"api_schema":{"generate":true},"relations":[{"type":"","name":"","fields":["companyId"],"references":["id"],"on_delete":"Cascade","field_name":"company","field_type":"Company","list":false,"optional":false},{"type":"","name":"","fields":["locationId"],"references":["id"],"on_delete":"Cascade","field_name":"location","field_type":"Location","list":false,"optional":false},{"type":"","name":"","fields":["orderId"],"references":["id"],"on_delete":"Cascade","field_name":"order","field_type":"Order","list":false,"optional":false},{"type":"","name":"","fields":["statementId"],"references":["id"],"on_delete":"SetNull","field_name":"statement","field_type":"Statement","list":false,"optional":true},{"type":"","name":"","fields":["payerId"],"references":["id"],"on_delete":"SetNull","field_name":"payer","field_type":"Customer","list":false,"optional":true},{"type":"","name":"disputedPayment","fields":["disputedPaymentId"],"references":["id"],"on_delete":"SetNull","field_name":"disputedPayment","field_type":"Payment","list":false,"optional":true},{"type":"","name":"disputedPayment","fields":null,"references":null,"field_name":"disputedDeductions","field_type":"Payment","list":true,"optional":false}],"constraints":[{"table":"payment","columns":["orderId","crdb_region"],"reference_table":"order","reference_columns":["id","crdb_region"],"on_delete":"Cascade"},{"table":"payment","columns":["statementId","crdb_region"],"reference_table":"statement","reference_columns":["id","crdb_region"],"on_delete":"SetDefault"},{"table":"payment","columns":["payerId","crdb_region"],"reference_table":"customer","reference_columns":["id","crdb_region"],"on_delete":"SetDefault"},{"table":"payment","columns":["companyId"],"reference_table":"company","reference_columns":["id"],"on_delete":"Cascade"},{"table":"payment","columns":["locationId"],"reference_table":"location","reference_columns":["id"],"on_delete":"Cascade"}],"related":[{"name":"quickbooksRefs","type":"QuickbooksPaymentRef","list":true,"optional":false}],"uniques":[{"fields":["companyId","locationId","receiptNumber"],"expression":""}],"modelVersion":"3f73a05719b3c5bc"},"success":true}`)
	var userSchema datatypes.SchemaResponse
	err := json.Unmarshal(userSchemaJson, &userSchema)
	if err != nil {
		return nil, err
	}
	populatedSchemaMap := map[string]dm.Model{
		"user": userSchema.Data,
	}

	return populatedSchemaMap, nil
}

func (p *SqlServerProvider) Import(data []byte, nc *nats.Conn) error {

	//TODO: Create cases for when the schema isn't up to date, or when the schema hasn't been created

	//Temporary code, figure out how to get the schema from the incoming message
	schemas, err := p.populateSchemaMap()
	if err != nil {
		return err
	}
	gzipped := false
	changeEventData, _ := datatypes.FromChangeEvent(data, gzipped)
	// END FILLER CODE
	// lookup model for data type
	sql, values, err := p.getSQL(changeEventData, schemas[changeEventData.Table])
	if err != nil {

		return err
	}
	p.logger.Debug("with sql: %s and values: %v", sql, values)
	_, err = p.db.Exec(sql, values...)
	if err != nil {
		return err
	}

	return nil
}

// upsertData will ensure the table schema is compatible with the incoming message
func (p *SqlServerProvider) upsertData(data datatypes.ChangeEventPayload, model dm.Model) error {

	// lookup model for data type
	sql, values, err := p.getSQL(data, model)
	if err != nil {

		return err
	}
	p.logger.Debug("with sql: %s and values: %v", sql, values)
	_, err = p.db.Exec(sql, values...)
	if err != nil {
		return err
	}

	return nil
}

func (p *SqlServerProvider) getSQL(c datatypes.ChangeEventPayload, m dm.Model) (string, []interface{}, error) {
	var query strings.Builder
	var values []interface{}

	if c.GetOperation() == datatypes.ChangeEventInsert || c.GetOperation() == datatypes.ChangeEventUpdate {

		var sqlColumns, sqlValuePlaceHolder strings.Builder

		data := c.GetAfter()
		p.logger.Debug("after object: %v", data)
		columnCount := 1

		// check if record exists.
		// using explicit check for existance results in much simpler queries
		// vs ON CONFLICT checks. This is also much more portable across db engines
		existsSql := fmt.Sprintf(`SELECT 1 from "%s" where "id"=@p1;`, m.Table)

		var shouldCreate bool

		var scanned interface{}
		if err := p.db.QueryRow(existsSql, data["id"].(string)).Scan(&scanned); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				p.logger.Debug("no rows found for: %s, %s", m.Table, data["id"])
				shouldCreate = true
			} else {
				return "", nil, fmt.Errorf("error checking existance: %s, %s, %v", m.Table, data["id"], err)

			}
		}

		if shouldCreate {
			for i, field := range m.Fields {
				// check if field is in payload
				if _, ok := data[field.Name]; !ok {
					continue
				}
				// if yes, then add column
				sqlColumns.WriteString(fmt.Sprintf(`"%s"`, field.Name))
				sqlValuePlaceHolder.WriteString(fmt.Sprintf(`@p%d`, columnCount))
				val, err := util.TryConvertJson(field.Type, data[field.Name])
				if err != nil {
					return "", nil, err
				}

				values = append(values, val)

				if i+1 < len(m.Fields) {
					sqlColumns.WriteString(",")
					sqlValuePlaceHolder.WriteString(",")
				}
				columnCount += 1
			}

			query.WriteString(fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, m.Table, sqlColumns.String(), sqlValuePlaceHolder.String()) + ";\n")
		} else {
			var updateColumns strings.Builder
			var updateValues []interface{}
			data := c.GetAfter()
			p.logger.Debug("after object: %v", data)
			columnCount := 1

			for i, field := range m.Fields {
				// check if field is in payload since we do not drop columns automatically
				if _, ok := data[field.Name]; !ok {
					continue
				}
				if field.Name == "id" {
					// can't update the id!
					continue
				}

				updateColumns.WriteString(fmt.Sprintf(`"%s" = @p%d`, field.Name, columnCount))

				val, err := util.TryConvertJson(field.Type, data[field.Name])
				if err != nil {
					return "", nil, err
				}

				updateValues = append(updateValues, val)
				if i+1 < len(m.Fields) {
					updateColumns.WriteString(",")
				}
				columnCount += 1
			}
			values = append(values, updateValues...)

			// add the id and version to the values array for safe substitution
			values = append(values, data["id"].(string), c.GetVersion())
			idPlaceholder := fmt.Sprintf(`@p%d`, columnCount)
			versionPlaceholder := fmt.Sprintf(`@p%d`, columnCount+1)

			query.WriteString(fmt.Sprintf(`UPDATE "%s" SET %s WHERE "id"=%s AND JSON_VALUE(meta, '$.version') >%s`, m.Table, updateColumns.String(), idPlaceholder, versionPlaceholder) + ";\n")
		}
	} else if c.GetOperation() == datatypes.ChangeEventDelete {
		data := c.GetBefore()
		p.logger.Debug("before object: %v", data)
		values = append(values, data["id"].(string))
		// TODO: maybe add soft-delete here? meta->>deleted=true, meta->>deletedAt=NOW()?
		query.WriteString(fmt.Sprintf(`DELETE FROM "%s" WHERE id=@p1`, m.Table) + ";\n")
	}

	return query.String(), values, nil
}

// ensureTableSchema will ensure the table schema is compatible with the incoming message
func (p *SqlServerProvider) ensureTableSchema(schema dm.Model) error {
	modelVersionId := fmt.Sprintf("%s-%s", schema.Table, schema.ModelVersion)
	modelVersionFound := p.modelVersionCache[modelVersionId]
	p.logger.Debug("model versions: %v", p.modelVersionCache)
	if modelVersionFound {
		p.logger.Debug("model version already applied: %v", modelVersionId)
		return nil // we've already applied this schema
	} else {
		// do the diff
		p.logger.Debug("start applying model version: %v", modelVersionId)
		if err := migrator.MigrateTable(p.logger, p.db, &schema, schema.Table, p.schema, util.Sqlserver); err != nil {
			return err
		}
		// update _migration table with the applied model_version_id
		sql := `INSERT INTO _migration ( model_version_id ) VALUES (@p1);`
		_, err := p.db.Exec(sql, modelVersionId)
		if err != nil {
			return fmt.Errorf("error inserting model_version_id into _migration table: %v", err)
		}
		p.modelVersionCache[modelVersionId] = true
		p.logger.Debug("end applying model version: %v", modelVersionId)
	}
	return nil
}
