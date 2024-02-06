package provider

import (
	"context"
	"database/sql"
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
	schemaModelCache  map[string]dm.Model
}

var _ internal.Provider = (*SqlServerProvider)(nil)

func NewSqlServerProvider(plogger logger.Logger, connString string, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix("[sqlserver]")
	logger.Info("starting mssql plugin connection with connection string: %s", util.MaskConnectionString(connString))
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
	p.schemaModelCache = make(map[string]dm.Model, 0)

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

func (p *SqlServerProvider) Import(dataMap map[string]interface{}, tableName string, nc *nats.Conn) error {

	var schema dm.Model
	var err error
	if tableName == "" {
		badImportDataMessage := "Empty table name provided. Check the file name being imported."
		badImportDataError := errors.New(badImportDataMessage)
		p.logger.Error(fmt.Sprintf(badImportDataMessage+" %s", dataMap))
		return badImportDataError
	}

	schema, schemaFound := p.schemaModelCache[tableName]
	if !schemaFound {
		schema, err = GetLatestSchema(p.logger, nc, tableName)
		if err != nil {
			return err
		}
		p.schemaModelCache[tableName] = schema
	}

	err = p.ensureTableSchema(schema)
	if err != nil {
		return err
	}

	sql, values, err := p.importSQL(dataMap, schema)
	if sql == "" {
		p.logger.Debug("no sql to run")
		return nil
	}
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

func (p *SqlServerProvider) importSQL(data map[string]interface{}, m dm.Model) (string, []interface{}, error) {
	var query strings.Builder
	var values []interface{}

	var sqlColumns, sqlValuePlaceHolder strings.Builder

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
		//TODO: Handle conflicts?
		query.WriteString(fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, m.Table, sqlColumns.String(), sqlValuePlaceHolder.String()) + ";\n")
	} else {
		p.logger.Info("Record is already in the database, skipping import")
		return "", nil, nil
	}

	return query.String(), values, nil
}

// upsertData will ensure the table schema is compatible with the incoming message
func (p *SqlServerProvider) upsertData(data datatypes.ChangeEventPayload, model dm.Model) error {

	// lookup model for data type
	sql, values, err := p.getSQL(data, model)
	if err != nil {

		return err
	}
	if sql == "" {
		p.logger.Debug("no sql to execute- we found this record in the db already")
		return nil
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

			query.WriteString(fmt.Sprintf(`UPDATE "%s" SET %s WHERE "id"=%s AND JSON_VALUE(meta, '$.version') < %s`, m.Table, updateColumns.String(), idPlaceholder, versionPlaceholder) + ";\n")
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
