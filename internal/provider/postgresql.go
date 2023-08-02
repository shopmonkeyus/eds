package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	"github.com/shopmonkeyus/eds-server/internal/migrator"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

type PostgresProvider struct {
	logger logger.Logger
	url    string
	db     *pgxpool.Pool
	once   sync.Once
	ctx    context.Context
	opts   *ProviderOpts

	modelVersionCache map[string]bool
}

var _ internal.Provider = (*PostgresProvider)(nil)

// NewPostgresProvider returns a provider that will stream files to a folder provided in the url
func NewPostgresProvider(plogger logger.Logger, connString string, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix("[postgresql]")
	logger.Info("starting postgres plugin with connection: %s", connString)
	ctx := context.Background()
	return &PostgresProvider{
		logger: logger,
		url:    connString,
		ctx:    ctx,
		opts:   opts,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *PostgresProvider) Start() error {
	p.logger.Info("start")

	db, err := pgxpool.New(p.ctx, p.url)
	if err != nil {
		p.logger.Error("unable to create connection pool: %w", err)
	}
	p.db = db

	// ensure _migration table
	sql := `CREATE TABLE IF NOT EXISTS _migration (model_version_id text primary key);`
	_, err = p.db.Exec(p.ctx, sql)
	if err != nil {
		return fmt.Errorf("unable to create _migration table: %w", err)
	}
	// fetch all the applied model version ids
	// and we'll use this to decide whether or not to run a diff
	query := `SELECT model_version_id from _migration;`
	rows, err := p.db.Query(p.ctx, query)
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
func (p *PostgresProvider) Stop() error {
	p.logger.Info("stop")
	p.db.Close()
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *PostgresProvider) Process(data datatypes.ChangeEventPayload, schema dm.Model) error {
	if p.opts != nil && p.opts.DryRun {
		p.logger.Info("[dry-run] would write: %v %v", data, schema)
		return nil
	}

	err := p.ensureTableSchema(schema)
	if err != nil {
		p.logger.Error("error ensuring table schema %s", err)
		return err
	}

	err = p.upsertData(data, schema)
	if err != nil {
		p.logger.Error("error updating model %s", err)
		return err
	}
	return nil
}

// upsertData will ensure the table schema is compatible with the incoming message
func (p *PostgresProvider) upsertData(data datatypes.ChangeEventPayload, model dm.Model) error {

	// lookup model for data type
	sql, values, err := p.getSQL(data, model)
	if err != nil {

		return err
	}
	p.logger.Debug("with sql: %s and values: %v", sql, values)
	_, err = p.db.Exec(p.ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("error executing sql: %v", err)
	}

	return nil
}

func (p *PostgresProvider) getSQL(c datatypes.ChangeEventPayload, m dm.Model) (string, []interface{}, error) {
	var sql strings.Builder
	var values []interface{}

	if c.GetOperation() == datatypes.ChangeEventInsert || c.GetOperation() == datatypes.ChangeEventUpdate {

		var sqlColumns, sqlValuePlaceHolder strings.Builder

		data := c.GetAfter()
		p.logger.Debug("after object: %v", data)
		columnCount := 1

		// check if record exists.
		// using explicit check for existance results in much simpler queries
		// vs ON CONFLICT checks. This is also much more portable across db engines
		existsSql := fmt.Sprintf(`SELECT 1 from "%s" where "id"=$1;`, m.Table)

		shouldCreate := false

		var scanned interface{}
		if err := p.db.QueryRow(p.ctx, existsSql, data["id"].(string)).Scan(&scanned); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
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
				sqlValuePlaceHolder.WriteString(fmt.Sprintf(`$%d`, columnCount))
				values = append(values, data[field.Name])

				if i+1 < len(m.Fields) {
					sqlColumns.WriteString(",")
					sqlValuePlaceHolder.WriteString(",")
				}
				columnCount += 1
			}
			sql.WriteString(fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT DO NOTHING`, m.Table, sqlColumns.String(), sqlValuePlaceHolder.String()) + ";\n")
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

				updateColumns.WriteString(fmt.Sprintf(`"%s" = $%d`, field.Name, columnCount))
				updateValues = append(updateValues, data[field.Name])
				if i+1 < len(m.Fields) {
					updateColumns.WriteString(",")
				}
				columnCount += 1
			}
			values = append(values, updateValues...)

			// add the id and version to the values array for safe substitution
			values = append(values, data["id"], c.GetVersion())
			idPlaceholder := fmt.Sprintf(`$%d`, columnCount)
			versionPlaceholder := fmt.Sprintf(`$%d`, columnCount+1)

			sql.WriteString(fmt.Sprintf(`UPDATE "%s" SET %s WHERE "id"=%s AND "meta"->>'version'>%s`, m.Table, updateColumns.String(), idPlaceholder, versionPlaceholder) + ";\n")
		}
	} else if c.GetOperation() == datatypes.ChangeEventDelete {
		data := c.GetBefore()
		p.logger.Debug("before object: %v", data)
		values = append(values, data["id"].(string))
		// TODO: maybe add soft-delete here? meta->>deleted=true, meta->>deletedAt=NOW()?
		sql.WriteString(fmt.Sprintf(`DELETE FROM "%s" WHERE id=$1`, m.Table) + ";\n")
	}

	return sql.String(), values, nil
}

// ensureTableSchema will ensure the table schema is compatible with the incoming message
func (p *PostgresProvider) ensureTableSchema(schema dm.Model) error {
	modelVersionId := fmt.Sprintf("%s-%s", schema.Table, schema.ModelVersion)
	// var dbschema = "public"
	modelVersionFound := p.modelVersionCache[modelVersionId]
	p.logger.Debug("model versions: %v", p.modelVersionCache)
	if modelVersionFound {
		p.logger.Debug("model version already applied: %v", modelVersionId)
		return nil // we've already applied this schema
	} else {
		// do the diff
		p.logger.Debug("start applying model version: %v", modelVersionId)
		if err := migrator.MigrateTable(p.logger, p.db, &schema, schema.Table); err != nil {
			return err
		}
		// update _migration table with the applied model_version_id
		sql := `INSERT INTO _migration ( model_version_id ) VALUES ($1) ON CONFLICT DO NOTHING;`
		_, err := p.db.Exec(p.ctx, sql, modelVersionId)
		if err != nil {
			return fmt.Errorf("error inserting model_version_id into _migration table: %v", err)
		}
		p.modelVersionCache[modelVersionId] = true
		p.logger.Debug("end applying model version: %v", modelVersionId)
	}
	return nil
}
