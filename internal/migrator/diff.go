package migrator

import (
	"fmt"
	"io"
	"strings"

	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

func (c *ModelChange) Format(name string, format string, writer io.Writer, dialect util.Dialect) {
	if format == "sql" {
		sql := c.SQL(dialect)
		writer.Write([]byte(sql))
	} else {
		switch c.Action {
		case AddAction:
			WriteAddedHeader(writer, name, "table")
		case UpdateAction:
			WriteChangeHeader(writer, name, "table")
			for _, fchange := range c.FieldChanges {
				switch fchange.Action {
				case DeleteAction:
					WriteRemovedLine(writer, "column", fchange.Name, fchange.Detail)
				case AddAction:
					WriteAddedLine(writer, "column", fchange.Name, fchange.Detail)
				case UpdateAction:
					WriteAlterLine(writer, "column", fchange.Name, fchange.Detail)
				}
			}
		}
	}
}

func diffModels(columns []Column, model *dm.Model, dialect util.Dialect) (bool, *ModelChange, error) {
	needToAdd := make(map[string]*dm.Model)
	var change = &ModelChange{
		Table:        model.Table,
		Model:        model,
		Action:       NoAction,
		FieldChanges: make([]FieldChange, 0),
	}
	var hasTypeChanges bool
	foundcolumns := make(map[string]bool)
	for _, column := range columns {
		foundcolumns[column.Name] = true
		var action Action
		var field *dm.Field
		details := make([]string, 0)
		var typeChanged bool
		var originalType string
		var newType string
		field = findField(model, column.Name)
		//The column data types in information_schema.columns (schema) get stored as the postgres data type currently
		//so we need to convert for the non-postgres dialects
		convertedDataType := column.DataType
		switch dialect {
		case util.Sqlserver:
			if field != nil && field.Type == "DateTime" {
				convertedDataType = "nvarchar(500)"
			} else {
				convertedDataType = column.ConvertPostgresDataTypeToSqlserver(column.Name)
			}

		case util.Snowflake:
			convertedDataType = column.ConvertPostgresDataTypeToSnowflake()
		}
		if field != nil {
			if field.GetDataType(dialect) != convertedDataType {
				action = UpdateAction
				typeChanged = true
				hasTypeChanges = true
				details = append(details, fmt.Sprintf("type changed from `%s` to `%s`", column.GetDataType(), field.GetDataType(dialect)))
				originalType = column.GetDataType()
				newType = field.GetDataType(dialect)
			}
		} else {
			// field is missing, need to create one
			field = &dm.Field{
				Table: column.Table,
				Name:  column.Name,
				Type:  column.GetDataType(),
			}
		}

		if action == NoAction {
			continue
		}
		if change.Action != UpdateAction {
			change.Action = UpdateAction
		}

		change.FieldChanges = append(change.FieldChanges, FieldChange{
			Action:       action,
			Field:        field,
			Name:         column.Name,
			Detail:       strings.Join(details, ", "),
			TypeChanged:  typeChanged,
			OriginalType: originalType,
			NewType:      newType,
		})
	}

	if model != nil {
		// now we're trying to find columns that are  new in the model
		// and need to be created
		for _, field := range model.Fields {
			if !foundcolumns[field.Name] {
				if change == nil {
					change = &ModelChange{
						Table:        model.Table,
						Model:        model,
						Action:       UpdateAction,
						FieldChanges: make([]FieldChange, 0),
					}
				}
				if change.Action == NoAction {
					change.Action = UpdateAction
				}
				change.FieldChanges = append(change.FieldChanges, FieldChange{
					Action: AddAction,
					Name:   field.Table,
					Field:  field,
					Detail: fmt.Sprintf("with type `%s`", field.GetDataType(dialect)),
				})

			}
		}
	}

	// This is the case where we don't have the model yet in the db and need to create it
	if len(columns) == 0 {
		needToAdd[model.Table] = model
		change.Action = AddAction
		for _, field := range model.Fields {
			change.FieldChanges = append(change.FieldChanges, FieldChange{
				Action: AddAction,
				Field:  field,
				Name:   field.Table,
				Detail: fmt.Sprintf("with type `%s`", field.GetDataType(dialect)),
			})
		}

	}

	return hasTypeChanges, change, nil
}
