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
	var change *ModelChange
	var hasTypeChanges bool

	foundcolumns := make(map[string]bool)
	for _, column := range columns {
		foundcolumns[column.Name] = true
		var action Action
		var field *dm.Field
		details := make([]string, 0)
		var typeChanged bool

		field = findField(model, column.Name)
		if field != nil {
			if field.GetDataType(dialect) != column.GetDataType() {
				action = UpdateAction
				typeChanged = true
				hasTypeChanges = true
				details = append(details, fmt.Sprintf("type changed from `%s` to `%s`", column.GetDataType(), field.GetDataType(dialect)))
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
		if change == nil {
			change = &ModelChange{
				Table:        model.Table,
				Model:        model,
				Action:       UpdateAction,
				FieldChanges: make([]FieldChange, 0),
			}
		}

		change.FieldChanges = append(change.FieldChanges, FieldChange{
			Action:      action,
			Field:       field,
			Name:        column.Name,
			Detail:      strings.Join(details, ", "),
			TypeChanged: typeChanged,
		})

		if model != nil {
			for _, field := range model.Fields {
				if !foundcolumns[field.Table] {
					if change == nil {
						change = &ModelChange{
							Table:        model.Table,
							Model:        model,
							Action:       UpdateAction,
							FieldChanges: make([]FieldChange, 0),
						}
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
	}
	if len(columns) == 0 {
		needToAdd[model.Table] = model
		change = &ModelChange{
			Table:        model.Table,
			Action:       AddAction,
			Model:        model,
			FieldChanges: make([]FieldChange, 0),
		}
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
