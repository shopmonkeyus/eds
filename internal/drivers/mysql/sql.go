package mysql

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
)

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|AUTHORIZATION)\b`)

func quoteIdentifier(val string) string {
	if needsQuote.MatchString(val) || keywords.MatchString(val) {
		return "`" + val + "`"
	}
	return val
}

func toSQLFromObject(operation string, model *internal.Schema, table string, event internal.DBChangeEvent, diff []string) (string, error) {
	o, err := event.GetObject()
	if err != nil {
		return "", err
	}
	var sql strings.Builder
	sql.WriteString("REPLACE INTO ")
	sql.WriteString(quoteIdentifier(table))
	var columns []string
	for _, name := range model.Columns() {
		columns = append(columns, quoteIdentifier(name))
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columns, ","))
	sql.WriteString(") VALUES (")
	var insertVals []string
	var updateValues []string
	if operation == "UPDATE" {
		for _, name := range diff {
			if !util.SliceContains(model.Columns(), name) || name == "id" {
				continue
			}
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop)
				updateValues = append(updateValues, v)
			}
		}
		for _, name := range model.Columns() {
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop)
				insertVals = append(insertVals, v)
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop)
				insertVals = append(insertVals, v)
			}
		}
	} else {
		for _, name := range model.Columns() {
			prop := model.Properties[name]
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), prop)
				if name != "id" {
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				}
				insertVals = append(insertVals, v)
			} else {
				v := util.ToJSONStringVal(name, "NULL", prop)
				updateValues = append(updateValues, v)
				insertVals = append(insertVals, v)
			}
		}
	}
	sql.WriteString(strings.Join(insertVals, ","))
	sql.WriteString(");\n")
	return sql.String(), nil
}

func toSQL(c internal.DBChangeEvent, model *internal.Schema) (string, error) {
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		var sql strings.Builder
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[1+i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
		return sql.String(), nil
	} else {
		return toSQLFromObject(c.Operation, model, c.Table, c, c.Diff)
	}
}

func propTypeToSQLType(property internal.SchemaProperty, isPrimaryKey bool) string {
	switch property.Type {
	case "string":
		if isPrimaryKey {
			return "VARCHAR(64)"
		}
		if property.Format == "date-time" {
			return "TIMESTAMP"
		}
		return "TEXT"
	case "integer":
		return "BIGINT"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return "JSON"
	case "array":
		if property.Items != nil && property.Items.Enum != nil {
			return "VARCHAR(64)" // this is an enum but we want to represent it as a string
		}
		return "JSON"
	default:
		return "TEXT"
	}
}

func createSQL(s *internal.Schema) string {
	var sql strings.Builder
	sql.WriteString("DROP TABLE IF EXISTS ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(";\n")
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(" (\n")
	var columns []string
	for _, name := range s.Columns() {
		if util.SliceContains(s.PrimaryKeys, name) {
			continue
		}
		columns = append(columns, name)
	}
	sort.Strings(columns)
	columns = append(s.PrimaryKeys, columns...)
	for _, name := range columns {
		prop := s.Properties[name]
		sql.WriteString("\t")
		sql.WriteString(quoteIdentifier(name))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop, util.SliceContains(s.PrimaryKeys, name)))
		if util.SliceContains(s.Required, name) && !prop.Nullable {
			sql.WriteString(" NOT NULL")
		}
		sql.WriteString(",\n")
	}
	if len(s.PrimaryKeys) > 0 {
		sql.WriteString("\tPRIMARY KEY (")
		for i, pk := range s.PrimaryKeys {
			sql.WriteString(quoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n) CHARACTER SET=utf8mb4;\n")
	return sql.String()
}

func addNewColumnsSQL(columns []string, s *internal.Schema) string {
	var sql strings.Builder
	for _, column := range columns {
		prop := s.Properties[column]
		sql.WriteString("ALTER TABLE ")
		sql.WriteString(quoteIdentifier((s.Table)))
		sql.WriteString(" ADD COLUMN ")
		sql.WriteString(quoteIdentifier(column))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop, false))
		sql.WriteString(";\n")
	}
	return sql.String()
}

func parseURLToDSN(urlstr string) (string, error) {
	//username:password@protocol(address)/dbname?param=value
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", fmt.Errorf("error parsing url: %w", err)
	}
	vals := u.Query()
	vals.Set("multiStatements", "true")
	var dsn strings.Builder
	if u.User != nil {
		dsn.WriteString(util.ToUserPass(u))
		dsn.WriteString("@")
	}
	dsn.WriteString("tcp(")
	dsn.WriteString(u.Host)
	dsn.WriteString(")")
	dsn.WriteString(u.Path)
	dsn.WriteString("?")
	dsn.WriteString(vals.Encode())
	return dsn.String(), nil
}
