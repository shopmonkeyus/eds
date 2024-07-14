package mysql

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|AUTHORIZATION)\b`)

func quoteIdentifier(val string) string {
	if needsQuote.MatchString(val) || keywords.MatchString(val) {
		return "`" + val + "`"
	}
	return val
}

func toSQLFromObject(operation string, model *internal.Schema, table string, o map[string]any, diff []string) string {
	var sql strings.Builder
	sql.WriteString("REPLACE INTO ")
	sql.WriteString(quoteIdentifier(table))
	var columns []string
	for _, name := range model.Columns {
		columns = append(columns, quoteIdentifier(name))
	}
	sql.WriteString(" (")
	sql.WriteString(strings.Join(columns, ","))
	sql.WriteString(") VALUES (")
	var insertVals []string
	var updateValues []string
	if operation == "UPDATE" {
		for _, name := range diff {
			if !util.SliceContains(model.Columns, name) {
				continue
			}
			if val, ok := o[name]; ok {
				v := quoteValue(val)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			} else {
				updateValues = append(updateValues, "NULL")
			}
		}
		for _, name := range model.Columns {
			if val, ok := o[name]; ok {
				v := quoteValue(val)
				insertVals = append(insertVals, v)
			} else {
				insertVals = append(insertVals, "NULL")
			}
		}
	} else {
		for _, name := range model.Columns {
			if val, ok := o[name]; ok {
				v := quoteValue(val)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				insertVals = append(insertVals, v)
			} else {
				updateValues = append(updateValues, "NULL")
				insertVals = append(insertVals, "NULL")
			}
		}
	}
	sql.WriteString(strings.Join(insertVals, ","))
	sql.WriteString(");\n")
	return sql.String()
}

func toSQL(c internal.DBChangeEvent, schema internal.SchemaMap) (string, error) {
	model := schema[c.Table]
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
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		return toSQLFromObject(c.Operation, model, c.Table, o, c.Diff), nil
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
		return "INTEGER"
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
	for _, name := range s.Columns {
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
		dsn.WriteString(u.User.String())
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
