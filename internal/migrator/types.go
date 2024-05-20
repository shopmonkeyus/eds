package migrator

import (
	"fmt"
	"sort"
	"strings"

	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

type Dialect string

const (
	Postgresql Dialect = "postgresql"
	Sqlserver  Dialect = "sqlserver"
	Snowflake  Dialect = "snowflake"
)

type SQL interface {
	SQL() string
}

type Action int

const (
	NoAction Action = iota
	DeleteAction
	AddAction
	UpdateAction
)

func (a Action) String() string {
	switch a {
	case NoAction:
		return "No Action"
	case DeleteAction:
		return "Delete"
	case AddAction:
		return "Add"
	case UpdateAction:
		return "Update"
	}
	return "Unknown"
}

type Index struct {
	Table     string
	Name      string
	Type      string
	Columns   []string
	Storing   []string
	Gin       []string
	OpClass   string
	TableType string
}

func (i Index) SQL() string {
	columns := make([]string, 0)
	for _, name := range i.Columns {
		columns = append(columns, fmt.Sprintf(`"%s"`, name))
	}
	if i.IsUnique() {
		return fmt.Sprintf(`CREATE UNIQUE INDEX "%s" ON "%s"(%s)`, i.Name, i.Table, strings.Join(columns, ", "))
	}
	if i.IsPrimaryKey() {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER PRIMARY KEY USING COLUMNS (%s)`, i.Table, strings.Join(columns, ", "))
	}
	return ""
}

func (i Index) IsPrimaryKey() bool {
	return i.Type == "PRIMARY KEY"
}

func (i Index) IsUnique() bool {
	return i.Type == "UNIQUE"
}

func (i Index) IsInverted() bool {
	return i.Type == "INVERTED"
}

type Constraint struct {
	Table            string
	Name             string
	Column           string
	ReferencedTable  string
	ReferencedColumn string
	UpdateRule       string
	DeleteRule       string
}

func (c Constraint) SQL() string {
	return fmt.Sprintf(
		`ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY ("%s") REFERENCES "%s"("%s") ON DELETE %s ON UPDATE %s`,
		c.Table,
		c.Name,
		c.Column,
		c.ReferencedTable,
		c.ReferencedColumn,
		c.DeleteRule,
		c.UpdateRule,
	)
}

type Column struct {
	Table               string
	Name                string
	Default             *string
	IsNullable          bool
	DataType            string
	MaxLength           *string
	UserDefinedTypeName *string
	Dialect             util.Dialect
}

func NewColumnFromField(table string, field *dm.Field, dialect util.Dialect) Column {

	var dataType string
	switch dialect {
	case util.Postgresql:
		dataType = field.SQLTypePostgres()
	case util.Sqlserver:
		dataType = field.SQLTypeSqlServer()
	case util.Snowflake:
		dataType = field.SQLTypeSnowflake()
	default:
		dataType = field.PrismaType()
	}

	return Column{
		Table:      table,
		Name:       field.Name,
		IsNullable: true,
		DataType:   dataType,
		Dialect:    dialect,
	}

}

func (c Column) GetDataType() string {
	return c.DataType
}

func (c Column) ConvertPostgresDataTypeToSqlserver(columnName string) string {
	var convertedDataType strings.Builder
	//TODO: Add correct conversions
	if columnName == "id" {
		convertedDataType.WriteString("varchar(100)")
		return convertedDataType.String()
	}
	switch c.DataType {
	case "TEXT":
		if c.Name == "id" {
			convertedDataType.WriteString("varchar(100)")
		} else {
			convertedDataType.WriteString("varchar(max)")
		}
	case "NUMBER":
		convertedDataType.WriteString("bigint")
	case "BOOLEAN":
		convertedDataType.WriteString("bit")
	case "TIMESTAMP_TZ":
		convertedDataType.WriteString("nvarchar(100)")
	case "varchar":
		convertedDataType.WriteString("varchar(max)")
	case "nvarchar":

		convertedDataType.WriteString("nvarchar(max)")
	default:
		convertedDataType.WriteString(c.DataType)
	}
	return convertedDataType.String()
}

func (c Column) ConvertPostgresDataTypeToSnowflake() string {
	var convertedDataType strings.Builder
	switch c.DataType {

	case "TEXT":
		convertedDataType.WriteString("STRING")
	case "NUMBER":
		convertedDataType.WriteString("INTEGER")
	case "BOOLEAN":
		convertedDataType.WriteString("BOOLEAN")
	case "TIMESTAMP_TZ":
		convertedDataType.WriteString("TIMESTAMPTZ")
	default:
		convertedDataType.WriteString(c.DataType)
	}
	return convertedDataType.String()
}

func (c Column) AlterDefaultSQL(force bool, dialect util.Dialect) string {
	if c.Default == nil || force {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT`, c.Table, c.Name)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s`, c.Table, c.Name, *c.Default)
}

func (c Column) AlterNotNullSQL(dialect util.Dialect) string {
	if c.IsNullable {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL`, c.Table, c.Name)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL`, c.Table, c.Name)
}

func (c Column) AlterTypeSQL(dialect util.Dialect, newType string) string {
	dt := c.DataType

	if dialect == util.Snowflake {
		output := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s_column_change" %s;`, c.Table, c.Name, dt) + "\n"
		snowflakeDatatypeConversion, err := util.DetermineSnowflakeConversion(newType)
		if err != nil {
			panic(err)
		}
		output += fmt.Sprintf(`UPDATE "%s" SET "%s_column_change" = %s("%s");`, c.Table, c.Name, snowflakeDatatypeConversion, c.Name) + "\n"
		output += fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s";`, c.Table, c.Name) + "\n"
		output += fmt.Sprintf(`ALTER TABLE "%s" RENAME COLUMN "%s_column_change" TO "%s";`, c.Table, c.Name, c.Name) + "\n"
		return output
	}
	if dialect == util.Sqlserver {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" %s`, c.Table, c.Name, dt)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s`, c.Table, c.Name, dt)
}

func (c Column) DropSQL(dialect util.Dialect) string {
	return fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s" CASCADE`, c.Table, c.Name)
}

func (c Column) SQL(quote bool, dialect util.Dialect) string {
	name := c.Name
	if quote {
		name = `"` + name + `"`
	}
	return fmt.Sprintf("%s %s", name, c.DataType)
}

func diffValues(oldValues []string, newValues []string) ([]string, []string) {
	add := make([]string, 0)
	remove := make([]string, 0)
	_current := make(map[string]bool)
	_new := make(map[string]bool)
	for _, v := range newValues {
		_new[v] = true
	}
	for _, v := range oldValues {
		_current[v] = true
	}
	for v := range _new {
		if !_current[v] {
			add = append(add, v)
		} else {
			delete(_current, v)
		}
	}
	for v := range _current {
		remove = append(remove, v)
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

type Schema struct {
	Database    string
	Tables      map[string][]Column
	Indexes     map[string][]Index
	Constraints map[string][]Constraint
	TTLs        map[string]string
	Localities  map[string]string
	TableCounts map[string]int64
}

type IndexChange struct {
	Table      string
	Action     Action
	Index      Index
	Constraint *dm.Constraint
}

func (c IndexChange) CreateSQL(dialect util.Dialect) string {
	typeIndex := "INDEX"
	if c.Index.IsUnique() {
		typeIndex = "UNIQUE " + typeIndex
	}
	if c.Index.IsInverted() {
		typeIndex = "INVERTED " + typeIndex
	}
	var storing string
	var gin string
	if len(c.Index.Storing) > 0 {
		storing = fmt.Sprintf(" STORING (%s)", util.QuoteJoin(c.Index.Storing, `"`, ","))
	}
	if len(c.Index.Gin) > 0 {
		var op string
		if c.Index.OpClass != "" {
			op = " " + c.Index.OpClass
		}
		storing = fmt.Sprintf(" GIN (%s%s)", util.QuoteJoin(c.Index.Gin, `"`, ","), op)
	}
	return fmt.Sprintf(`CREATE %s "%s" ON "%s"(%s)%s%s`, typeIndex, c.Index.Name, c.Table, util.QuoteJoin(c.Index.Columns, `"`, ", "), storing, gin)
}

func (c IndexChange) DropSQL(dialect util.Dialect) string {
	return fmt.Sprintf(`DROP INDEX "%s"`, c.Index.Name)
}

type ModelChange struct {
	Table        string
	Action       Action
	Model        *dm.Model
	FieldChanges []FieldChange
	Destructive  bool
}

func (m ModelChange) SQL(dialect util.Dialect) string {
	var sql strings.Builder

	switch m.Action {
	case AddAction:
		if dialect == util.Sqlserver {
			sql.WriteString(fmt.Sprintf(`IF OBJECT_ID(N'%s', N'U') IS NULL`+"\n", m.Table))
			sql.WriteString(fmt.Sprintf(`CREATE TABLE "%s" (`, m.Table) + "\n")
		} else if dialect == util.Postgresql || dialect == util.Snowflake {
			sql.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (`, m.Table) + "\n")
		}

		pks := m.Model.PrimaryKey()
		for i, field := range m.Model.Fields {
			column := NewColumnFromField(m.Table, field, dialect)
			sql.WriteString(spacer + column.SQL(true, dialect))
			if i+1 < len(m.Model.Fields) || len(pks) > 0 {
				sql.WriteString(",\n")
			}
		}
		sql.WriteString("\n")
		if len(pks) > 0 {
			index := dm.GenerateIndexName(m.Model.Table, nil, "pkey")
			// TODO: check dialect
			sql.WriteString(spacer + fmt.Sprintf(`CONSTRAINT "%s" PRIMARY KEY (%s));`, index, util.QuoteJoin(pks, `"`, ",")))
			sql.WriteString("\n")
		}
	case UpdateAction:
		for _, change := range m.FieldChanges {
			column := NewColumnFromField(m.Model.Table, change.Field, dialect)
			switch change.Action {
			case DeleteAction:
				sql.WriteString(column.DropSQL(dialect))
				sql.WriteString(";\n")
			case AddAction:
				if dialect == util.Sqlserver {
					sql.WriteString(fmt.Sprintf(`ALTER TABLE "%s" ADD %s`, m.Model.Table, column.SQL(true, dialect)))
				} else {
					sql.WriteString(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN %s`, m.Model.Table, column.SQL(true, dialect)))
				}
				sql.WriteString(";\n")
			case UpdateAction:
				if change.TypeChanged {
					sql.WriteString(column.AlterTypeSQL(dialect, change.NewType))
					sql.WriteString(";\n")

				}
			}
		}
	}

	return sql.String()
}

type FieldChange struct {
	Action          Action
	Name            string
	Field           *dm.Field
	Detail          string
	DefaultChanged  bool
	TypeChanged     bool
	OptionalChanged bool
	OriginalType    string
	NewType         string
}
