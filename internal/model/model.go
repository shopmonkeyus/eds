package model

import (
	"fmt"
	"strings"

	"github.com/shopmonkeyus/eds-server/internal/util"
)

type Datamodel struct {
	Models []*Model
}

func (m *Datamodel) String() string {
	return fmt.Sprintf("Datamodel[models=%v]", m.Models)
}

func (m *Datamodel) GetModel(table string) *Model {
	for _, model := range m.Models {
		if model.Table == table || model.Name == table {
			return model
		}
	}
	return nil
}

type EntityChangeConfig struct {
	Create bool            `json:"create,omitempty"`
	Delete bool            `json:"delete,omitempty"`
	Update map[string]bool `json:"update,omitempty"`
}

type Model struct {
	Name         string        `json:"name"`
	Table        string        `json:"table"`
	Fields       []*Field      `json:"fields"`
	ModelVersion string        `json:"modelVersion"`
	Relations    []*Relation   `json:"relations,omitempty"`
	Constraints  []*Constraint `json:"constraints,omitempty"`
	Related      []*Related    `json:"related,omitempty"`
}

// func (m *Model) FindAnnotation(name string) *Annotation {
// 	for _, annotation := range m.Annotations {
// 		if annotation.Name == name {
// 			return annotation
// 		}
// 	}
// 	return nil
// }

// func (m *Model) AddUnique(fields []string) {
// 	m.Uniques = append(m.Uniques, fields)
// }

// func (m *Model) AddIndex(fields []string) {
// 	m.Indexes = append(m.Indexes, fields)
// }

func (m *Model) String() string {
	return fmt.Sprintf("Model[name=%s,table=%s,fields=%s]", m.Name, m.Table, m.Fields)
}

func (m *Model) FindRelation(themodel *Model) *Relation {
	for _, relation := range m.Relations {
		if relation.FieldType == themodel.Name {
			return relation
		}
	}
	return nil
}

func (m *Model) FindField(name string) *Field {
	return m.FindFieldF(func(field *Field) bool {
		return field.Table == name
	})
}

func (m *Model) FindFieldF(f func(*Field) bool) *Field {
	for _, field := range m.Fields {
		if f(field) {
			return field
		}
	}
	return nil
}

func (m *Model) PrimaryKey() []string {
	keys := make([]string, 0)
	for _, field := range m.Fields {
		if field.IsPrimaryKey {
			keys = append(keys, field.Table)
		}
	}
	return keys
}

type Computed struct {
	Expression string `json:"expression"`
	Stored     bool   `json:"stored"`
}

type Field struct {
	Line         int    `json:"line"`
	Name         string `json:"name"`
	Table        string `json:"table"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"primary_key"`
	IsUnique     bool   `json:"unique"`
	IsOptional   bool   `json:"optional"`
	IsTimestampZ bool   `json:"timestampz"`
	IsPrivate    bool   `json:"private"`
	IsList       bool   `json:"list"`
	IsEnum       bool   `json:"enum"`

	// IsScalar     bool          `json:"scalar"`
	// IsUpgrade    bool          `json:"upgrade"`
	// Default      *string       `json:"default,omitempty"`
	// Comment      string        `json:"comment"`
	// Annotations  []*Annotation `json:"annotations,omitempty"`
	// Attributes   []*Annotation `json:"attributes,omitempty"`
	// Computed     *Computed     `json:"computed,omitempty"`
}

func (f *Field) String() string {
	return fmt.Sprintf("Field[name=%s,type=%s]", f.Name, f.Type)
}

// func (f *Field) FindAnnotation(name string) *Annotation {
// 	for _, annotation := range f.Annotations {
// 		if annotation.Name == name {
// 			return annotation
// 		}
// 	}
// 	return nil
// }

func (f *Field) SQLTypePostgres() string {
	var builder strings.Builder
	switch f.Type {
	case "String":
		builder.WriteString("text")
	case "DateTime":
		if f.IsTimestampZ {
			builder.WriteString("TIMESTAMPTZ(6)")
		} else {
			builder.WriteString("TIMESTAMP(3)")
		}
	case "BigInt":
		builder.WriteString("INT8")
	case "Int":
		builder.WriteString("INT8") // NOTE: for Prisma, this is INT4 which is 32bit. we want to default to 64bit
	case "Double":
		builder.WriteString("DOUBLE")
	case "Float":
		builder.WriteString("FLOAT8")
	case "Boolean":
		builder.WriteString("BOOL")
	case "Json":
		builder.WriteString("JSONB")
	case "Bytes":
		builder.WriteString("BYTES")
	case "Decimal":
		builder.WriteString("DECIMAL")
		// default:
		// 	if strings.HasPrefix(f.Type, `"`) {
		// 		builder.WriteString(f.Type)
		// 	} else {
		// 		builder.WriteString(`"` + f.Type + `"`)
		// 	}
	}

	if f.IsEnum {
		builder.WriteString("text")
	}

	if f.IsList {
		builder.WriteString(" ARRAY")
	}
	return builder.String()
}

func (f *Field) PrismaType() string {
	typename := f.Type
	if f.IsOptional && !f.IsList {
		typename += "?"
	}
	if f.IsList {
		typename += "[]"
	}
	return typename
}

type EnumField struct {
	Name  string `json:"name"`
	Table string `json:"table"`
}

type Enum struct {
	Name   string       `json:"name"`
	Fields []*EnumField `json:"fields"`
	Tables []string     `json:"tables"`
}

func (e *Enum) AddTable(table string) {
	if e.Tables == nil {
		e.Tables = []string{table}
	} else {
		util.AddIfNotExists(table, &e.Tables)
	}
}

func (e *Enum) IsUsedByTable(table string) bool {
	for _, tn := range e.Tables {
		if tn == table {
			return true
		}
	}
	return false
}

func (e *Enum) Values() []string {
	values := make([]string, 0)
	for _, field := range e.Fields {
		values = append(values, field.Name)
	}
	return values
}

func (e *Enum) TableValues() []string {
	values := make([]string, 0)
	for _, field := range e.Fields {
		values = append(values, field.Table)
	}
	return values
}

func (e *Enum) Names() []string {
	values := make([]string, 0)
	for _, field := range e.Fields {
		values = append(values, field.Name)
	}
	return values
}

type Annotation struct {
	Name   string            `json:"name"`
	Arg    string            `json:"arg"`
	Tokens map[string]string `json:"-"`
}

func (a *Annotation) Parse() error {
	a.Tokens = make(map[string]string)
	if a.Arg != "" {
		tokens := util.Tokenize(a.Arg)
		for _, token := range tokens {
			tok := strings.Split(token, "=")
			key := tok[0]
			var val string
			if len(tok) > 1 {
				val = tok[1]
			}
			a.Tokens[key] = util.Dequote(val)
		}
	}
	return nil
}

func (a *Annotation) String() string {
	return fmt.Sprintf("Annotation[name=%s,arg=%s]", a.Name, a.Arg)
}

type Related struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	IsList bool   `json:"list"`
}

func (r *Related) String() string {
	return fmt.Sprintf("Related[name=%s,type=%s,list=%v]", r.Name, r.Type, r.IsList)
}

type Relation struct {
	Type       string   `json:"type"`
	Name       string   `json:"name"`
	Fields     []string `json:"fields"`
	References []string `json:"references"`
	OnDelete   *string  `json:"on_delete,omitempty"`
	OnUpdate   *string  `json:"on_update,omitempty"`
	FieldName  string   `json:"field_name"`
	FieldType  string   `json:"field_type"`
	IsList     bool     `json:"list"`
}

func (r *Relation) String() string {
	return fmt.Sprintf("Relation[name=%s,fields=%v,references=%v,field=%s]", r.Name, r.Fields, r.References, r.FieldName)
}

type Constraint struct {
	Table           string  `json:"table"`
	Column          string  `json:"column"`
	ReferenceTable  string  `json:"reference_table"`
	ReferenceColumn string  `json:"reference_column"`
	OnDelete        *string `json:"on_delete,omitempty"`
	OnUpdate        *string `json:"on_update,omitempty"`
}

func (c *Constraint) Name() string {
	return GenerateIndexName(c.Table, []string{c.Column}, "fkey")
}

func FindAnnotation(annotations []*Annotation, name string) (bool, string) {
	for _, ann := range annotations {
		if ann.Name == name {
			return true, ann.Arg
		}
	}
	return false, ""
}

func HasAnnotation(annotations []*Annotation, name string) bool {
	ok, _ := FindAnnotation(annotations, name)
	return ok
}

func GenerateIndexName(table string, columns []string, suffix string) string {
	name := table + "_"
	name += strings.Join(columns, "_")
	if len(name)+len(suffix)+1 > 63 {
		trim := len(name) + len(suffix) + 1 - 63
		name = name[0 : len(name)-trim]
	}
	if len(columns) > 0 {
		name += "_" + suffix
	} else {
		name += suffix
	}
	return name
}
