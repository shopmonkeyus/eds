package types

type Field struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey string `json:"primary_key"`
	Enum       bool   `json:"enum"`
	Optional   bool   `json:"optional"`
	Scalar     bool   `json:"scalar"`
	List       bool   `json:"list"`
}

type Constraint struct {
	Table           string `json:"table"`
	Column          string `json:"column"`
	ReferenceTable  string `json:"reference_table"`
	ReferenceColumn string `json:"reference_column"`
}

type Relation struct {
	Name       string   `json:"name"`
	Fields     []string `json:"fields,omitempty"`
	References []string `json:"references,omitempty"`
	FieldName  string   `json:"field_name"` // Schema relation name. In case of email relation it can be email, emails
	FieldType  string   `json:"field_type"` // Schema relation type. In case of email relation it will be Email
	List       bool     `json:"list"`
}

type Related struct {
	Name string `json:"name"`
	Type string `json:"type"`
	List bool   `json:"list"`
}

type Table struct {
	Name         string       `json:"name"`
	Table        string       `json:"table"`
	Fields       []string     `json:"fields"`
	Related      []Related    `json:"related,omitempty"`
	Relations    []Relation   `json:"relations,omitempty"`
	Constraints  []Constraint `json:"constraints,omitempty"`
	ModelVersion string       `json:"modelVersion"`
}
