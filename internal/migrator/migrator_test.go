package migrator

import (
	"strings"
	"testing"

	"github.com/shopmonkeyus/eds-server/internal/util"
)

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestBuildTableQuerySchemaString(t *testing.T) {
	query :=
		`SELECT 
			c.table_name, 
			c.column_name, 
			c.column_default, 
			c.is_nullable, 
			c.data_type, 
			c.character_maximum_length 
			FROM information_schema.columns c 
			WHERE c.table_schema = $1 AND 
			c.table_name = $2 
			ORDER BY c.table_name, c.ordinal_position;`
	testCases := []struct {
		dialect                  util.Dialect
		table_schema_placeholder string
		table_name_placeholder   string
		expectedQuery            string
		expectedError            error
	}{
		{
			dialect:                  util.Sqlserver,
			table_schema_placeholder: "@p1",
			table_name_placeholder:   "@p2",
			expectedError:            nil,
		},
		{
			dialect:                  util.Postgresql,
			table_schema_placeholder: "$1",
			table_name_placeholder:   "$2",
			expectedError:            nil,
		},
		{
			dialect:                  util.Snowflake,
			table_schema_placeholder: "?",
			table_name_placeholder:   "?",
			expectedError:            nil,
		},
	}

	for _, testCase := range testCases {

		expectedQuery := strings.ReplaceAll(query, "$1", testCase.table_schema_placeholder)
		expectedQuery = strings.ReplaceAll(expectedQuery, "$2", testCase.table_name_placeholder)
		expectedNorm := normalizeWhitespace(expectedQuery)

		query := buildTableQuerySchemaString(testCase.dialect)

		if normalized := normalizeWhitespace(query); normalized != expectedNorm {
			t.Errorf("For dialect %v, expected query: %s but got: %s", testCase.dialect, expectedQuery, query)
		}
	}
}
