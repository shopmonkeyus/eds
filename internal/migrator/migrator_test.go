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
	testCases := []struct {
		dialect       util.Dialect
		expectedQuery string
		expectedError error
	}{
		{
			dialect: util.Sqlserver,
			expectedQuery: `SELECT 
			c.table_name, 
			c.column_name, 
			c.column_default, 
			c.is_nullable, 
			c.data_type, 
			c.character_maximum_length 
			FROM information_schema.columns c 
			WHERE c.table_schema = @p1 AND 
			c.table_name = @p2 
			ORDER BY c.table_name, c.ordinal_position;`,
			expectedError: nil,
		},
		{
			dialect: util.Postgresql,
			expectedQuery: `SELECT 
			c.table_name, 
			c.column_name, 
			c.column_default, 
			c.is_nullable, 
			c.data_type, 
			c.character_maximum_length 
			FROM information_schema.columns c 
			WHERE c.table_schema = $1 AND 
			c.table_name = $2 
			ORDER BY c.table_name, c.ordinal_position;`,
			expectedError: nil,
		},
		{
			dialect: util.Snowflake,
			expectedQuery: `SELECT 
			c.table_name, 
			c.column_name, 
			c.column_default, 
			c.is_nullable, 
			c.data_type, 
			c.character_maximum_length 
			FROM information_schema.columns c 
			WHERE c.table_schema = ? AND 
			c.table_name = ?
			ORDER BY c.table_name, c.ordinal_position;`,
			expectedError: nil,
		},
	}

	for _, testCase := range testCases {
		query := buildTableQuerySchemaString(testCase.dialect)

		if normalized := normalizeWhitespace(query); normalized != normalizeWhitespace(testCase.expectedQuery) {
			t.Errorf("For dialect %v, expected query: %s but got: %s", testCase.dialect, testCase.expectedQuery, query)
		}
	}
}
