package util

import (
	"testing"
)

func TestMaskConnectionString(t *testing.T) {
	tests := []struct {
		name                     string
		originalConnectionString string
		expectedMaskedString     string
	}{
		{
			name:                     "Valid Snowflake Connection String",
			originalConnectionString: "snowflake://jsmith:pasdhowdh-ghx@tflxsoy-lt41015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true",
			expectedMaskedString:     "snowflake://*****:*****@tflxsoy-lt41015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true",
		},
		{
			name:                     "Another Valid Connection String",
			originalConnectionString: "snowflake://user:password@server:port/database?param=value",
			expectedMaskedString:     "snowflake://*****:*****@server:port/database?param=value",
		},
		{
			name:                     "Valid Postgres Connection String",
			originalConnectionString: "postgresql://postgres:$PGPASS@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "postgresql://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Another Valid Postgres Connection String",
			originalConnectionString: "postgresql://postgresUserName:ReallyLongPasswordHereForTesting123!!!##@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "postgresql://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Valid Sql Server Connection String",
			originalConnectionString: "sqlserver://sa:$PGPASS@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "sqlserver://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Another Valid Sql Server Connection String",
			originalConnectionString: "sqlserver://sqlUserName:p@ssw..rd.a.!!!##@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "sqlserver://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Invalid Connection String",
			originalConnectionString: "invalid-connection-string",
			expectedMaskedString:     "invalid-connection-string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maskedString := MaskConnectionString(tt.originalConnectionString)

			if maskedString != tt.expectedMaskedString {
				t.Errorf("Got %s, expected %s", maskedString, tt.expectedMaskedString)
			}
		})
	}
}
