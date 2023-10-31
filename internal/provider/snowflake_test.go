package provider

import (
	"testing"
)

func TestGetSnowflakeSchema(t *testing.T) {
	tests := []struct {
		name           string
		connectionStr  string
		expectedSchema string
		expectError    bool
	}{
		{
			name:           "Valid Schema",
			connectionStr:  "snowflake://jnguyen:pasdhowdh-ghx@tflxsoy-lt41015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true",
			expectedSchema: "PUBLIC",
			expectError:    false,
		},
		{
			name:           "Other Valid Schema",
			connectionStr:  "snowflake://jngsaan:pasdsddh-ghx@tflxcjy-lsa5015/mydb/test?warehouse=JON_WH",
			expectedSchema: "test",
			expectError:    false,
		},
		{
			name:           "No Schema",
			connectionStr:  "snowflake://jnguyen:pasdhowdh-ghx@tflxcjy-gu49015/mydb/?warehouse=COMPUTE_WH&client_session_keep_alive=true",
			expectedSchema: "",
			expectError:    true,
		},
		{
			name:           "Bad Connection string",
			connectionStr:  "jnguyen:pasdhowgh-ex@tflxcjy-lu41115/mydb/?warehouse=COMPUTE_WH&client_session_keep_alive=true",
			expectedSchema: "",
			expectError:    true,
		},
		{
			name:           "Invalid Connection String",
			connectionStr:  "invalid-connection-string",
			expectedSchema: "",
			expectError:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := getSnowflakeSchema(test.connectionStr)

			if test.expectError {
				if err == nil {
					t.Errorf("Expected an error but got nil")
				} else if schema != test.expectedSchema {
					t.Errorf("Expected schema %s, but got %s", test.expectedSchema, schema)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got an error: %v", err)
				} else if schema != test.expectedSchema {
					t.Errorf("Expected schema %s, but got %s", test.expectedSchema, schema)
				}
			}
		})
	}
}
