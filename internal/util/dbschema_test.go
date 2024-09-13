package util

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestBuildDBSchemaFromInfoSchema(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_catalog = '1").WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "data_type"}).AddRow("table1", "column1", "int"))

	res, err := BuildDBSchemaFromInfoSchema(context.Background(), db, "table_catalog", "1")
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotEmpty(t, res)
	assert.Equal(t, "int", res["table1"]["column1"])
	assert.Equal(t, []string{"column1"}, res.Columns("table1"))
	ok, val := res.GetType("table1", "column1")
	assert.True(t, ok)
	assert.Equal(t, "int", val)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetCurrentDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT DATABASE\\(\\)").WithArgs().WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("test"))

	res, err := GetCurrentDatabase(context.Background(), db, "DATABASE()")
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "test", res)
}
