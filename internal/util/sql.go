package util

import (
	"context"
	"database/sql"
	"net/url"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

// QuoteIdentifier quotes an identifier with double quotes
func QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

// QuoteStringIdentifiers quotes a slice of identifiers with double quotes
func QuoteStringIdentifiers(vals []string) []string {
	res := make([]string, len(vals))
	for i, val := range vals {
		res[i] = QuoteIdentifier(val)
	}
	return res
}

// SQLExecuter returns a wrapper around a SQL database connection that can execute SQL statements or log them in dry-run mode
func SQLExecuter(ctx context.Context, log logger.Logger, db *sql.DB, dryRun bool) func(sql string) error {
	return func(sql string) error {
		if dryRun {
			log.Info("[dry-run] %s", sql)
			return nil
		}
		log.Debug("executing: %s", strings.TrimRight(sql, "\n"))
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
		return nil
	}
}

func isEmptyVal(val string) bool {
	return val == "''" || val == "" || val == "NULL"
}

// ToJSONStringVal returns a JSON string value checking for empty string and converting it to '{}'
func ToJSONStringVal(name string, val string, prop internal.SchemaProperty) string {
	if prop.IsArrayOrJSON() && prop.IsNotNull() && isEmptyVal(val) {
		switch prop.Type {
		case "array":
			return "'[]'"
		case "object":
			return "'{}'"
		}
	}
	return val
}

// ToUserPass returns a user:pass string from a URL
func ToUserPass(u *url.URL) string {
	var dsn strings.Builder
	user := u.User.Username()
	pass, ok := u.User.Password()
	dsn.WriteString(user)
	if ok {
		dsn.WriteString(":")
		dsn.WriteString(pass)
	}
	return dsn.String()
}
