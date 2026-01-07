package snowflake

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"

	sf "github.com/snowflakedb/gosnowflake"
)

type snowflakeKeyPairDriver struct {
	snowflakeDriver
}

var secretKey = "secret-key"

var _ internal.Driver = (*snowflakeKeyPairDriver)(nil)
var _ internal.DriverLifecycle = (*snowflakeKeyPairDriver)(nil)
var _ internal.Importer = (*snowflakeKeyPairDriver)(nil)
var _ internal.DriverSessionHandler = (*snowflakeKeyPairDriver)(nil)
var _ internal.DriverHelp = (*snowflakeKeyPairDriver)(nil)
var _ internal.DriverMigration = (*snowflakeKeyPairDriver)(nil)

func (p *snowflakeKeyPairDriver) Name() string {
	return "Snowflake Key Pair"
}

func (p *snowflakeKeyPairDriver) Description() string {
	return "Temporary driver for migrating to key-pair authentication for Snowflake"
}

func (p *snowflakeKeyPairDriver) ExampleURL() string {
	return "snowflake-keypair://user@account/database/schema?secret-key=SECRET_ENV_VAR_NAME"
}

// Defines the fields shown in the frontend
func (p *snowflakeKeyPairDriver) Configuration() []internal.DriverField {
	defaultDatabase := "DBNAME/SCHEMA"
	defaultAccount := "abcdefg-ab12345"
	defaultSecret := "SNOWFLAKE_SECRET_ACCESS_KEY"
	return []internal.DriverField{
		internal.RequiredStringField("Database", "The database name including the schema, e.g. DBNAME/SCHEMA", &defaultDatabase),
		internal.RequiredStringField("Username", "The username to use. Note the user must be associated with the public key", nil),
		internal.RequiredStringField("Account", "The full Snowflake account identifier including the organization, e.g. abcdefg-ab12345", &defaultAccount),
		internal.OptionalStringField("Secret", "Name of environment variable on the EDS server that contains the unencrypted PKCS#8 private key", &defaultSecret),
	}
}

func OpenSnowflakeWithKeyPair(secret string, username string, account string, database string, schema string) (*sql.DB, error) {
	block, _ := pem.Decode([]byte(secret))
	if block == nil {
		return nil, fmt.Errorf("failed to decode secret")
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}
	privateKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not RSA")
	}

	cfg := &sf.Config{
		User:          username,
		Account:       account,
		Database:      database,
		Schema:        schema,
		PrivateKey:    privateKey,
		Authenticator: sf.AuthTypeJwt,
	}

	connector := sf.NewConnector(sf.SnowflakeDriver{}, *cfg)

	return sql.OpenDB(connector), nil
}

func (p *snowflakeKeyPairDriver) connectToDBWithKeyPair(ctx context.Context, urlString string) (*sql.DB, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	username := parsedURL.User.Username()
	account := parsedURL.Host

	pathParts := strings.Split(strings.TrimPrefix(parsedURL.Path, "/"), "/")
	if len(pathParts) < 2 {
		return nil, fmt.Errorf("invalid URL path: expected /database/schema, got %s", parsedURL.Path)
	}
	database := pathParts[0]
	schema := pathParts[1]

	secret := os.Getenv(parsedURL.Query().Get(secretKey))
	if secret == "" {
		secret = os.Getenv("SNOWFLAKE_SECRET_ACCESS_KEY")
	}

	db, err := OpenSnowflakeWithKeyPair(secret, username, account, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to open snowflake with keypair: %w", err)
	}

	row := db.QueryRowContext(ctx, "SELECT 1")
	if err := row.Err(); err != nil {
		db.Close()
		return nil, err
	}

	if err := p.snowflakeDriver.RefreshSchema(ctx, db, false); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (p *snowflakeKeyPairDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	account := internal.GetRequiredStringValue("Account", values)
	database := internal.GetRequiredStringValue("Database", values)
	username := internal.GetRequiredStringValue("Username", values)
	secret := internal.GetOptionalStringValue("Secret", "", values)

	var u url.URL
	u.Scheme = "snowflake-keypair"
	u.User = url.User(username)
	u.Host = account
	u.Path = database
	q := u.Query()
	q.Set(secretKey, secret)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (p *snowflakeKeyPairDriver) Start(config internal.DriverConfig) error {
	p.connectToDBFunc = p.connectToDBWithKeyPair
	return p.snowflakeDriver.Start(config)
}

func (p *snowflakeKeyPairDriver) Import(config internal.ImporterConfig) error {
	p.connectToDBFunc = p.connectToDBWithKeyPair
	return p.snowflakeDriver.Import(config)
}

// Test is called to test the driver's connectivity with the configured url.
func (p *snowflakeKeyPairDriver) Test(ctx context.Context, logger logger.Logger, urlString string) error {
	p.logger = logger.WithPrefix("[snowflake-keypair]") // Have to have this or RefreshSchema panics
	db, err := p.connectToDBWithKeyPair(ctx, urlString)
	if err != nil {
		return err
	}
	return db.Close()
}

func init() {
	internal.RegisterDriver("snowflake-keypair", &snowflakeKeyPairDriver{})
	internal.RegisterImporter("snowflake-keypair", &snowflakeKeyPairDriver{})
}
