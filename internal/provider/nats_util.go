package provider

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

func GetLatestSchema(logger logger.Logger, nc *nats.Conn, table string) (dm.Model, error) {
	var schema dm.Model
	var emptyJSON = []byte("{}")
	const modelRequestTimeout = time.Duration(time.Second * 30)
	entry, err := nc.Request(fmt.Sprintf("schema.%s.latest", table), emptyJSON, modelRequestTimeout)
	if err != nil {
		return schema, err
	}
	var foundSchema datatypes.SchemaResponse

	err = json.Unmarshal(entry.Data, &foundSchema)
	if err != nil {
		return schema, fmt.Errorf("error unmarshalling change event schema: %s. %s", string(entry.Data), err)
	}
	schema = foundSchema.Data
	if foundSchema.Success {
		logger.Trace("got latest schema model for: %v for table: %s", foundSchema.Data, table)
		return schema, nil
	} else {
		return schema, fmt.Errorf("no schema model found when searching for latest schema: %v for table: %s", foundSchema.Data, table)
	}
}
