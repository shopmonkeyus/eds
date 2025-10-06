package connections

import (
	"database/sql"
	"reflect"

	"github.com/nats-io/nats.go"
)

type connectionsHandler struct {
	connectionsMap map[reflect.Type]connection
}

var handler *connectionsHandler = &connectionsHandler{
	connectionsMap: make(map[reflect.Type]connection),
}

type connection interface {
	GetConnection() connection
	Close()
}

func DisconnectAll() {
	for _, connection := range handler.connectionsMap {
		connection.Close()
	}
}

func Get[T any]() T {
	var t T
	connType := reflect.TypeOf(t)
	c, ok := handler.connectionsMap[connType]
	if !ok {
		panic("no connection registered for type: " + connType.String())
	}
	return c.GetConnection().(T)
}

type natsConnection struct {
	nc *nats.Conn
}

func (c *natsConnection) GetConnection() connection {
	if c.nc == nil {
		url := "nats://localhost:4222"
		var err error
		nc, err := nats.Connect(url)
		if err != nil {
			panic(err)
		}
		c.nc = nc
	}
	return c
}

func (c *natsConnection) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

func GetNATS() *nats.Conn {
	return Get[*natsConnection]().nc
}

// Type alias for *sql.DB as *crdb.DB
type crdbDB = sql.DB

type crdbConnection struct {
	Connection *sql.DB
}

func (c *crdbConnection) GetConnection() connection {
	// Implement CRDB connection logic here
	// For example: return c.conn
	panic("CRDB connection not implemented yet")
}

func (c *crdbConnection) Close() {
	// Implement CRDB close logic here
	// For example: c.conn.Close()
}

func Register(c connection) {
	connType := reflect.TypeOf(c)
	handler.connectionsMap[connType] = c
}

// Initialize default factories
func init() {
	Register(&natsConnection{})
	Register(&crdbConnection{})
}
