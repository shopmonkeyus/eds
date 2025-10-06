package integrationtest

import (
<<<<<<< HEAD
=======
	"database/sql"

	"github.com/lib/pq"
>>>>>>> e034a66 (integration testing staging branch)
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var GlobalConnectionHandler connectionHandler

type connectable interface {
	Connect()
	Close()
}

// this is needed because connections must return particular types and connectable can't be generic
type typedConnection[T any] interface {
	connectable
	Get() T
}

type connectionHandler struct {
	connections []connectable
}

func (c *connectionHandler) addConnection(connection connectable) {
	c.connections = append(c.connections, connection)
}

func (c *connectionHandler) DisconnectAll() {
	for _, connection := range c.connections {
		connection.Close()
	}
}

func NewConnection[T any](connection typedConnection[T]) T {
	GlobalConnectionHandler.addConnection(connection)
	connection.Connect()
	return connection.Get()
}

type NatsConnection struct {
	nc *nats.Conn
}

func (c *NatsConnection) Connect() {
	if c.nc == nil {
		url := "nats://localhost:4222"
		var err error
		c.nc, err = nats.Connect(url)
		if err != nil {
			panic(err)
		}
	}
}

func (c *NatsConnection) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

func (c *NatsConnection) Get() *nats.Conn {
	return c.nc
}

type JetstreamConnection struct {
	NatsConnection
	js jetstream.JetStream
}

func (c *JetstreamConnection) Connect() {
	c.NatsConnection.Connect()
	var err error
	if c.js, err = jetstream.New(c.NatsConnection.nc); err != nil {
		panic(err)
	}
}

func (c *JetstreamConnection) Close() {
	c.NatsConnection.Close()
}

func (c *JetstreamConnection) Get() jetstream.JetStream {
	return c.js
}
<<<<<<< HEAD
=======

type CRDBConnection struct {
	db *sql.DB
}

func (c *CRDBConnection) Connect() {
	base, err := pq.NewConnector("postgres://root@localhost:26257/defaultdb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	c.db = sql.OpenDB(base)
}

func (c *CRDBConnection) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

func (c *CRDBConnection) Get() *sql.DB {
	return c.db
}
>>>>>>> e034a66 (integration testing staging branch)
