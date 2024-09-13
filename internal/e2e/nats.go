//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/shopmonkeyus/eds/internal/util"
)

func createAccount(name string, oskp nkeys.KeyPair, jetstream bool) (nkeys.KeyPair, string, string) {
	// create an account keypair
	akp, err := nkeys.CreateAccount()
	if err != nil {
		panic(err)
	}
	// extract the public key for the account
	apk, err := akp.PublicKey()
	if err != nil {
		panic(err)
	}
	ac := jwt.NewAccountClaims(apk)
	ac.Name = name
	if jetstream {
		ac.Limits.JetStreamLimits.MemoryStorage = jwt.NoLimit
		ac.Limits.JetStreamLimits.DiskMaxStreamBytes = jwt.NoLimit
		ac.Limits.JetStreamLimits.DiskStorage = jwt.NoLimit
		ac.Limits.JetStreamLimits.Consumer = jwt.NoLimit
		ac.Limits.JetStreamLimits.MaxAckPending = jwt.NoLimit
		ac.Limits.JetStreamLimits.Streams = jwt.NoLimit
	}

	// create a signing key that we can use for issuing users
	askp, err := nkeys.CreateAccount()
	if err != nil {
		panic(err)
	}
	// extract the public key
	aspk, err := askp.PublicKey()
	if err != nil {
		panic(err)
	}
	// add the signing key (public) to the account
	ac.SigningKeys.Add(aspk)

	// now we could encode an issue the account using the operator
	// key that we generated above, but this will illustrate that
	// the account could be self-signed, and given to the operator
	// who can then re-sign it
	accountJWT, err := ac.Encode(akp)
	if err != nil {
		panic(err)
	}

	// the operator would decode the provided token, if the token
	// is not self-signed or signed by an operator or tampered with
	// the decoding would fail
	ac, err = jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		panic(err)
	}

	// here the operator is going to use its private signing key to
	// re-issue the account
	accountJWT, err = ac.Encode(oskp)
	if err != nil {
		panic(err)
	}

	return askp, apk, accountJWT
}

func createNatsTestServer(dir string, port int) (*server.Server, *nats.Conn, string) {
	// create an operator key pair (private key)
	okp, err := nkeys.CreateOperator()
	if err != nil {
		panic(err)
	}
	// extract the public key
	opk, err := okp.PublicKey()
	if err != nil {
		panic(err)
	}

	// create an operator claim using the public key for the identifier
	oc := jwt.NewOperatorClaims(opk)
	oc.Name = "O"
	// add an operator signing key to sign accounts
	oskp, err := nkeys.CreateOperator()
	if err != nil {
		panic(err)
	}
	// get the public key for the signing key
	ospk, err := oskp.PublicKey()
	if err != nil {
		panic(err)
	}
	// add the signing key to the operator - this makes any account
	// issued by the signing key to be valid for the operator
	oc.SigningKeys.Add(ospk)

	// self-sign the operator JWT - the operator trusts itself
	operatorJWT, err := oc.Encode(okp)
	if err != nil {
		panic(err)
	}

	askp, apk, accountJWT := createAccount("A", oskp, true)
	_, sysapk, sysaccountJWT := createAccount("SYS", oskp, false)

	// now back to the account, the account can issue users
	// need not be known to the operator - the users are trusted
	// because they will be signed by the account. The server will
	// look up the account get a list of keys the account has and
	// verify that the user was issued by one of those keys
	ukp, err := nkeys.CreateUser()
	if err != nil {
		panic(err)
	}
	upk, err := ukp.PublicKey()
	if err != nil {
		panic(err)
	}
	uc := jwt.NewUserClaims(upk)
	uc.Name = serverID
	uc.Pub.Allow = []string{
		"$JS.API.STREAM.NAMES",
		"$JS.ACK.dbchange.>",
		"$JS.API.CONSUMER.MSG.NEXT.dbchange.>",
		"$JS.API.STREAM.CREATE.dbchange",
		"$JS.API.CONSUMER.>",
		"$JS.API.CONSUMER.*.dbchange.*",
		fmt.Sprintf("eds.client.%s.>", sessionId),
		"_INBOX.>",
		"dbchange.>",
	}
	uc.Sub.Allow = []string{
		fmt.Sprintf("dbchange.*.*.%s.*.PUBLIC.>", companyId),
		fmt.Sprintf("eds.notify.%s.>", sessionId),
		"_INBOX.>",
	}

	// since the jwt will be issued by a signing key, the issuer account
	// must be set to the public ID of the account
	uc.IssuerAccount = apk
	userJwt, err := uc.Encode(askp)
	if err != nil {
		panic(err)
	}
	// the seed is a version of the keypair that is stored as text
	useed, err := ukp.Seed()
	if err != nil {
		panic(err)
	}
	// generate a creds formatted file that can be used by a NATS client
	creds, err := jwt.FormatUserConfig(userJwt, useed)
	if err != nil {
		panic(err)
	}

	// fmt.Println(string(creds))

	if err := os.MkdirAll(filepath.Join(dir, "store"), 0755); err != nil {
		panic(err)
	}

	// we are generating a memory resolver server configuration
	// it lists the operator and all account jwts the server should
	// know about
	resolver := fmt.Sprintf(`operator: %s
listen: 127.0.0.1:%d
server_name: e2e
jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
resolver: MEMORY
resolver_preload: {
	%s: %s
	%s: %s
}
system_account: %s
`, operatorJWT, port, filepath.Join(dir, "store"), apk, accountJWT, sysapk, sysaccountJWT, sysapk)
	if err := os.WriteFile(filepath.Join(dir, "resolver.conf"),
		[]byte(resolver), 0600); err != nil {
		panic(err)
	}

	// fmt.Println(strings.Repeat("=", 100))
	// fmt.Println(resolver)
	// fmt.Println(strings.Repeat("=", 100))

	// store the creds
	credsPath := filepath.Join(dir, "user.creds")
	if err := os.WriteFile(credsPath, creds, 0600); err != nil {
		panic(err)
	}

	opts, err := server.ProcessConfigFile(filepath.Join(dir, "resolver.conf"))
	if err != nil {
		panic(err)
	}

	// fmt.Println(util.JSONStringify(opts))

	srv, err := server.NewServer(opts)
	if err != nil {
		panic(err)
	}
	srv.ConfigureLogger()
	srv.Start()

	nc, err := nats.Connect(srv.ClientURL(), nats.UserCredentials(credsPath))
	if err != nil {
		panic(err)
	}

	return srv, nc, string(creds)
}

func runNatsTestServer(fn func(nc *nats.Conn, js jetstream.JetStream, srv *server.Server, userCreds string)) {
	port, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	srv, nc, userCreds := createNatsTestServer(dir, port)

	defer nc.Close()
	defer srv.Shutdown()

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:         "dbchange",
		Subjects:     []string{"dbchange.>"},
		MaxConsumers: 1,
		Storage:      jetstream.MemoryStorage,
	}); err != nil {
		panic(err)
	}
	fn(nc, js, srv, userCreds)
}
