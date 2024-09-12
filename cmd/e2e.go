//go:build e2e
// +build e2e

package cmd

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"

	_ "github.com/shopmonkeyus/eds/internal/drivers/mysql"
	_ "github.com/shopmonkeyus/eds/internal/drivers/postgresql"
)

const (
	sessionId    = "abc123f"
	companyId    = "1234567890"
	enrollToken  = "1234567890"
	modelVersion = "fff000110"
	serverID     = "1"
	dbuser       = "eds"
	dbpass       = "Asdf1234!"
	dbname       = "eds"
	apikey       = "apikey"
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

type ShutdownFunc func()

func setupServer(logger logger.Logger, creds string) (int, ShutdownFunc) {
	p, err := util.GetFreePort()
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/v3/eds/internal/enroll/{code}", func(w http.ResponseWriter, r *http.Request) {
		var resp enrollResponse
		resp.Success = true
		resp.Data = enrollTokenData{
			Token:    enrollToken,
			ServerID: serverID,
		}
		logger.Info("enroll request received: %s", r.PathValue("code"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/eds/internal", func(w http.ResponseWriter, r *http.Request) {
		var resp sessionStartResponse
		resp.Success = true
		usercreds := base64.StdEncoding.EncodeToString([]byte(creds))
		resp.Data = edsSession{
			SessionId:  sessionId,
			Credential: &usercreds,
		}
		logger.Info("session start")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/eds/internal/log", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/v3/eds/internal/{sessionid}", func(w http.ResponseWriter, r *http.Request) {
		var resp sessionEndResponse
		resp.Success = true
		resp.Data.URL = fmt.Sprintf("http://127.0.0.1:%d/v3/eds/internal/log", p)
		resp.Data.ErrorURL = fmt.Sprintf("http://127.0.0.1:%d/v3/eds/internal/log", p)
		logger.Info("session send")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/v3/schema", func(w http.ResponseWriter, r *http.Request) {
		var resp = make(internal.SchemaMap)
		resp["order"] = &internal.Schema{
			Table:        "order",
			ModelVersion: modelVersion,
			Properties: map[string]internal.SchemaProperty{
				"id": {
					Type: "string",
				},
				"name": {
					Type: "string",
				},
			},
			PrimaryKeys: []string{"id"},
		}
		logger.Info("schema fetched")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	s := &http.Server{
		Addr: fmt.Sprintf(":%d", p),
	}

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return p, func() {
		if err := s.Shutdown(context.Background()); err != nil {
			logger.Error("error shutting down http server: %s", err)
		}
	}
}

type runCallbackFunc func(*exec.Cmd)

func run(cmd string, args []string, cb runCallbackFunc) {
	c := exec.Command(getExecutable(), append([]string{cmd}, args...)...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if cb != nil {
		go cb(c)
	}
	if err := c.Run(); err != nil {
		panic(err)
	}
}

type readResult func(event internal.DBChangeEvent) internal.DBChangeEvent

func runTest(logger logger.Logger, _ *nats.Conn, js jetstream.JetStream, readResult readResult) {
	var event internal.DBChangeEvent
	event.ID = util.Hash(time.Now())
	event.Operation = "INSERT"
	event.Table = "order"
	event.Key = []string{"12345"}
	event.ModelVersion = modelVersion
	event.Timestamp = time.Now().UnixMilli()
	event.MVCCTimestamp = fmt.Sprintf("%d", time.Now().Nanosecond())
	event.After = json.RawMessage(`{"id":"12345","name":"test"}`)
	event.Imported = false
	buf, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	subject := "dbchange.order.INSERT." + companyId + ".1.PUBLIC.2"
	logger.Info("publishing event: %s with timestamp: %d", subject, event.Timestamp)
	msgId := util.Hash(event)
	if _, err := js.Publish(context.Background(), subject, buf, jetstream.WithMsgID(msgId)); err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	event2 := readResult(event)
	event2.Imported = false // remove for comparison
	if util.JSONStringify(event) != util.JSONStringify(event2) {
		logger.Fatal(fmt.Sprintf("events do not match. sent: %s, received: %s", util.JSONStringify(event), util.JSONStringify(event2)))
	}
}

type columnFormat func(string) string

func validateSQLEvent(logger logger.Logger, event internal.DBChangeEvent, driver string, url string, format columnFormat, placeholder string) internal.DBChangeEvent {
	logger.Info("testing: %s => %s", driver, url)
	db, err := sql.Open(driver, url)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	query := fmt.Sprintf("select * from %s where id = %s", format(event.Table), placeholder)
	logger.Info("running query: %s", query)
	rows, err := db.Query(query, event.GetPrimaryKey())
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			panic(err)
		}
		kv, err := event.GetObject()
		if err != nil {
			panic(err)
		}
		if id != kv["id"] {
			panic(fmt.Sprintf("id values do not match, was: %s, expected: %s", id, kv["id"]))
		}
		if name != kv["name"] {
			panic(fmt.Sprintf("name values do not match, was: %s, expected: %s", name, kv["name"]))
		}
		logger.Info("event validated: %s", util.JSONStringify(event))
	}
	return event
}

func getEndpointResolver(url string) aws.EndpointResolverWithOptionsFunc {
	return func(_service, region string, _options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           url,
			SigningRegion: "us-east-1",
			SigningMethod: "v4",
		}, nil
	}
}

type e2eTest struct {
	name string
	url  func(dir string) string
	test func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string)
}

var e2eTests = []e2eTest{
	{
		name: "file",
		url: func(dir string) string {
			return util.ToFileURI(dir, "export")
		},
		test: func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, _ string) {
			runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
				fn := filepath.Join(dir, "export", event.Table, fmt.Sprintf("%d-%s.json", time.UnixMilli(event.Timestamp).Unix(), event.Key[0]))
				buf, err := os.ReadFile(fn)
				if err != nil {
					panic(err)
				}
				var event2 internal.DBChangeEvent
				if err := json.Unmarshal(buf, &event2); err != nil {
					panic(err)
				}
				return event2
			})
		},
	},
	{
		name: "mysql",
		url: func(dir string) string {
			return fmt.Sprintf("mysql://%s:%s@127.0.0.1:13306/%s", dbuser, dbpass, dbname)
		},
		test: func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, _ string) {
			runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
				return validateSQLEvent(logger, event, "mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:13306)/%s", dbuser, dbpass, dbname), func(table string) string {
					return fmt.Sprintf("`%s`", table)
				}, "?")
			})
		},
	},
	{
		name: "postgres",
		url: func(dir string) string {
			return fmt.Sprintf("postgres://%s:%s@127.0.0.1:15432/%s?sslmode=disable", dbuser, dbpass, dbname)
		},
		test: func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) {
			runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
				return validateSQLEvent(logger, event, "postgres", url, func(table string) string {
					return fmt.Sprintf(`"%s"`, table)
				}, "$1")
			})
		},
	},
	{
		name: "s3",
		url: func(dir string) string {
			return fmt.Sprintf("s3://127.0.0.1:4566/%s?region=us-east-1&access-key-id=test&secret-access-key=eds", dbname)
		},
		test: func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) {
			runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
				logger.Info("need to finish s3 validation") // FIXME
				provider := config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "eds", ""))
				cfg, err := config.LoadDefaultConfig(context.Background(),
					config.WithRegion("us-east-1"),
					config.WithEndpointResolverWithOptions(getEndpointResolver("http://127.0.0.1:4566")),
					provider,
				)
				if err != nil {
					panic(err)
				}
				s3 := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
					o.UsePathStyle = true
				})
				res, err := s3.GetObject(context.Background(), &awss3.GetObjectInput{
					Bucket: aws.String(dbname),
					Key:    aws.String(fmt.Sprintf("order/%s.json", event.GetPrimaryKey())),
				})
				if err != nil {
					logger.Error("error getting object: %s", err)
				}
				var event2 internal.DBChangeEvent
				if err := json.NewDecoder(res.Body).Decode(&event2); err != nil {
					logger.Fatal("error decoding event: %s", err)
				}
				return event2
			})
		},
	},
	{
		name: "kafka",
		url: func(dir string) string {
			return fmt.Sprintf("kafka://127.0.0.1:29092/%s", dbname)
		},
		test: func(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) {
			runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
				reader := gokafka.NewReader(gokafka.ReaderConfig{
					Brokers:        []string{"127.0.0.1:29092"},
					Topic:          "eds",
					CommitInterval: 0,
					GroupID:        "eds",
				})
				defer reader.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()
				msg, err := reader.FetchMessage(ctx)
				if err != nil {
					logger.Fatal("error fetching message: %s", err)
				}
				if err := reader.CommitMessages(ctx, msg); err != nil {
					logger.Fatal("error committing message: %s", err)
				}
				var event2 internal.DBChangeEvent
				if err := json.Unmarshal(msg.Value, &event2); err != nil {
					logger.Fatal("error decoding event: %s", err)
				}
				return event2
			})
		},
	},
}

var e2eCmd = &cobra.Command{
	Use:   "e2e",
	Short: "Run the end to end test suite",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[e2e]")
		started := time.Now()
		logger.Info("Starting e2e test suite")
		tmpdir, err := os.MkdirTemp("", "e2e")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(tmpdir)
		healthPort, err := util.GetFreePort()
		if err != nil {
			panic(err)
		}
		runNatsTestServer(func(nc *nats.Conn, js jetstream.JetStream, srv *server.Server, userCreds string) {
			logger.Trace("creds: %s", userCreds)
			httpport, shutdown := setupServer(logger, userCreds)
			apiurl := fmt.Sprintf("http://127.0.0.1:%d", httpport)
			run("enroll", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "1234"}, nil)
			for _, test := range e2eTests {
				var wg sync.WaitGroup
				wg.Add(1)
				logger.Info("running test: %s", test.name)
				ts := time.Now()
				url := test.url(tmpdir)
				run("import", []string{"--api-url", apiurl, "-v", "-d", tmpdir, "--no-confirm", "--schema-only", "--api-key", apikey, "--url", url}, nil)
				run("server", []string{"--api-url", apiurl, "--url", url, "-v", "-d", tmpdir, "--server", srv.ClientURL(), "--port", strconv.Itoa(healthPort)}, func(c *exec.Cmd) {
					time.Sleep(time.Second)
					_logger := logger.WithPrefix("[" + test.name + "]")
					_logger.Info("ready to test")
					resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", healthPort))
					if err != nil {
						panic(err)
					}
					_logger.Info("health check: %s", resp.Status)
					if resp.StatusCode != http.StatusOK {
						panic("health check failed")
					}
					test.test(_logger, tmpdir, nc, js, url)
					c.Process.Signal(os.Interrupt)
					wg.Done()
				})
				wg.Wait()
				logger.Info("test: %s completed in %s", test.name, time.Since(ts))
			}
			logger.Info("shutting down server")
			shutdown()
		})
		logger.Info("Completed %d tests in %s", len(e2eTests), time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(e2eCmd)
}
