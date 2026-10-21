package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal/util"
)

// ServeConsumerRequests answers consumer requests the way the Shopmonkey backend does, for tests
// and e2e runs where there is no backend.
func ServeConsumerRequests(nc *nats.Conn, js jetstream.JetStream, serverID string) (*nats.Subscription, error) {
	return nc.Subscribe("eds.client.*.consumer", func(msg *nats.Msg) {
		respond := func(resp consumerResponse) {
			msg.Respond([]byte(util.JSONStringify(resp)))
		}
		var req consumerRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			respond(consumerResponse{Message: err.Error()})
			return
		}
		if len(req.CompanyIDs) == 0 {
			respond(consumerResponse{Message: "no permitted companies"})
			return
		}
		name := "eds-" + serverID
		if req.Suffix != "" {
			name += "-" + req.Suffix
		}
		var subjects []string
		for _, companyID := range req.CompanyIDs {
			subjects = append(subjects, "dbchange.*.*."+companyID+".*.PUBLIC.>")
		}
		config := jetstream.ConsumerConfig{
			Durable:           name,
			MaxAckPending:     req.MaxAckPending,
			MaxDeliver:        20,
			AckWait:           time.Minute * 5,
			MaxRequestBatch:   req.MaxRequestBatch,
			FilterSubjects:    subjects,
			AckPolicy:         jetstream.AckExplicitPolicy,
			InactiveThreshold: time.Hour * 24 * 3,
			MaxWaiting:        1,
		}
		ctx := context.Background()
		var err error
		if existing, lookupErr := js.Consumer(ctx, "dbchange", name); lookupErr == nil {
			// the delivery policy can only be set when the consumer is created
			config.DeliverPolicy = existing.CachedInfo().Config.DeliverPolicy
			config.OptStartTime = existing.CachedInfo().Config.OptStartTime
			config.MaxWaiting = existing.CachedInfo().Config.MaxWaiting
			_, err = js.UpdateConsumer(ctx, "dbchange", config)
		} else {
			if req.DeliverAll {
				config.DeliverPolicy = jetstream.DeliverAllPolicy
			} else if req.StartTime != nil {
				config.DeliverPolicy = jetstream.DeliverByStartTimePolicy
				config.OptStartTime = req.StartTime
			}
			_, err = js.CreateConsumer(ctx, "dbchange", config)
		}
		if err != nil {
			respond(consumerResponse{Message: err.Error()})
			return
		}
		respond(consumerResponse{Success: true, Name: name})
	})
}
