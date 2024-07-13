package consumer

import (
	"errors"
	"fmt"
	"os"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

func getNatsCreds(creds string) (nats.Option, []string, string, error) {
	if !util.Exists(creds) {
		return nil, nil, "", fmt.Errorf("credential file: %s cannot be found", creds)
	}
	buf, err := os.ReadFile(creds)
	if err != nil {
		return nil, nil, "", fmt.Errorf("reading credentials file: %w", err)
	}
	natsCredentials := nats.UserCredentials(creds)

	natsJWT, err := jwt.ParseDecoratedJWT(buf)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parsing valid JWT: %s", err)

	}
	claim, err := jwt.DecodeUserClaims(natsJWT)
	if err != nil {
		return nil, nil, "", fmt.Errorf("decoding JWT claims: %s", err)

	}
	var companyIDs []string
	allowedSubs := claim.Sub.Allow
	for _, sub := range allowedSubs {
		companyID := util.ExtractCompanyIdFromSubscription(sub)
		if companyID != "" {
			companyIDs = append(companyIDs, companyID)
		}
	}
	if len(companyIDs) == 0 {
		return nil, nil, "", errors.New("issue parsing company ID from JWT claims. Ensure the JWT has the correct permissions")
	}
	companyName := claim.Name
	if companyName == "" {
		return nil, nil, "", errors.New("missing company name in credential")
	}
	return natsCredentials, companyIDs, companyName, nil
}
