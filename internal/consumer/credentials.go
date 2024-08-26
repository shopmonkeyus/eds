package consumer

import (
	"errors"
	"fmt"
	"os"
	"regexp"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds/internal/util"
)

var companyIDRE = regexp.MustCompile(`^dbchange\.\*\.\*\.([a-f0-9-]+)\.`)
var sessionIDRE = regexp.MustCompile(`^eds.notify.([a-f0-9-]+)\.`)

func extractCompanyIdFromDBChangeSubscription(sub string) string {
	return getFirstMatch(companyIDRE, sub)
}

func extractSessionIdFromEdsSubscription(sub string) string {
	return getFirstMatch(sessionIDRE, sub)
}

func getFirstMatch(re *regexp.Regexp, s string) string {
	if re.MatchString(s) {
		return re.FindStringSubmatch(s)[1]
	}
	return ""
}

func getNatsCreds(creds string) (nats.Option, *CredentialInfo, error) {
	if !util.Exists(creds) {
		return nil, nil, fmt.Errorf("credential file: %s cannot be found", creds)
	}
	buf, err := os.ReadFile(creds)
	if err != nil {
		return nil, nil, fmt.Errorf("reading credentials file: %w", err)
	}
	natsCredentials := nats.UserCredentials(creds)

	natsJWT, err := jwt.ParseDecoratedJWT(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing valid JWT: %s", err)

	}
	claim, err := jwt.DecodeUserClaims(natsJWT)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding JWT claims: %s", err)
	}
	var companyIDs []string
	var sessionID string
	allowedSubs := claim.Sub.Allow
	for _, sub := range allowedSubs {
		if maybeSessionID := extractSessionIdFromEdsSubscription(sub); maybeSessionID != "" {
			sessionID = maybeSessionID
			continue
		}
		if companyID := extractCompanyIdFromDBChangeSubscription(sub); companyID != "" {
			companyIDs = append(companyIDs, companyID)
		}
	}
	if len(companyIDs) == 0 {
		return nil, nil, errors.New("issue parsing company IDs from JWT claims. Ensure the JWT has the correct permissions")
	}
	serverID := claim.Name
	if serverID == "" {
		return nil, nil, errors.New("missing server id in credential")
	}
	return natsCredentials, &CredentialInfo{
		CompanyIDs: companyIDs,
		ServerID:   serverID,
		SessionID:  sessionID,
	}, nil
}
