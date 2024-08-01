package util

import (
	"fmt"

	jwt "github.com/golang-jwt/jwt/v5"
)

// GetAPIURLFromJWT extracts the API URL from a JWT token
func GetAPIURLFromJWT(jwtString string) (string, error) {
	p := jwt.NewParser(jwt.WithoutClaimsValidation())
	var claims jwt.RegisteredClaims
	tokens, _, err := p.ParseUnverified(jwtString, &claims)
	if err != nil {
		return "", fmt.Errorf("failed to parse jwt: %w", err)
	}
	iss, err := tokens.Claims.GetIssuer()
	if err != nil {
		return "", fmt.Errorf("failed to get issuer from jwt: %w", err)
	}
	if iss == "https://shopmonkey.io" {
		// support for legacy tokens
		iss = "https://api.shopmonkey.cloud"
	}
	return iss, nil
}
