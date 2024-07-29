package util

import (
	"fmt"
	"net/url"
	"strings"

	cstr "github.com/shopmonkeyus/go-common/string"
)

// MaskURL returns a masked version of the URL string attempting to hide sensitive information.
func MaskURL(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	var str strings.Builder
	str.WriteString(u.Scheme)
	str.WriteString("://")
	if u.User != nil {
		str.WriteString(cstr.Mask(u.User.Username()))
		pass, ok := u.User.Password()
		if ok {
			str.WriteString(":")
			str.WriteString(cstr.Mask(pass))
		}
		str.WriteString("@")
	}
	str.WriteString(u.Host)
	p := u.Path
	if p != "/" {
		str.WriteString("/")
		if len(p) > 1 && p[0] == '/' {
			str.WriteString(cstr.Mask(p[1:]))
		}
	}
	qs := u.Query().Encode()
	if qs != "" {
		str.WriteString("?")
		str.WriteString(cstr.Mask(qs))
	}
	return str.String(), nil
}
