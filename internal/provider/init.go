package provider

import (
	"fmt"
	"net/url"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

func parseURLForProvider(urlstring string) (string, *url.URL, error) {
	u, err := url.Parse(urlstring)
	if err != nil {
		return "", nil, err
	}
	return u.Scheme, u, nil
}

type ProviderOpts struct {
	DryRun  bool
	Verbose bool
}

// NewProviderForURL will return a new internal.Provider for the driver based on the url
func NewProviderForURL(logger logger.Logger, urlstr string, opts *ProviderOpts) (internal.Provider, error) {
	driver, u, err := parseURLForProvider(urlstr)
	if err != nil {
		return nil, err
	}
	switch driver {
	case "file":
		qs := u.Query()
		args := []string{u.Path}
		for k, v := range qs {
			args = append(args, k)
			if len(v) > 0 {
				args = append(args, v...)
			}
		}
		return NewFileProvider(logger, args, opts)
	case "postgresql":
		return NewPostgresProvider(logger, urlstr, opts)
	default:
		return nil, fmt.Errorf("no suitable provider found for url: %s", urlstr)
	}
}
