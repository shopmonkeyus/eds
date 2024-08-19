package upgrade

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ProtonMail/gopenpgp/v3/crypto"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type UpgradeConfig struct {
	Logger       logger.Logger
	Context      context.Context
	BinaryURL    string
	SignatureURL string
	Filename     string
	PublicKey    string
}

// Upgrade downloads the binary from the given URL, verifies the signature with the given public key, and installs the binary in the given directory.
func Upgrade(config UpgradeConfig) error {
	started := time.Now()
	defer func() {
		config.Logger.Debug("download took %s", time.Since(started))
	}()
	tmp, err := os.CreateTemp("", "eds")
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	config.Logger.Trace("created temp file %s to download archive", tmp.Name())
	publicKey, err := crypto.NewKeyFromArmored(config.PublicKey)
	if err != nil {
		return fmt.Errorf("error reading public key: %w", err)
	}
	pgp := crypto.PGP()
	verifier, err := pgp.Verify().VerificationKey(publicKey).New()
	if err != nil {
		return fmt.Errorf("error creating PGP verifier: %w", err)
	}
	config.Logger.Trace("created PGP verifier using public key")
	req, err := http.NewRequest("GET", config.BinaryURL, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}
	resp, err := util.NewHTTPRetry(req.WithContext(config.Context)).Do()
	if err != nil {
		return fmt.Errorf("error downloading binary from %s: %w", config.BinaryURL, err)
	}
	defer resp.Body.Close()
	binaryLen, err := io.Copy(tmp, resp.Body)
	if err != nil {
		return fmt.Errorf("error copying binary data: %w", err)
	}
	resp.Body.Close()
	tmp.Close()
	config.Logger.Debug("downloaded binary of size %d bytes from %s", binaryLen, config.BinaryURL)
	sreq, err := http.NewRequest("GET", config.SignatureURL, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}
	sresp, err := util.NewHTTPRetry(sreq.WithContext(config.Context)).Do()
	if err != nil {
		return fmt.Errorf("error downloading signature from %s: %w", config.SignatureURL, err)
	}
	defer sresp.Body.Close()
	signature, err := io.ReadAll(sresp.Body)
	if err != nil {
		return fmt.Errorf("error reading signature: %w", err)
	}
	sresp.Body.Close()
	config.Logger.Debug("downloaded signature of size %d bytes from %s", len(signature), config.SignatureURL)
	of, err := os.Open(tmp.Name())
	if err != nil {
		return fmt.Errorf("error opening file %s: %w", tmp.Name(), err)
	}
	defer of.Close()
	reader, err := verifier.VerifyingReader(of, bytes.NewReader(signature), crypto.Auto)
	if err != nil {
		return fmt.Errorf("error verifying signature data: %w", err)
	}
	verifyResult, err := reader.ReadAllAndVerifySignature()
	if err != nil {
		return fmt.Errorf("error verifying signature: %w", err)
	}
	if sigErr := verifyResult.SignatureError(); sigErr != nil {
		return fmt.Errorf("error in signature verification: %w", sigErr)
	}
	config.Logger.Debug("verified signature of binary")
	of.Close()
	of, err = os.Create(config.Filename)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", config.Filename, err)
	}
	defer of.Close()
	if filepath.Ext(config.BinaryURL) == ".zip" {
		config.Logger.Debug("extracting zip file: %s", tmp.Name())
		uz, err := zip.OpenReader(tmp.Name())
		if err != nil {
			return fmt.Errorf("error opening zip file: %w", err)
		}
		defer uz.Close()
		for _, f := range uz.File {
			config.Logger.Trace("zip file: %s", f.Name)
			if filepath.Ext(f.Name) == ".exe" {
				af, err := f.Open()
				if err != nil {
					return fmt.Errorf("error opening file in archive: %w", err)
				}
				if _, err := io.Copy(of, af); err != nil {
					return fmt.Errorf("error copying file from archive: %w", err)
				}
				af.Close()
				return nil
			}
		}
	} else {
		config.Logger.Debug("extracting tar.gz file: %s", tmp.Name())
		tmpf, err := os.Open(tmp.Name())
		if err != nil {
			return fmt.Errorf("error opening file %s: %w", tmp.Name(), err)
		}
		gz, err := gzip.NewReader(tmpf)
		if err != nil {
			return fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			header, err := tr.Next()
			if err != nil {
				return fmt.Errorf("error reading tar header: %w", err)
			}
			config.Logger.Trace("tar file: %s", header.Name)
			if header.Name == "eds" {
				if _, err := io.Copy(of, tr); err != nil {
					return fmt.Errorf("error copying file from archive: %w", err)
				}
				break
			}
		}
		gz.Close()
		tmpf.Close()
	}
	config.Logger.Trace("setting permissions on file %s", config.Filename)
	if err := os.Chmod(config.Filename, 0755); err != nil {
		return fmt.Errorf("error setting permissions on file: %w", err)
	}
	return nil
}
