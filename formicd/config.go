package main

import (
	"os"
	"strconv"
)

type config struct {
	path               string
	port               int
	oortValueSyndicate string
	oortGroupSyndicate string
	insecureSkipVerify bool
	skipMutualTLS      bool
}

func resolveConfig(c *config) *config {
	cfg := &config{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("FORMICD_PATH"); env != "" {
		cfg.path = env
	}
	if cfg.path == "" {
		cfg.path = "/var/lib/formic"
	}
	if env := os.Getenv("FORMICD_PORT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.port = val
		}
	}
	if cfg.port == 0 {
		cfg.port = 8445
	}
	if env := os.Getenv("FORMICD_OORT_VALUE_SYNDICATE"); env != "" {
		cfg.oortValueSyndicate = env
	}
	// cfg.oortValueSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_OORT_GROUP_SYNDICATE"); env != "" {
		cfg.oortGroupSyndicate = env
	}
	// cfg.oortGroupSyndicate == "" means default SRV resolution.
	if env := os.Getenv("FORMICD_INSECURE_SKIP_VERIFY"); env == "true" {
		cfg.insecureSkipVerify = true
	}
	if env := os.Getenv("FORMICD_SKIP_MUTUAL_TLS"); env == "true" {
		cfg.skipMutualTLS = true
	}
	return cfg
}
