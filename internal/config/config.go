package config

import (
	"flag"
	"log"
)

type Config struct {
	dataBridgeURL  string
	dataDir        string
	port           string
	allowedOrigins string
	noAudit        bool
}

func (c *Config) DataBridgeURL() string {
	return c.dataBridgeURL
}

func (c *Config) DataDir() string {
	return c.dataDir
}

func (c *Config) Port() string {
	return c.port
}

func (c *Config) AllowedOrigins() string {
	return c.allowedOrigins
}

func (c *Config) HasAudit() bool {
	return !c.noAudit
}

var serviceConfig = &Config{
	dataBridgeURL:  "http://localhost:9701",
	dataDir:        "",
	port:           "9702",
	allowedOrigins: "*",
}

func ServiceConfig() *Config {
	return serviceConfig
}

func LoadConfig() {
	// Parse flags
	confDataBridgeUrl := flag.String("bridge-url", "http://localhost:9701", "path to the api bridge service")
	confDataDir := flag.String("data-dir", "", "path to the local data directory")
	confPort := flag.String("port", "9702", "port from which to run the service")
	confAllowedOrigins := flag.String("origins", "*", "cors origins")
	confNoAudit := flag.Bool("no-audit", false, "disables audit on startup")
	flag.Parse()

	// Check validity
	if *confDataDir == "" {
		log.Fatalln("data directory is not specified")
	}

	// Set all config variables
	serviceConfig.dataBridgeURL = *confDataBridgeUrl
	serviceConfig.dataDir = *confDataDir
	serviceConfig.port = *confPort
	serviceConfig.allowedOrigins = *confAllowedOrigins
	serviceConfig.noAudit = *confNoAudit
}
