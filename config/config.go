package config

import (
    "os"

    "gopkg.in/yaml.v3"
)

type Config struct {
    Server Server `yaml:"server"`
}

type Server struct {
    Slurmdb Slurmdb `yaml:"slurmdb"`
    LDAP    LDAP    `yaml:"ldap"`
}

type Slurmdb struct {
    ClusterName     string `yaml:"ClusterName"`
    Host            string `yaml:"host"`
    Port            int    `yaml:"port"`
    User            string `yaml:"user"`
    Password        string `yaml:"password"`
    Database        string `yaml:"database"`
    Charset         string `yaml:"charset"`
    ParseTime       bool   `yaml:"parseTime"`
    Loc             string `yaml:"loc"`
    TLS             string `yaml:"tls"`
    MaxOpenConns    int    `yaml:"maxOpenConns"`
    MaxIdleConns    int    `yaml:"maxIdleConns"`
    ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

type LDAP struct {
    Host               string `yaml:"host"`
    Port               int    `yaml:"port"`
    UseTLS             bool   `yaml:"useTLS"`
    StartTLS           bool   `yaml:"startTLS"`
    InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
    ServerName         string `yaml:"serverName"`
    RootCAFile         string `yaml:"rootCAFile"`
    ClientCertFile     string `yaml:"clientCertFile"`
    ClientKeyFile      string `yaml:"clientKeyFile"`
    BindDN             string `yaml:"bindDN"`
    BindPassword       string `yaml:"bindPassword"`
    BaseDN             string `yaml:"baseDN"`
    ConnectTimeout     string `yaml:"connectTimeout"`
    ReadTimeout        string `yaml:"readTimeout"`
}

// Load reads a YAML config file from the given path and unmarshals into Config.
func Load(path string) (*Config, error) {
    b, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var cfg Config
    if err := yaml.Unmarshal(b, &cfg); err != nil {
        return nil, err
    }
    return &cfg, nil
}
