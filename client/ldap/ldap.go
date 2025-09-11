package ldap

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "net"
    "os"
    "strings"
    "time"

    gldap "github.com/go-ldap/ldap/v3"

    "solid/config"
    "solid/internal/pkg/model"
)

// Client wraps an established LDAP connection.
type Client struct {
    Conn         *gldap.Conn
    BaseDN       string
    UsernameAttr string
}

// Close closes the underlying LDAP connection.
func (c *Client) Close() {
    if c != nil && c.Conn != nil {
        c.Conn.Close()
    }
}

// Package-level default client for convenience wiring across handlers.
var defaultClient *Client

// SetDefault sets the package-level default LDAP client.
func SetDefault(c *Client) { defaultClient = c }

// Default returns the package-level default LDAP client.
func Default() *Client { return defaultClient }

// New creates and binds an LDAP client connection based on the provided config.
// It supports plain LDAP, LDAPS, and STARTTLS, optional custom CAs and client certs,
// and connect/read timeouts.
func New(cfg config.LDAP) (*Client, error) {
	// Build TLS config if any TLS-related options are set.
	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Determine scheme and address.
	scheme := "ldap"
	if cfg.UseTLS {
		scheme = "ldaps"
	}
	addr := fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, cfg.Port)

	// Build dial options with optional TLS and timeouts.
	var opts []gldap.DialOpt
	if tlsCfg != nil {
		opts = append(opts, gldap.DialWithTLSConfig(tlsCfg))
	}
	if d := connectDialer(cfg); d != nil {
		opts = append(opts, gldap.DialWithDialer(d))
	}

	// Dial the server.
	conn, err := gldap.DialURL(addr, opts...)
	if err != nil {
		return nil, err
	}

	// If requested, upgrade to TLS via STARTTLS (not needed when using LDAPS).
	if cfg.StartTLS && !cfg.UseTLS {
		if err := conn.StartTLS(tlsCfg); err != nil {
			conn.Close()
			return nil, err
		}
	}

	// Apply read timeout if provided.
	if rt := parseDuration(cfg.ReadTimeout); rt > 0 {
		conn.SetTimeout(rt)
	}

	// Perform bind if credentials are provided.
	if cfg.BindDN != "" || cfg.BindPassword != "" {
		if err := conn.Bind(cfg.BindDN, cfg.BindPassword); err != nil {
			conn.Close()
			return nil, err
		}
	}

    usernameAttr := "uid"
    return &Client{Conn: conn, BaseDN: cfg.BaseDN, UsernameAttr: usernameAttr}, nil
}

// buildTLSConfig constructs a tls.Config based on config.LDAP.
// Returns nil if no TLS options are needed and UseTLS/StartTLS are false.
func buildTLSConfig(cfg config.LDAP) (*tls.Config, error) {
	needsTLS := cfg.UseTLS || cfg.StartTLS || cfg.InsecureSkipVerify || cfg.RootCAFile != "" || cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" || cfg.ServerName != ""
	if !needsTLS {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // configurable for testing/non-prod
	}
	if cfg.ServerName != "" {
		tlsCfg.ServerName = cfg.ServerName
	}

	// Load custom Root CA if provided.
	if cfg.RootCAFile != "" {
		pem, err := os.ReadFile(cfg.RootCAFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if ok := pool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("failed to append Root CA from %s", cfg.RootCAFile)
		}
		tlsCfg.RootCAs = pool
	}

	// Load client certificate if provided.
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// connectDialer builds a net.Dialer with the configured timeout.
func connectDialer(cfg config.LDAP) *net.Dialer {
	to := parseDuration(cfg.ConnectTimeout)
	if to <= 0 {
		return nil
	}
	return &net.Dialer{Timeout: to}
}

// parseDuration returns 0 on empty or invalid duration strings.
func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// GetUserAttributesByUIDs searches LDAP for users by uid and returns a list of
// model.User with LDAPAttrs populated. Non-LDAP fields remain zero values.
func (c *Client) GetUserAttributesByUIDs(ctx context.Context, usernames []string) (model.Users, error) {
    if c == nil || c.Conn == nil {
        return nil, fmt.Errorf("ldap client not initialized")
    }
    if len(usernames) == 0 {
        return model.Users{}, nil
    }
    parts := make([]string, 0, len(usernames))
    for _, u := range usernames {
        if u == "" {
            continue
        }
        parts = append(parts, fmt.Sprintf("(%s=%s)", c.UsernameAttr, gldap.EscapeFilter(u)))
    }
    if len(parts) == 0 {
        return model.Users{}, nil
    }
    filter := fmt.Sprintf("(|%s)", strings.Join(parts, ""))

    req := gldap.NewSearchRequest(
        c.BaseDN,
        gldap.ScopeWholeSubtree,
        gldap.NeverDerefAliases,
        0, 0, false,
        filter,
        []string{},
        nil,
    )
    // go-ldap doesn't accept context in Search; timeouts handled by conn
    resp, err := c.Conn.Search(req)
    if err != nil {
        return nil, err
    }
    out := make(model.Users, 0, len(resp.Entries))
    for _, e := range resp.Entries {
        attrs := make(map[string][]string, len(e.Attributes))
        for _, a := range e.Attributes {
            vv := make([]string, len(a.Values))
            copy(vv, a.Values)
            attrs[a.Name] = vv
        }
        name := e.GetAttributeValue(c.UsernameAttr)
        if name == "" {
            name = e.GetAttributeValue("cn")
        }
        out = append(out, model.User{
            Name:      name,
            LDAPAttrs: attrs,
        })
    }
    return out, nil
}
