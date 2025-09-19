package ldap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	gldap "github.com/go-ldap/ldap/v3"

	"solid/config"
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

type Attribute map[string]string

// GetUsers 获取ou=Peoples,<c.BaseDN> 下所有 uid 条目的属性(用户), 输出结果按照 uidNumber 升序排列
func (c *Client) GetUsers(ctx context.Context) ([]Attribute, error) {
	if c == nil || c.Conn == nil {
		return nil, fmt.Errorf("nil ldap client or connection")
	}

	base := fmt.Sprintf("ou=Peoples,%s", c.BaseDN)

	req := gldap.NewSearchRequest(
		base,
		gldap.ScopeSingleLevel,
		gldap.NeverDerefAliases,
		0,
		0,
		false,
		"(uid=*)",
		[]string{"*", "+"},
		nil,
	)

	const step = 500
	res, err := c.Conn.SearchWithPaging(req, step)
	if err != nil {
		return nil, err
	}

	type userItem struct {
		uidNumber int
		attrs     Attribute
	}

	items := make([]userItem, 0, len(res.Entries))
	for _, e := range res.Entries {
		// uid=xxx entries are expected to have uidNumber; skip if missing or invalid
		uidNumStr := e.GetAttributeValue("uidNumber")
		if uidNumStr == "" {
			continue
		}
		uidNum, err := strconv.Atoi(uidNumStr)
		if err != nil {
			continue
		}
		attrs := make(Attribute, len(e.Attributes))
		for _, a := range e.Attributes {
			vals := make([]string, len(a.Values))
			copy(vals, a.Values)
			attrs[a.Name] = strings.Join(vals, ",")
		}
		items = append(items, userItem{uidNumber: uidNum, attrs: attrs})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].uidNumber < items[j].uidNumber })

	out := make([]Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, it.attrs)
	}
	return out, nil
}

// GetAdditionalGroupsOfUser 获取用户的附加组. 附加组信息存储在 ou=Groups,<c.BaseDN> 下 cn 条目(用户组)中的 memberUid 中.
func (c *Client) GetAdditionalGroupsOfUser(ctx context.Context, uid string) ([]string, error) {
	if c == nil || c.Conn == nil {
		return nil, fmt.Errorf("nil ldap client or connection")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, fmt.Errorf("uid is required")
	}

	base := fmt.Sprintf("ou=Groups,%s", c.BaseDN)
	filter := fmt.Sprintf("(memberUid=%s)", gldap.EscapeFilter(uid))
	req := gldap.NewSearchRequest(
		base,
		gldap.ScopeSingleLevel,
		gldap.NeverDerefAliases,
		0,
		0,
		false,
		filter,
		[]string{"cn"},
		nil,
	)

	const step = 500
	res, err := c.Conn.SearchWithPaging(req, step)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, len(res.Entries))
	for _, e := range res.Entries {
		cns := e.GetAttributeValues("cn")
		for _, v := range cns {
			v = strings.TrimSpace(v)
			if v != "" {
				groups = append(groups, v)
			}
		}
	}
	sort.Strings(groups)
	return groups, nil
}

// GetUser 获取ou=Peoples,<c.BaseDN> 下 uid 条目的属性(用户).
func (c *Client) GetUser(ctx context.Context, uid string) (Attribute, error) {
	if c == nil || c.Conn == nil {
		return nil, fmt.Errorf("nil ldap client or connection")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, fmt.Errorf("uid is required")
	}

	base := fmt.Sprintf("ou=Peoples,%s", c.BaseDN)
	filter := fmt.Sprintf("(uid=%s)", gldap.EscapeFilter(uid))
	req := gldap.NewSearchRequest(
		base,
		gldap.ScopeSingleLevel,
		gldap.NeverDerefAliases,
		2, // size limit small, expect a single match
		0,
		false,
		filter,
		[]string{"*", "+"},
		nil,
	)
	res, err := c.Conn.Search(req)
	if err != nil {
		return nil, err
	}
	if len(res.Entries) == 0 {
		return nil, nil // not found
	}
	e := res.Entries[0]
	attrs := make(Attribute, len(e.Attributes))
	for _, a := range e.Attributes {
		vals := make([]string, len(a.Values))
		copy(vals, a.Values)
		attrs[a.Name] = strings.Join(vals, ",")
	}
	return attrs, nil
}

// DelUser 删除ou=Peoples,<c.BaseDN> 下 uid 条目(用户).
func (c *Client) DelUser(ctx context.Context, uid string) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return fmt.Errorf("uid is required")
	}

	dn := fmt.Sprintf("uid=%s,ou=Peoples,%s", gldap.EscapeDN(uid), c.BaseDN)
	req := gldap.NewDelRequest(dn, nil)
	// go-ldap Conn methods don't accept context; timeout should be set on the connection if needed
	return c.Conn.Del(req)
}

// AddUser 在 ou=Peoples,<c.BaseDN> 下新增 uid 条目(用户). ObjectClass=[inetOrgPerson, posixAccount, shadowAccount]
func (c *Client) AddUser(ctx context.Context, uid string, attr Attribute) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return fmt.Errorf("uid is required")
	}
	if attr == nil {
		attr = make(Attribute)
	}

	// DN under ou=Peoples
	dn := fmt.Sprintf("uid=%s,ou=Peoples,%s", gldap.EscapeDN(uid), c.BaseDN)

	// Normalize attributes map -> []string, splitting on comma
	toVals := func(s string) []string {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	normalized := make(map[string][]string, len(attr)+2)
	for k, v := range attr {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		vals := toVals(v)
		if len(vals) > 0 {
			normalized[k] = vals
		}
	}

	// Ensure uid attribute contains the provided uid
	if vals, ok := normalized["uid"]; ok {
		found := false
		for _, x := range vals {
			if x == uid {
				found = true
				break
			}
		}
		if !found {
			normalized["uid"] = append(vals, uid)
		}
	} else {
		normalized["uid"] = []string{uid}
	}

	// Ensure required objectClass set
	requiredOC := []string{"inetOrgPerson", "posixAccount", "shadowAccount"}
	ocSet := make(map[string]struct{}, len(requiredOC))
	for _, oc := range toVals(attr["objectClass"]) {
		ocSet[oc] = struct{}{}
	}
	for _, oc := range requiredOC {
		ocSet[oc] = struct{}{}
	}
	ocs := make([]string, 0, len(ocSet))
	for oc := range ocSet {
		ocs = append(ocs, oc)
	}
	sort.Strings(ocs)
	normalized["objectClass"] = ocs

	// Build add request
	req := gldap.NewAddRequest(dn, nil)
	for k, vals := range normalized {
		if len(vals) == 0 {
			continue
		}
		req.Attribute(k, vals)
	}

	// Execute add
	return c.Conn.Add(req)
}

// UpdateUser 在 ou=Peoples,<c.BaseDN> 下更新 uid 条目属性(用户), 不允许更新 ObjectClass 和 uid.
// 传入的 attr 为属性到字符串的映射；若值包含逗号，将被拆分为多值；
// 若某属性值为空字符串，将对其执行删除操作。
func (c *Client) UpdateUser(ctx context.Context, uid string, attr Attribute) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return fmt.Errorf("uid is required")
	}
	if attr == nil {
		return fmt.Errorf("attributes required")
	}

	dn := fmt.Sprintf("uid=%s,ou=Peoples,%s", gldap.EscapeDN(uid), c.BaseDN)
	req := gldap.NewModifyRequest(dn, nil)

	toVals := func(s string) []string {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	ops := 0
	for k, v := range attr {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		// Disallow updates to objectClass and uid
		kl := strings.ToLower(key)
		if kl == "objectclass" || kl == "uid" {
			continue
		}
		vals := toVals(v)
		if len(vals) == 0 {
			req.Delete(key, nil)
			ops++
			continue
		}
		req.Replace(key, vals)
		ops++
	}
	if ops == 0 {
		return nil
	}
	return c.Conn.Modify(req)
}

// GetGroups 获取ou=Groups,<c.BaseDN> 下所有 cn 条目(用户组).
func (c *Client) GetGroups(ctx context.Context) ([]Attribute, error) {
	if c == nil || c.Conn == nil {
		return nil, fmt.Errorf("nil ldap client or connection")
	}
	base := fmt.Sprintf("ou=Groups,%s", c.BaseDN)
	req := gldap.NewSearchRequest(
		base,
		gldap.ScopeSingleLevel,
		gldap.NeverDerefAliases,
		0,
		0,
		false,
		"(cn=*)",
		[]string{"*", "+"},
		nil,
	)
	const step = 500
	res, err := c.Conn.SearchWithPaging(req, step)
	if err != nil {
		return nil, err
	}
	type grp struct {
		gidNumber int
		attrs     Attribute
	}
	items := make([]grp, 0, len(res.Entries))
	for _, e := range res.Entries {
		gidStr := e.GetAttributeValue("gidNumber")
		if gidStr == "" {
			continue
		}
		gid, err := strconv.Atoi(gidStr)
		if err != nil {
			continue
		}
		attrs := make(Attribute, len(e.Attributes))
		for _, a := range e.Attributes {
			vals := make([]string, len(a.Values))
			copy(vals, a.Values)
			attrs[a.Name] = strings.Join(vals, ",")
		}
		items = append(items, grp{gidNumber: gid, attrs: attrs})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].gidNumber < items[j].gidNumber })
	out := make([]Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, it.attrs)
	}
	return out, nil
}

// GetGroup 获取ou=Groups,<c.BaseDN> 下 cn 条目(用户组).
func (c *Client) GetGroup(ctx context.Context, cn string) (Attribute, error) {
	if c == nil || c.Conn == nil {
		return nil, fmt.Errorf("nil ldap client or connection")
	}
	cn = strings.TrimSpace(cn)
	if cn == "" {
		return nil, fmt.Errorf("cn is required")
	}
	base := fmt.Sprintf("ou=Groups,%s", c.BaseDN)
	filter := fmt.Sprintf("(cn=%s)", gldap.EscapeFilter(cn))
	req := gldap.NewSearchRequest(
		base,
		gldap.ScopeSingleLevel,
		gldap.NeverDerefAliases,
		2,
		0,
		false,
		filter,
		[]string{"*", "+"},
		nil,
	)
	res, err := c.Conn.Search(req)
	if err != nil {
		return nil, err
	}
	if len(res.Entries) == 0 {
		return nil, nil
	}
	e := res.Entries[0]
	attrs := make(Attribute, len(e.Attributes))
	for _, a := range e.Attributes {
		vals := make([]string, len(a.Values))
		copy(vals, a.Values)
		attrs[a.Name] = strings.Join(vals, ",")
	}
	return attrs, nil
}

// DelGroup 删除ou=Groups,<c.BaseDN> 下 cn 条目(用户组).
func (c *Client) DelGroup(ctx context.Context, cn string) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	cn = strings.TrimSpace(cn)
	if cn == "" {
		return fmt.Errorf("cn is required")
	}
	dn := fmt.Sprintf("cn=%s,ou=Groups,%s", gldap.EscapeDN(cn), c.BaseDN)
	req := gldap.NewDelRequest(dn, nil)
	return c.Conn.Del(req)
}

// AddGroup 在 ou=Groups,<c.BaseDN> 下新增 cn 条目(用户组), ObjectClass=["top", "organizationalUnit"]
func (c *Client) AddGroup(ctx context.Context, cn string, attr Attribute) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	cn = strings.TrimSpace(cn)
	if cn == "" {
		return fmt.Errorf("cn is required")
	}
	// Build DN under ou=Groups
	dn := fmt.Sprintf("cn=%s,ou=Groups,%s", gldap.EscapeDN(cn), c.BaseDN)

	// Normalize attributes: split comma-separated values, trim empties
	toVals := func(s string) []string {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	normalized := make(map[string][]string, len(attr)+2)
	for k, v := range attr {
		key := strings.TrimSpace(k)
		if key == "" || strings.EqualFold(key, "dn") {
			continue
		}
		vals := toVals(v)
		if len(vals) > 0 {
			normalized[key] = vals
		}
	}

	// Ensure cn attribute includes provided cn
	if vals, ok := normalized["cn"]; ok {
		present := false
		for _, x := range vals {
			if x == cn {
				present = true
				break
			}
		}
		if !present {
			normalized["cn"] = append(vals, cn)
		}
	} else {
		normalized["cn"] = []string{cn}
	}

	// Ensure objectClass includes posixGroup (common for groups)
	ocSet := map[string]struct{}{}
	if ocs, ok := normalized["objectClass"]; ok {
		for _, oc := range ocs {
			ocSet[oc] = struct{}{}
		}
	}
	ocSet["posixGroup"] = struct{}{}
	// Render objectClass values deterministically
	ocs := make([]string, 0, len(ocSet))
	for oc := range ocSet {
		ocs = append(ocs, oc)
	}
	sort.Strings(ocs)
	normalized["objectClass"] = ocs

	req := gldap.NewAddRequest(dn, nil)
	for k, vs := range normalized {
		if len(vs) == 0 {
			continue
		}
		req.Attribute(k, vs)
	}
	return c.Conn.Add(req)
}

// UpdateGroup 更新 ou=Groups,<c.BaseDN> 下 cn 条目(用户组), 若 cn 不存在则不需要更新, ObjectClass=["top", "organizationalUnit"]
func (c *Client) UpdateGroup(ctx context.Context, cn string, attr Attribute) error {
	if c == nil || c.Conn == nil {
		return fmt.Errorf("nil ldap client or connection")
	}
	cn = strings.TrimSpace(cn)
	if cn == "" {
		return fmt.Errorf("cn is required")
	}
	if attr == nil {
		return fmt.Errorf("attributes required")
	}

	dn := fmt.Sprintf("cn=%s,ou=Groups,%s", gldap.EscapeDN(cn), c.BaseDN)
	req := gldap.NewModifyRequest(dn, nil)

	toVals := func(s string) []string {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	ops := 0
	for k, v := range attr {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		kl := strings.ToLower(key)
		// Disallow updates to cn and objectClass
		if kl == "cn" || kl == "objectclass" {
			continue
		}
		vals := toVals(v)
		if len(vals) == 0 {
			req.Delete(key, nil)
			ops++
			continue
		}
		req.Replace(key, vals)
		ops++
	}
	if ops == 0 {
		return nil
	}
	return c.Conn.Modify(req)
}
