package slurmdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"

	"solid/config"
	"solid/internal/pkg/client/slurmctl/models"
	"solid/internal/pkg/model"
)

// GormClient wraps a GORM DB connection for SlurmDB.
type Client struct {
	DB          *gorm.DB
	ClusterName string
	logger      *slog.Logger
}

// Close closes the underlying connection pool.
func (c *Client) Close() error {
	if c == nil || c.DB == nil {
		return nil
	}
	sqlDB, err := c.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// NewGorm creates a GORM Client configured from config.Slurmdb.
// New creates a read-only GORM Client configured from config.Slurmdb.
func New(cfg config.Slurmdb, logger *slog.Logger) (*Client, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	logger.Debug("build dsn", "dsn", dsn)

	gcfg := &gorm.Config{
		Logger: glogger.Default.LogMode(glogger.Warn),
	}

	db, err := gorm.Open(mysql.Open(dsn), gcfg)
	if err != nil {
		return nil, err
	}

    // Tune the underlying connection pool
    if sqlDB, err := db.DB(); err == nil {
        if cfg.MaxOpenConns > 0 {
            sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
        }
        if cfg.MaxIdleConns > 0 {
            sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
        }
        if d := parseDuration(cfg.ConnMaxLifetime); d > 0 {
            sqlDB.SetConnMaxLifetime(d)
        }
        // Proactive connectivity check with timeout to avoid hanging on unreachable DB
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := sqlDB.PingContext(ctx); err != nil {
            return nil, err
        }
    }

	// Enforce read-only at ORM layer
	enforceReadOnly(db)

	return &Client{DB: db, ClusterName: cfg.ClusterName, logger: logger}, nil
}

// buildDSN constructs a DSN string without importing the mysql driver package.
// Format: user:pass@tcp(host:port)/dbname?param=value
func buildDSN(cfg config.Slurmdb) (string, error) {
	// Credentials
	creds := cfg.User
	if cfg.Password != "" {
		// Password may contain special chars; percent-encode conservatively
		// as recommended by go-sql-driver/mysql when needed.
		creds = fmt.Sprintf("%s:%s", cfg.User, cfg.Password)
	}

	// Address and database
	addr := fmt.Sprintf("tcp(%s:%d)", cfg.Host, cfg.Port)
	dbname := cfg.Database

    // Params
    params := make([]string, 0, 8)
	if cfg.Charset != "" {
		params = append(params, fmt.Sprintf("charset=%s", cfg.Charset))
	}
	if cfg.ParseTime {
		params = append(params, "parseTime=true")
	} else {
		params = append(params, "parseTime=false")
	}
	if cfg.Loc != "" {
		params = append(params, fmt.Sprintf("loc=%s", url.QueryEscape(cfg.Loc)))
	}
    if cfg.TLS != "" {
        params = append(params, fmt.Sprintf("tls=%s", cfg.TLS))
    }
    // Set conservative timeouts to prevent hangs on connect/read/write
    // See https://github.com/go-sql-driver/mysql#dsn-data-source-name
    params = append(params, "timeout=5s")
    params = append(params, "readTimeout=5s")
    params = append(params, "writeTimeout=5s")

	dsn := fmt.Sprintf("%s@%s/%s", creds, addr, dbname)
	if len(params) > 0 {
		dsn = dsn + "?" + joinParams(params)
	}
	return dsn, nil
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

// joinParams joins DSN parameters with '&'.
func joinParams(params []string) string {
	if len(params) == 0 {
		return ""
	}
	out := params[0]
	for i := 1; i < len(params); i++ {
		out += "&" + params[i]
	}
	return out
}

// Package-level default Client for convenience wiring.
var defaultClient *Client

// SetDefault sets the package-level default SlurmDB Client.
func SetDefault(c *Client) { defaultClient = c }

// Default returns the package-level default SlurmDB Client.
func Default() *Client { return defaultClient }

// enforceReadOnly installs GORM callbacks that reject write operations and non-read raw SQL.
func enforceReadOnly(db *gorm.DB) {
	block := func(tx *gorm.DB) {
		tx.AddError(errors.New("slurmdb Client is read-only"))
	}
	// Block create/update/delete
	_ = db.Callback().Create().Before("gorm:create").Register("solid:readonly_create", block)
	_ = db.Callback().Update().Before("gorm:update").Register("solid:readonly_update", block)
	_ = db.Callback().Delete().Before("gorm:delete").Register("solid:readonly_delete", block)

	// Block raw/exec that are not read-only
	_ = db.Callback().Raw().Before("gorm:raw").Register("solid:readonly_raw", func(tx *gorm.DB) {
		sql := strings.TrimSpace(tx.Statement.SQL.String())
		up := strings.ToUpper(sql)
		if strings.HasPrefix(up, "SELECT") || strings.HasPrefix(up, "SHOW") || strings.HasPrefix(up, "DESCRIBE") || strings.HasPrefix(up, "EXPLAIN") {
			return
		}
		tx.AddError(errors.New("read-only: raw SQL must be SELECT/SHOW/DESCRIBE/EXPLAIN"))
	})
}

// GetUsers queries user_table with optional filters on deleted and admin_level.
// Pass nil for a filter to ignore it.
func (c *Client) GetUsers(ctx context.Context, deleted *int, adminLevel *int) (model.Users, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	res := make(model.Users, 0)
	tx := c.DB.WithContext(ctx).Model(&model.User{})
	if deleted != nil {
		tx = tx.Where("deleted = ?", *deleted)
	}
	if adminLevel != nil {
		tx = tx.Where("admin_level = ?", *adminLevel)
	}
	if err := tx.Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

// GetUsersPaged queries user_table with filters and pagination.
// Returns the paged users and the total count before paging.
func (c *Client) GetUsersPaged(ctx context.Context, deleted *int, adminLevel *int, offset, limit int) (model.Users, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}
	base := c.DB.WithContext(ctx).Model(&model.User{})
	if deleted != nil {
		base = base.Where("deleted = ?", *deleted)
	}
	if adminLevel != nil {
		base = base.Where("admin_level = ?", *adminLevel)
	}

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var res model.Users
	q := base
	if limit > 0 {
		q = q.Limit(limit)
	}
	if offset > 0 {
		q = q.Offset(offset)
	}
	if err := q.Find(&res).Error; err != nil {
		return nil, 0, err
	}
	return res, total, nil
}

// GetAccts queries acct_table with an optional deleted filter.
// Pass nil for deleted to ignore the filter.
func (c *Client) GetAccts(ctx context.Context, deleted *int) (model.Accounts, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	res := make(model.Accounts, 0)
	tx := c.DB.WithContext(ctx).Model(&model.Account{})
	if deleted != nil {
		tx = tx.Where("deleted = ?", *deleted)
	}
	if err := tx.Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

// GetAcctsPaged queries acct_table with an optional deleted filter and pagination.
// Returns the paged accounts and total count before paging.
func (c *Client) GetAcctsPaged(ctx context.Context, deleted *int, offset, limit int) (model.Accounts, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}
	base := c.DB.WithContext(ctx).Model(&model.Account{})
	if deleted != nil {
		base = base.Where("deleted = ?", *deleted)
	}
	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var res model.Accounts
	q := base
	if limit > 0 {
		q = q.Limit(limit)
	}
	if offset > 0 {
		q = q.Offset(offset)
	}
	if err := q.Find(&res).Error; err != nil {
		return nil, 0, err
	}
	return res, total, nil
}

// GetAcctByName returns a single account by name from acct_table with an optional deleted filter.
func (c *Client) GetAcctByName(ctx context.Context, name string, deleted *int) (*model.Account, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("account name is required")
	}
	var acct model.Account
	tx := c.DB.WithContext(ctx).Model(&model.Account{})
	if deleted != nil {
		tx = tx.Where("deleted = ?", *deleted)
	}
	if err := tx.Where("name = ?", name).First(&acct).Error; err != nil {
		return nil, err
	}
	return &acct, nil
}

// GetSubAccountsAndUsers returns direct child accounts (by parent_acct) and users
// associated to the given account in <ClusterName>_assoc_table (deleted=0 only).
func (c *Client) GetSubAccountsAndUsers(ctx context.Context, account string) ([]string, []string, error) {
	if c == nil || c.DB == nil {
		return nil, nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return nil, nil, fmt.Errorf("account name is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := fmt.Sprintf("%s_assoc_table", c.ClusterName)

	// Sub-accounts: rows with user='' and parent_acct = account
	var subAccts []string
	if err := c.DB.WithContext(ctx).
		Table(table).
		Where("parent_acct = ? AND deleted = 0 AND `user` = ''", account).
		Distinct().
		Pluck("acct", &subAccts).Error; err != nil {
		return nil, nil, err
	}

	// Sub-users: rows with acct=account and user<>''
	var subUsers []string
	if err := c.DB.WithContext(ctx).
		Table(table).
		Where("acct = ? AND deleted = 0 AND `user` <> ''", account).
		Distinct().
		Pluck("`user`", &subUsers).Error; err != nil {
		return nil, nil, err
	}
	return subAccts, subUsers, nil
}

// GetParentAccountsByUser returns distinct account names (acct) associated with a user
// from <ClusterName>_assoc_table with deleted=0.
func (c *Client) GetParentAccountsByUser(ctx context.Context, username string) ([]string, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(username) == "" {
		return nil, fmt.Errorf("username is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := fmt.Sprintf("%s_assoc_table", c.ClusterName)
	var accts []string
	if err := c.DB.WithContext(ctx).
		Table(table).
		Where("`user` = ? AND deleted = 0", username).
		Distinct().
		Pluck("acct", &accts).Error; err != nil {
		return nil, err
	}
	return accts, nil
}

// GetUserAssociations fetches association rows for a given username from
// the cluster-specific assoc table (<ClusterName>_assoc_table), excluding deleted rows.
func (c *Client) GetUserAssociations(ctx context.Context, username string) ([]model.UserAssociation, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(username) == "" {
		return nil, fmt.Errorf("username is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := model.AssocTableName(c.ClusterName)

	var rows []model.UserAssociation
	q := c.DB.WithContext(ctx).Table(table).
		Where("`user` = ? AND deleted = 0", username)
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// ErrMultipleAssociations indicates that more than one association matched the filter.
var ErrMultipleAssociations = errors.New("multiple associations matched")

// FindAssociationOne finds a single association in <ClusterName>_assoc_table by filters.
// Required: account. Optional: user, partition. Always filters deleted=0.
// Returns ErrMultipleAssociations if more than one row matches.
func (c *Client) FindAssociationOne(ctx context.Context, account string, user, partition *string) (*model.UserAssociation, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("account is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := model.AssocTableName(c.ClusterName)
	tx := c.DB.WithContext(ctx).Table(table).Where("deleted = 0 AND acct = ?", account)
	if user != nil && strings.TrimSpace(*user) != "" {
		tx = tx.Where("`user` = ?", *user)
	}
	if partition != nil && strings.TrimSpace(*partition) != "" {
		tx = tx.Where("`partition` = ?", *partition)
	}
	var rows []model.UserAssociation
	if err := tx.Find(&rows).Error; err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, gorm.ErrRecordNotFound
	case 1:
		return &rows[0], nil
	default:
		return nil, ErrMultipleAssociations
	}
}

// GetUserNamesByAccount returns distinct user names that belong to the given
// account in the cluster-specific assoc table (<ClusterName>_assoc_table).
// Only non-deleted (deleted = 0) user nodes are returned; account nodes are
// excluded by requiring `user` to be non-empty.
func (c *Client) GetUserNamesByAccount(ctx context.Context, account string) ([]string, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := fmt.Sprintf("%s_assoc_table", c.ClusterName)

	var users []string
	tx := c.DB.WithContext(ctx).
		Table(table).
		Where("acct = ? AND `user` <> '' AND deleted = 0", account).
		Distinct().
		Pluck("`user`", &users)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return users, nil
}

type JobsFilter struct{}

// GetJobs 获取
func (sc *Client) GetJobs(ctx context.Context, filter JobsFilter, page, pageSize int64) (models.Jobs, int64, error) {
	return nil, 0, nil
}

// GetQos 获取 QoS, 若 id 为 -1 表示获取所有 QoS, 否则只获取指定 QoS 信息.
func (sc *Client) GetQos(ctx context.Context, id int) (model.Qoses, error) {
	if sc == nil || sc.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	res := make(model.Qoses, 0)
	tx := sc.DB.WithContext(ctx).Model(&model.Qos{}).Where("deleted = 0")
	if id != -1 {
		tx = tx.Where("id = ?", id)
	}
	if err := tx.Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}
