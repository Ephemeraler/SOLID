package slurmdb

import (
    "context"
    "errors"
    "fmt"
    "net/url"
    "strings"
    "time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"

	"solid/config"
	"solid/internal/pkg/model"
)

// GormClient wraps a GORM DB connection for SlurmDB.
type Client struct {
	DB          *gorm.DB
	ClusterName string
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

// NewGorm creates a GORM client configured from config.Slurmdb.
// New creates a read-only GORM client configured from config.Slurmdb.
func New(cfg config.Slurmdb) (*Client, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

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
		// Optional: warm up ping
		_ = sqlDB.Ping()
	}

	// Enforce read-only at ORM layer
	enforceReadOnly(db)

	return &Client{DB: db, ClusterName: cfg.ClusterName}, nil
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
	params := make([]string, 0, 4)
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

// Package-level default client for convenience wiring.
var defaultClient *Client

// SetDefault sets the package-level default SlurmDB client.
func SetDefault(c *Client) { defaultClient = c }

// Default returns the package-level default SlurmDB client.
func Default() *Client { return defaultClient }

// enforceReadOnly installs GORM callbacks that reject write operations and non-read raw SQL.
func enforceReadOnly(db *gorm.DB) {
	block := func(tx *gorm.DB) {
		tx.AddError(errors.New("slurmdb client is read-only"))
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
		return nil, fmt.Errorf("nil slurmdb client")
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
        return nil, 0, fmt.Errorf("nil slurmdb client")
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
        return nil, fmt.Errorf("nil slurmdb client")
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
        return nil, 0, fmt.Errorf("nil slurmdb client")
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

// GetUserNamesByAccount returns distinct user names that belong to the given
// account in the cluster-specific assoc table (<ClusterName>_assoc_table).
// Only non-deleted (deleted = 0) user nodes are returned; account nodes are
// excluded by requiring `user` to be non-empty.
func (c *Client) GetUserNamesByAccount(ctx context.Context, account string) ([]string, error) {
    if c == nil || c.DB == nil {
        return nil, fmt.Errorf("nil slurmdb client")
    }
    if strings.TrimSpace(account) == "" {
        return nil, fmt.Errorf("account name is required")
    }
    if strings.TrimSpace(c.ClusterName) == "" {
        return nil, fmt.Errorf("cluster name is empty in slurmdb client")
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
