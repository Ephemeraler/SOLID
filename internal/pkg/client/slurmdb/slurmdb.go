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

// GetUser 根据用户名称获取用户信息.
func (c *Client) GetUserByName(ctx context.Context, name string) (model.Users, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("username is required")
	}
	var res model.Users
	tx := c.DB.WithContext(ctx).Model(&model.User{}).
		Where("deleted = 0 AND name = ?", name)
	if err := tx.Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

// GetUsers 获取全部用户信息, 支持分页.
func (c *Client) GetUsersPaged(ctx context.Context, paging bool, page, pageSize int) (model.Users, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}
	base := c.DB.WithContext(ctx).Model(&model.User{}).Where("deleted = 0")

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	q := base
	if paging {
		if page < 1 {
			page = 1
		}
		if pageSize <= 0 {
			pageSize = 20
		}
		offset := (page - 1) * pageSize
		q = q.Offset(offset).Limit(pageSize)
	}

	var res model.Users
	if err := q.Find(&res).Error; err != nil {
		return nil, 0, err
	}
	return res, total, nil
}

// GetAcctsPaged queries acct_table with an optional deleted filter and pagination.
// Returns the paged accounts and total count before paging.
func (c *Client) GetAccounts(ctx context.Context, paging bool, offset, limit int) (model.Accounts, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}

	base := c.DB.WithContext(ctx).Model(&model.Account{}).Where("deleted = 0")
	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var res model.Accounts
	q := base
	if paging == true {
		if limit > 0 {
			q = q.Limit(limit)
		}
		if offset > 0 {
			q = q.Offset(offset)
		}
	}

	if err := q.Find(&res).Error; err != nil {
		return nil, 0, err
	}
	return res, total, nil
}

// GetAcctByName returns a single account by name from acct_table with an optional deleted filter.
func (c *Client) GetAcctByName(ctx context.Context, name string) (*model.Account, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("account name is required")
	}
	var acct model.Account
	tx := c.DB.WithContext(ctx).Model(&model.Account{}).Where("deleted = 0")
	if err := tx.Where("name = ?", name).First(&acct).Error; err != nil {
		return nil, err
	}
	return &acct, nil
}

type AccountNode struct {
	Name         string     `json:"name"`         // 当前账号节点
	Organization string     `json:"organization"` // 单位
	Description  string     `json:"description"`  // 描述
	SubAccounts  []string   `json:"sub_accounts"` // 子账号名称
	SubUsers     []UserNode `json:"sub_users"`    // 子用户节点信息
}

type UserNode struct {
	Name              string   `json:"name"`               // 用户名
	AdminLevel        int      `json:"admin_level"`        // 管理级别
	AvailableAccounts []string `json:"available_accounts"` // 可用账号
}

// GetAccountsTree 获取当前账户 account 的子节点信息.
func (c *Client) GetChildNodesOfAccount(ctx context.Context, account string) (AccountNode, error) {
	tree := AccountNode{
		Name: account,
	}
	if c == nil || c.DB == nil {
		return tree, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return tree, fmt.Errorf("account name is required")
	}

	acct, err := c.GetAcctByName(ctx, account)
	if err != nil {
		return tree, fmt.Errorf("unable to find '%s': %w", account, err)
	}
	tree.Description = acct.Description
	tree.Organization = acct.Organization

	subAcctsName, subUsersName, err := c.GetSubAccountsAndUsers(ctx, account)
	if err != nil {
		return tree, fmt.Errorf("unable to find %s's subaccounts or subusers: %w", account, err)
	}

	for _, name := range subUsersName {
		ps, err := c.GetParentAccountsByUser(ctx, name)
		if err != nil {
			return tree, fmt.Errorf("unable to find user(%s)'s all parents: %w", name, err)
		}
		al, err := c.GetUserAdminLevels(ctx, []string{name})
		if err != nil {
			return tree, fmt.Errorf("unable to find user(%s)'s admin level: %w", name, err)
		}
		tree.SubUsers = append(tree.SubUsers, UserNode{Name: name, AdminLevel: al[name], AvailableAccounts: ps})
	}

	for _, account := range subAcctsName {
		tree.SubAccounts = append(tree.SubAccounts, account)
	}

	return tree, nil
}

type AssociationNode struct {
	Name        string   `json:"name"`         // 账户名称
	Partition   string   `json:"partition"`    // 默认分区
	SubAccounts []string `json:"sub_accounts"` // 子账户
	SubUsers    []AssociationUserNode
}

type AssociationUserNode struct {
	Name       string   `json:"name"` // 用户名
	Partitions []string // 关联分区名称
}

func (c *Client) GetAssociationChildNodesOfAccount(ctx context.Context, account string) (AssociationNode, error) {
	node := AssociationNode{Name: account}
	if c == nil || c.DB == nil {
		return node, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return node, fmt.Errorf("account name is required")
	}

	// Default partition of current account (from assoc account row where user='')
	part, err := c.GetPartitionOfAccount(ctx, account)
	if err != nil {
		return node, fmt.Errorf("unable to find account(%s)'s partition: %w", account, err)
	}
	node.Partition = part

	// Direct sub-accounts and sub-users
	subAccts, subUsers, err := c.GetSubAccountsAndUsers(ctx, account)
	if err != nil {
		return node, fmt.Errorf("unable to find %s's subaccounts or subusers: %w", account, err)
	}
	node.SubAccounts = append(node.SubAccounts, subAccts...)

	// For each sub-user, collect partitions within this account
	node.SubUsers = make([]AssociationUserNode, 0, len(subUsers))
	for _, u := range subUsers {
		parts, err := c.GetPartitionsOfUser(ctx, account, u)
		if err != nil {
			return node, fmt.Errorf("unable to find partitions for user(%s) in account(%s): %w", u, account, err)
		}
		node.SubUsers = append(node.SubUsers, AssociationUserNode{Name: u, Partitions: parts})
	}
	return node, nil
}

// GetPartitionOfAccount 从 assoc_table 中查找某个账户的分区信息.
func (c *Client) GetPartitionOfAccount(ctx context.Context, account string) (string, error) {
	table := fmt.Sprintf("%s_assoc_table", c.ClusterName)
	var partition string
	if err := c.DB.WithContext(ctx).
		Table(table).
		Where("acct = ? AND deleted = 0 AND `user` = ''", account).
		Distinct("`partition`").
		Pluck("`partition`", &partition).Error; err != nil {
		return partition, err
	}
	return partition, nil
}

// GetPartitionsOfUser 在 <cluster_name>_assoc_table 中寻找所有满足 acct = account and user = user 条目的 partition 字段, 并返回.
func (c *Client) GetPartitionsOfUser(ctx context.Context, account, user string) ([]string, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if strings.TrimSpace(user) == "" {
		return nil, fmt.Errorf("username is required")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}

	table := fmt.Sprintf("%s_assoc_table", c.ClusterName)
	var parts []string
	if err := c.DB.WithContext(ctx).
		Table(table).
		Where("acct = ? AND `user` = ? AND deleted = 0", account, user).
		Where("`partition` <> ''").
		Distinct().
		Pluck("`partition`", &parts).Error; err != nil {
		return nil, err
	}
	return parts, nil
}

type AssociationTree struct {
	Name        string // 当前节点账户名称
	Partition   string // 当前节点分区名称
	SubAccounts []string
	SubUsers    []AssociationUser
}

type AssociationUser struct {
	Name      string
	Partition []string
}

func (c *Client) GetAssociationTree(ctx context.Context, account string) (AssociationTree, error) {
	tree := AssociationTree{
		Name: account,
	}
	if c == nil || c.DB == nil {
		return tree, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(account) == "" {
		return tree, fmt.Errorf("account name is required")
	}
	partition, err := c.GetPartitionOfAccount(ctx, account)
	if err != nil {
		return tree, fmt.Errorf("unable to find account(%s)'s partition: %w", account, err)
	}
	tree.Partition = partition

	subAcctsName, subUsersName, err := c.GetSubAccountsAndUsers(ctx, account)
	if err != nil {
		return tree, fmt.Errorf("unable to find %s's subaccounts or subusers: %w", account, err)
	}

	for _, name := range subUsersName {
		parts, err := c.GetPartitionsOfUser(ctx, account, name)
		if err != nil {
			return tree, fmt.Errorf("unable to find <%s, %s>'s partitions: %w", account, name, err)
		}
		tree.SubUsers = append(tree.SubUsers, AssociationUser{Name: name, Partition: parts})
	}

	for _, account := range subAcctsName {
		tree.SubAccounts = append(tree.SubAccounts, account)
	}

	return tree, nil
}

// GetSubAccountsAndUsers 返回子账号及子用户returns direct child accounts (by parent_acct) and users
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

func (c *Client) GetAssociation(ctx context.Context, account, user, partition string) (*model.UserAssociation, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	table := model.AssocTableName(c.ClusterName)
	var row model.UserAssociation
	err := c.DB.WithContext(ctx).
		Table(table).
		Where("deleted = 0 AND acct = ? AND `user` = ? AND `partition` = ?", account, user, partition).
		First(&row).Error
	if err != nil {
		return nil, err
	}
	return &row, nil
}

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

// GetUserAdminLevels returns a map of username -> admin_level for the given usernames
// from user_table, filtering deleted = 0. Unknown users are omitted from the map.
func (c *Client) GetUserAdminLevels(ctx context.Context, usernames []string) (map[string]int, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if len(usernames) == 0 {
		return map[string]int{}, nil
	}
	// Deduplicate to keep query concise
	uniq := make(map[string]struct{}, len(usernames))
	list := make([]string, 0, len(usernames))
	for _, u := range usernames {
		if u == "" {
			continue
		}
		if _, ok := uniq[u]; ok {
			continue
		}
		uniq[u] = struct{}{}
		list = append(list, u)
	}
	if len(list) == 0 {
		return map[string]int{}, nil
	}

	var rows model.Users
	if err := c.DB.WithContext(ctx).
		Model(&model.User{}).
		Where("deleted = 0 AND name IN ?", list).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		out[r.Name] = int(r.AdminLevel)
	}
	return out, nil
}

func (c *Client) GetAccoutingJobs(ctx context.Context, paging bool, page, page_size int64) {}

func (c *Client) GetJobSteps(ctx context.Context, jobid int64) (model.Steps, error) {
	steps := make(model.Steps, 0)
	if c == nil || c.DB == nil {
		return steps, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return steps, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	if jobid <= 0 {
		return steps, fmt.Errorf("invalid jobid")
	}

	jobTable := fmt.Sprintf("%s_job_table", c.ClusterName)
	stepTable := fmt.Sprintf("%s_step_table", c.ClusterName)

	// Join job and step tables by job_db_inx, filter by jobid and deleted=0, order by start/id
	q := c.DB.WithContext(ctx).
		Table(stepTable+" AS s").
		Joins("JOIN "+jobTable+" AS j ON s.job_db_inx = j.job_db_inx").
		Where("j.id_job = ? AND s.deleted = 0", jobid)
	if err := q.Find(&steps).Error; err != nil {
		return nil, err
	}
	return steps, nil
}

// GetJobDetail 返回指定 jobid 的作业详情（来自 <cluster>_job_table），过滤 deleted=0。
// 当 jobid 存在多行（如数组作业），返回最新记录（按 job_db_inx DESC）。
func (c *Client) GetJobDetail(ctx context.Context, jobid int64) (*model.Job, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	if jobid <= 0 {
		return nil, fmt.Errorf("invalid jobid")
	}
	table := fmt.Sprintf("%s_job_table", c.ClusterName)
	var row model.Job
	tx := c.DB.WithContext(ctx).
		Table(table).
		Where("id_job = ? AND deleted = 0", jobid).
		Order("job_db_inx DESC").
		First(&row)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &row, nil
}

// GetJobsDetail 按 jobid 降序分页返回作业详情（deleted=0）。
// page 从 1 开始；page_size > 0。内部按 id_job DESC 排序。
func (c *Client) GetJobsDetail(ctx context.Context, page, pageSize int) (model.Jobs, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}
	if strings.TrimSpace(c.ClusterName) == "" {
		return nil, 0, fmt.Errorf("cluster name is empty in slurmdb Client")
	}
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	table := fmt.Sprintf("%s_job_table", c.ClusterName)
	base := c.DB.WithContext(ctx).Table(table).Where("deleted = 0")

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var rows model.Jobs
	q := base.Order("id_job DESC").Offset(offset).Limit(pageSize)
	if err := q.Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}

// GetQos 根据 ID 获取单个 QoS（deleted=0）。
func (c *Client) GetQos(ctx context.Context, id int) (*model.Qos, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("nil slurmdb Client")
	}

	var row model.Qos
	tx := c.DB.WithContext(ctx).Model(&model.Qos{}).Where("deleted = 0 AND id = ?", id).First(&row)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &row, nil
}

// GetQosAll 获取 QoS 列表，按 id 降序排列；当 paging=true 时应用分页。
func (c *Client) GetQosAll(ctx context.Context, paging bool, page, pageSize int) (model.Qoses, int64, error) {
	if c == nil || c.DB == nil {
		return nil, 0, fmt.Errorf("nil slurmdb Client")
	}
	base := c.DB.WithContext(ctx).Model(&model.Qos{}).Where("deleted = 0")

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	q := base.Order("id DESC")
	if paging {
		if page < 1 {
			page = 1
		}
		if pageSize <= 0 {
			pageSize = 20
		}
		offset := (page - 1) * pageSize
		q = q.Offset(offset).Limit(pageSize)
	}

	var rows model.Qoses
	if err := q.Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}
