package model

// Accounts is a slice of Account.
type Accounts []Account

// Account represents a row in acct_table.
//
// Columns reference:
//  - creation_time: bigint unsigned, not null
//  - mod_time:      bigint unsigned, not null
//  - deleted:       tinyint, default 0
//  - flags:         int unsigned, default 0
//  - name:          tinytext, primary key
//  - description:   text, not null
//  - organization:  text, not null
type Account struct {
    CreationTime uint64 `gorm:"column:creation_time" json:"creation_time"`
    ModTime      uint64 `gorm:"column:mod_time" json:"mod_time"`
    Deleted      int8   `gorm:"column:deleted" json:"deleted"`
    Flags        uint32 `gorm:"column:flags" json:"flags"`
    Name         string `gorm:"column:name;primaryKey" json:"name"`
    Description  string `gorm:"column:description" json:"description"`
    Organization string `gorm:"column:organization" json:"organization"`
}

func (Account) TableName() string { return "acct_table" }

