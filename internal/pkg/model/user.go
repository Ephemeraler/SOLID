package model

type Users []User

// User represents a row in user_table.
type User struct {
    CreationTime uint64 `gorm:"column:creation_time" json:"creation_time"`
    ModTime      uint64 `gorm:"column:mod_time" json:"mod_time"`
    Deleted      int8   `gorm:"column:deleted" json:"deleted"`
    Name         string `gorm:"column:name;primaryKey" json:"name"`
    AdminLevel   int16  `gorm:"column:admin_level" json:"admin_level"`
    // LDAPAttrs holds attributes fetched from LDAP for this user.
    // It is ignored by GORM when reading from the DB.
    LDAPAttrs    map[string][]string `json:"ldap_attrs" gorm:"-"`
}

func (User) TableName() string { return "user_table" }
