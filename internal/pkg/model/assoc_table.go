package model

import "fmt"

// UserAssociation represents a row from the cluster-specific <ClusterName>_assoc_table
// for a user. It captures common relationship columns.
type UserAssociation struct {
	CreationTime   uint64 `gorm:"column:creation_time" json:"creation_time"`
	ModTime        uint64 `gorm:"column:mod_time" json:"mod_time"`
	Deleted        int8   `gorm:"column:deleted" json:"deleted"`
	Comment        string `gorm:"column:comment" json:"comment"`
	Flags          uint32 `gorm:"column:flags" json:"flags"`
	IsDef          int8   `gorm:"column:is_def" json:"is_def"`
	IDAssoc        uint32 `gorm:"column:id_assoc" json:"id_assoc"`
	User           string `gorm:"column:user" json:"user"`
	Acct           string `gorm:"column:acct" json:"acct"`
	Partition      string `gorm:"column:partition" json:"partition"`
	ParentAcct     string `gorm:"column:parent_acct" json:"parent_acct"`
	IDParent       uint32 `gorm:"column:id_parent" json:"id_parent"`
	Lft            int32  `gorm:"column:lft" json:"lft"`
	Rgt            int32  `gorm:"column:rgt" json:"rgt"`
	Shares         int32  `gorm:"column:shares" json:"shares"`
	MaxJobs        int32  `gorm:"column:max_jobs" json:"max_jobs"`
	MaxJobsAccrue  int32  `gorm:"column:max_jobs_accrue" json:"max_jobs_accrue"`
	MinPrioThresh  int32  `gorm:"column:min_prio_thresh" json:"min_prio_thresh"`
	MaxSubmitJobs  int32  `gorm:"column:max_submit_jobs" json:"max_submit_jobs"`
	MaxTresPJ      string `gorm:"column:max_tres_pj" json:"max_tres_pj"`
	MaxTresPN      string `gorm:"column:max_tres_pn" json:"max_tres_pn"`
	MaxTresMinsPJ  string `gorm:"column:max_tres_mins_pj" json:"max_tres_mins_pj"`
	MaxTresRunMins string `gorm:"column:max_tres_run_mins" json:"max_tres_run_mins"`
	MaxWallPJ      int32  `gorm:"column:max_wall_pj" json:"max_wall_pj"`
	GrpJobs        int32  `gorm:"column:grp_jobs" json:"grp_jobs"`
	GrpJobsAccrue  int32  `gorm:"column:grp_jobs_accrue" json:"grp_jobs_accrue"`
	GrpSubmitJobs  int32  `gorm:"column:grp_submit_jobs" json:"grp_submit_jobs"`
	GrpTres        string `gorm:"column:grp_tres" json:"grp_tres"`
	GrpTresMins    string `gorm:"column:grp_tres_mins" json:"grp_tres_mins"`
	GrpTresRunMins string `gorm:"column:grp_tres_run_mins" json:"grp_tres_run_mins"`
	GrpWall        int32  `gorm:"column:grp_wall" json:"grp_wall"`
	Priority       uint32 `gorm:"column:priority" json:"priority"`
	DefQosID       int32  `gorm:"column:def_qos_id" json:"def_qos_id"`
	QOS            string `gorm:"column:qos" json:"qos"`
}

// AssocTableName returns the physical table name for the given cluster name.
func AssocTableName(cluster string) string { return fmt.Sprintf("%s_assoc_table", cluster) }
