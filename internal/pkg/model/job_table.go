package model

// Jobs is a slice of Job rows.
type Jobs []Job

// Job represents a row in <cluster>_job_table.
// Physical table name is cluster-specific; use DB.Table to target it.
type Job struct {
	JobDBInx         uint64 `gorm:"column:job_db_inx;primaryKey" json:"job_db_inx"`
	ModTime          uint64 `gorm:"column:mod_time" json:"mod_time"`
	Deleted          int8   `gorm:"column:deleted" json:"deleted"`
	Account          string `gorm:"column:account" json:"account"`
	AdminComment     string `gorm:"column:admin_comment" json:"admin_comment"`
	ArrayTaskStr     string `gorm:"column:array_task_str" json:"array_task_str"`
	ArrayMaxTasks    uint32 `gorm:"column:array_max_tasks" json:"array_max_tasks"`
	ArrayTaskPending uint32 `gorm:"column:array_task_pending" json:"array_task_pending"`
	Constraints      string `gorm:"column:constraints" json:"constraints"`
	Container        string `gorm:"column:container" json:"container"`
	CPUsReq          uint32 `gorm:"column:cpus_req" json:"cpus_req"`
	DerivedEC        uint32 `gorm:"column:derived_ec" json:"derived_ec"`
	DerivedES        string `gorm:"column:derived_es" json:"derived_es"`
	EnvHashInx       uint64 `gorm:"column:env_hash_inx" json:"env_hash_inx"`
	ExitCode         uint32 `gorm:"column:exit_code" json:"exit_code"`
	Extra            string `gorm:"column:extra" json:"extra"`
	Flags            uint32 `gorm:"column:flags" json:"flags"`
	FailedNode       string `gorm:"column:failed_node" json:"failed_node"`
	JobName          string `gorm:"column:job_name" json:"job_name"`
	IDAssoc          uint32 `gorm:"column:id_assoc" json:"id_assoc"`
	IDArrayJob       uint32 `gorm:"column:id_array_job" json:"id_array_job"`
	IDArrayTask      uint32 `gorm:"column:id_array_task" json:"id_array_task"`
	IDBlock          string `gorm:"column:id_block" json:"id_block"`
	IDJob            uint32 `gorm:"column:id_job" json:"id_job"`
	IDQOS            uint32 `gorm:"column:id_qos" json:"id_qos"`
	IDResv           uint32 `gorm:"column:id_resv" json:"id_resv"`
	IDWckey          uint32 `gorm:"column:id_wckey" json:"id_wckey"`
	IDUser           uint32 `gorm:"column:id_user" json:"id_user"`
	IDGroup          uint32 `gorm:"column:id_group" json:"id_group"`
	HetJobID         uint32 `gorm:"column:het_job_id" json:"het_job_id"`
	HetJobOffset     uint32 `gorm:"column:het_job_offset" json:"het_job_offset"`
	KillRequid       uint32 `gorm:"column:kill_requid" json:"kill_requid"`
	StateReasonPrev  uint32 `gorm:"column:state_reason_prev" json:"state_reason_prev"`
	Licenses         string `gorm:"column:licenses" json:"licenses"`
	MCSLabel         string `gorm:"column:mcs_label" json:"mcs_label"`
	MemReq           uint64 `gorm:"column:mem_req" json:"mem_req"`
	Nodelist         string `gorm:"column:nodelist" json:"nodelist"`
	NodesAlloc       uint32 `gorm:"column:nodes_alloc" json:"nodes_alloc"`
	NodeInx          string `gorm:"column:node_inx" json:"node_inx"`
	Partition        string `gorm:"column:partition" json:"partition"`
	Priority         uint32 `gorm:"column:priority" json:"priority"`
	ScriptHashInx    uint64 `gorm:"column:script_hash_inx" json:"script_hash_inx"`
	State            uint32 `gorm:"column:state" json:"state"`
	TimeLimit        uint32 `gorm:"column:timelimit" json:"timelimit"`
	TimeSubmit       uint64 `gorm:"column:time_submit" json:"time_submit"`
	TimeEligible     uint64 `gorm:"column:time_eligible" json:"time_eligible"`
	TimeStart        uint64 `gorm:"column:time_start" json:"time_start"`
	TimeEnd          uint64 `gorm:"column:time_end" json:"time_end"`
	TimeSuspended    uint64 `gorm:"column:time_suspended" json:"time_suspended"`
	GresUsed         string `gorm:"column:gres_used" json:"gres_used"`
	Wckey            string `gorm:"column:wckey" json:"wckey"`
	WorkDir          string `gorm:"column:work_dir" json:"work_dir"`
	StdErr           string `gorm:"column:std_err" json:"std_err"`
	StdIn            string `gorm:"column:std_in" json:"std_in"`
	StdOut           string `gorm:"column:std_out" json:"std_out"`
	SubmitLine       string `gorm:"column:submit_line" json:"submit_line"`
	SystemComment    string `gorm:"column:system_comment" json:"system_comment"`
	TresAlloc        string `gorm:"column:tres_alloc" json:"tres_alloc"`
	TresReq          string `gorm:"column:tres_req" json:"tres_req"`
}
