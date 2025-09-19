package model

// Steps is a slice of Step rows.
type Steps []Step

// Step represents a row in <cluster>_step_table.
// Note: physical table name is cluster-specific ("<cluster>_step_table").
// Use DB.Table to target it, while this struct defines the columns' mapping.
type Step struct {
	JobDBInx              uint64  `gorm:"column:job_db_inx;primaryKey" json:"job_db_inx"`
	Deleted               int8    `gorm:"column:deleted" json:"deleted"`
	ExitCode              int32   `gorm:"column:exit_code" json:"exit_code"`
	IDStep                int32   `gorm:"column:id_step;primaryKey" json:"id_step"`
	StepHetComp           uint32  `gorm:"column:step_het_comp;primaryKey" json:"step_het_comp"`
	KillRequid            int32   `gorm:"column:kill_requid" json:"kill_requid"`
	Nodelist              string  `gorm:"column:nodelist" json:"nodelist"`
	NodesAlloc            uint32  `gorm:"column:nodes_alloc" json:"nodes_alloc"`
	NodeInx               string  `gorm:"column:node_inx" json:"node_inx"`
	State                 uint64  `gorm:"column:state" json:"state"`
	StepName              string  `gorm:"column:step_name" json:"step_name"`
	TaskCnt               uint32  `gorm:"column:task_cnt" json:"task_cnt"`
	TaskDist              int16   `gorm:"column:task_dist" json:"task_dist"`
	TimeStart             uint64  `gorm:"column:time_start" json:"time_start"`
	TimeEnd               uint64  `gorm:"column:time_end" json:"time_end"`
	TimeSuspended         uint64  `gorm:"column:time_suspended" json:"time_suspended"`
	UserSec               uint32  `gorm:"column:user_sec" json:"user_sec"`
	UserUsec              uint32  `gorm:"column:user_usec" json:"user_usec"`
	SysSec                uint32  `gorm:"column:sys_sec" json:"sys_sec"`
	SysUsec               uint32  `gorm:"column:sys_usec" json:"sys_usec"`
	ActCPUFreq            float64 `gorm:"column:act_cpufreq" json:"act_cpufreq"`
	ConsumedEnergy        uint64  `gorm:"column:consumed_energy" json:"consumed_energy"`
	ReqCPUFreqMin         uint32  `gorm:"column:req_cpufreq_min" json:"req_cpufreq_min"`
	ReqCPUFreq            uint32  `gorm:"column:req_cpufreq" json:"req_cpufreq"`
	ReqCPUFreqGov         uint32  `gorm:"column:req_cpufreq_gov" json:"req_cpufreq_gov"`
	TRESAlloc             string  `gorm:"column:tres_alloc" json:"tres_alloc"`
	TRESUsageInAve        string  `gorm:"column:tres_usage_in_ave" json:"tres_usage_in_ave"`
	TRESUsageInMax        string  `gorm:"column:tres_usage_in_max" json:"tres_usage_in_max"`
	TRESUsageInMaxTaskID  string  `gorm:"column:tres_usage_in_max_taskid" json:"tres_usage_in_max_taskid"`
	TRESUsageInMaxNodeID  string  `gorm:"column:tres_usage_in_max_nodeid" json:"tres_usage_in_max_nodeid"`
	TRESUsageInMin        string  `gorm:"column:tres_usage_in_min" json:"tres_usage_in_min"`
	TRESUsageInMinTaskID  string  `gorm:"column:tres_usage_in_min_taskid" json:"tres_usage_in_min_taskid"`
	TRESUsageInMinNodeID  string  `gorm:"column:tres_usage_in_min_nodeid" json:"tres_usage_in_min_nodeid"`
	TRESUsageInTot        string  `gorm:"column:tres_usage_in_tot" json:"tres_usage_in_tot"`
	TRESUsageOutAve       string  `gorm:"column:tres_usage_out_ave" json:"tres_usage_out_ave"`
	TRESUsageOutMax       string  `gorm:"column:tres_usage_out_max" json:"tres_usage_out_max"`
	TRESUsageOutMaxTaskID string  `gorm:"column:tres_usage_out_max_taskid" json:"tres_usage_out_max_taskid"`
	TRESUsageOutMaxNodeID string  `gorm:"column:tres_usage_out_max_nodeid" json:"tres_usage_out_max_nodeid"`
	TRESUsageOutMin       string  `gorm:"column:tres_usage_out_min" json:"tres_usage_out_min"`
	TRESUsageOutMinTaskID string  `gorm:"column:tres_usage_out_min_taskid" json:"tres_usage_out_min_taskid"`
	TRESUsageOutMinNodeID string  `gorm:"column:tres_usage_out_min_nodeid" json:"tres_usage_out_min_nodeid"`
	TRESUsageOutTot       string  `gorm:"column:tres_usage_out_tot" json:"tres_usage_out_tot"`
}

// TableName intentionally omitted due to cluster-specific physical table names.
