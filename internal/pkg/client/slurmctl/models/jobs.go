package models

type Jobs []Job

type Job struct {
	Jobid     string `json:"jobid"`     // 作业ID
	State     string `json:"state"`     // 状态
	User      string `json:"user"`      // 用户
	Account   string `json:"account"`   // 账户
	CPUs      string `json:"cpus"`      // 资源个数
	Nodelist  string `json:"nodelist"`  // 节点列表
	Partition string `json:"partition"` // 分区
	QoS       string `json:"qos"`       // QoS
	Reason    string `json:"reason"`    // 原因
}

type Steps []Step

type Step struct {
	ID    string `json:"id"`
	Name  string `json:"Name"`
	State string `json:"state"`
}
