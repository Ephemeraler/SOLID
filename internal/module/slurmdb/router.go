package slurmdb

import (
	slurmdbc "solid/internal/pkg/client/slurmdb"

	"github.com/gin-gonic/gin"
)

type Router struct {
	client *slurmdbc.Client
}

func (rt Router) Register(r *gin.Engine) {
	v1 := r.Group("/api/v1/slurm/accounting")
	{
		v1.GET("/qos", HandlerGetQoS)                                    // GET /api/v1/slurm/accountting/qos
		v1.GET("/qos/all", HandlerGetQoSAll)                             // GET /api/v1/slurm/accountting/qos/all
		v1.GET("/accounts", HandlerGetAccounts)                          // GET /api/v1/slurm/accounting/acounts
		v1.GET("/accounts/tree", HandlerGetAccountsTree)                 // GET /api/v1/slurm/accounting/acount
		v1.GET("/associations/tree", HandlerGetAssociationsTree)         // GET /api/v1/slurm/accounting/tree/association
		v1.GET("/associations/detail", HandlerGetTreeAssociationsDetail) // GET /api/v1/slurm/accounting/tree/association/detail
		v1.GET("/job/all", HandlerGetAccountingJobs)                     // GET /api/v1/accounting/job/all
		v1.GET("/job/steps", HandlerGetAccountingJobsSteps)              // GET /api/v1/accounting/job/steps?jobid=xxx
		v1.GET("/job/detail", HandlerGetAccountingJobDetail)             // GET /api/v1/accouting/job?jobid=xxx
	}
}
