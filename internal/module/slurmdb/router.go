package slurmdb

import (
	"github.com/gin-gonic/gin"
)

type Router struct{}

func (rt Router) Register(r *gin.Engine) {
	v1 := r.Group("/api/v1/slurm/accounting")
	{
		v1.GET("/user/:name", HandlerGetUserByName)                                          // GET /api/v1/slurm/accountting/user/:name
		v1.GET("/user/all", HandlerGetUserAll)                                               // GET /api/v1/slurm/accountting/user/all
		v1.GET("/qos", HandlerGetQoS)                                                        // GET /api/v1/slurm/accountting/qos
		v1.GET("/qos/all", HandlerGetQoSAll)                                                 // GET /api/v1/slurm/accountting/qos/all?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/account/:name", HandlerGetAccountByName)                                    // GET /api/v1/slurm/accountting/account/:name
		v1.GET("/account/all", HandlerGetAccountAll)                                         // GET /api/v1/slurm/accountting/account/all\
		v1.GET("/account/:name/childnodes", HandlerChildNodesOfAccount)                      // GET /api/v1/slurm/accouting/account/:name/childnodes
		v1.GET("/association/:account/childnodes", HandlerGetAssociationChildNodesOfAccount) // GET /api/v1/slurm/accouting/associations/:account/childnodes
		v1.GET("/association/detail", HandlerGetTreeAssociationsDetail)                      // GET /api/v1/slurm/accounting/tree/association/detail
		v1.GET("/job/all", HandlerGetAccountingJobs)                                         // GET /api/v1/slurm/accounting/job/all
		v1.GET("/job/steps", HandlerGetAccountingJobsSteps)                                  // GET /api/v1/slurm/accounting/job/steps?jobid=xxx
		v1.GET("/job", HandlerGetJobFromAccounting)                                          // GET /api/v1/slurm/accouting/job?jobid=xxx
	}
}
