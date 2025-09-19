package slurmctld

import (
	"github.com/gin-gonic/gin"
)

type Router struct{}

func (rt Router) Register(r *gin.Engine) {
	v1 := r.Group("/api/v1/slurm/scheduling")
	{
		v1.GET("/node/all", HandlerGetAllNodes)           // GET /api/v1/slurm/scheduling/node/all?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/job/all", HandlerGetAllJobs)             // GET /api/v1/slurm/scheduling/job/all?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/job", HandlerGetJob)                     // ✅GET /api/v1/slurm/scheduling/job?jobid=xxx
		v1.GET("/job/steps", HandlerGetStepsOfJob)        // GET /api/v1/slurm/scheduling/job/steps?jobid=xxx
		v1.GET("/partition/all", HandlerGetAllPartitions) // ✅GET /api/v1/slurm/scheduling/partition/all?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/partition", HandlerGetPartition)         // ✅GET // GET /api/v1/slurm/scheduling/partition?name=xxx
	}
}
