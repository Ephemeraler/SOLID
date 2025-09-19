package slurmctld

import (
	"net/http"
	"solid/internal/pkg/client/slurmctl"
	slurmctlmodels "solid/internal/pkg/client/slurmctl/models"
	"solid/internal/pkg/common/response"
	"solid/internal/pkg/model"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

// @Param partiton query string false "分区, 多分区采用逗号分割" example("p1,p2")
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页号(从1开始)" example("1") default(1) minimum(1)
// @Param page_size query int false "每页数量" example("20") default(20) minimum(1)
// @Router /api/v1/slurm/scheduling/node/all?partiton=xxx&paging=xxx&page=xxx&page_size=xxx [get]
func HandlerGetAllNodes(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	// 可选分区过滤（多分区逗号分隔）
	condPartition := strings.TrimSpace(c.Query("partiton"))

	// 调用 client.GetNodes()
	nodesMap, err := client.GetNodes(c.Request.Context(), condPartition)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	// 将 map 转为切片并按名称排序，便于稳定分页
	list := make([]*slurmctlmodels.Node, 0, len(nodesMap))
	// 收集并排序键，保证稳定性
	keys := make([]string, 0, len(nodesMap))
	for k := range nodesMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Append in order
	for _, k := range keys {
		n := nodesMap[k]
		list = append(list, n)
	}

	total := len(list)

	// 处理分页开关（默认 true）
	var pagingFlag struct {
		Paging *bool `form:"paging"`
	}
	_ = c.ShouldBindQuery(&pagingFlag)
	paging := true
	if pagingFlag.Paging != nil {
		paging = *pagingFlag.Paging
	}

	if paging {
		var pq model.PagingQuery
		_ = c.ShouldBindQuery(&pq)
		pq.SetDefaults(1, 20, 100)
		if err := pq.Validate(); err != nil {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
			return
		}

		start := pq.Offset()
		if start > total {
			start = total
		}
		end := start + pq.Limit()
		if end > total {
			end = total
		}
		pageSlice := list[start:end]
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
		c.JSON(http.StatusOK, response.Response{Count: total, Previous: prevURL, Next: nextURL, Results: pageSlice})
		return
	}

	c.JSON(http.StatusOK, response.Response{Count: total, Results: list})
}

// HandlerGetAllJobs 获取作业列表（可分页）。
//
// @Summary 获取作业列表
// @Description 通过 squeue 获取全部作业信息；支持分页返回
// @Tags slurm-scheduling, job
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页号(从1开始)" example("1") default(1) minimum(1)
// @Param page_size query int false "每页数量" example("20") default(20) minimum(1)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/scheduling/job/all?paging=xxx&page=xxx&page_size=xxx [get]
func HandlerGetAllJobs(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	jobs, err := client.GetJobs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	total := len(jobs)

	// 分页开关，默认 true
	var pagingFlag struct {
		Paging *bool `form:"paging"`
	}
	_ = c.ShouldBindQuery(&pagingFlag)
	paging := true
	if pagingFlag.Paging != nil {
		paging = *pagingFlag.Paging
	}

	if paging {
		var pq model.PagingQuery
		_ = c.ShouldBindQuery(&pq)
		pq.SetDefaults(1, 20, 100)
		if err := pq.Validate(); err != nil {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
			return
		}
		start := pq.Offset()
		if start > total {
			start = total
		}
		end := start + pq.Limit()
		if end > total {
			end = total
		}
		pageSlice := jobs[start:end]
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
		c.JSON(http.StatusOK, response.Response{Count: total, Previous: prevURL, Next: nextURL, Results: pageSlice})
		return
	}

	c.JSON(http.StatusOK, response.Response{Count: total, Results: jobs})
}

// HandlerGetJob 获取指定 Job 的详情。
//
// @Summary 获取 Job 详情
// @Description 通过 jobid 调用 scontrol show job，返回作业关键信息
// @Tags slurm-scheduling, job
// @Produce json
// @Param jobid query string true "Job ID"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/scheduling/job?jobid=xxx [get]
func HandlerGetJob(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	jobid := strings.TrimSpace(c.Query("jobid"))
	if jobid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing jobid parameter"})
		return
	}

	job, err := client.GetJob(c.Request.Context(), jobid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, response.Response{Results: job})
}

// HandlerGetStepsOfJob 获取指定 Job 的步骤列表。
//
// @Summary 获取 Job 的步骤列表
// @Description 通过 jobid 调用调度端查询步骤信息，返回 stepid/stepname/stepstate
// @Tags slurm-scheduling, job
// @Produce json
// @Param jobid query string true "Job ID"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/scheduling/job/steps?jobid=xxx [get]
func HandlerGetStepsOfJob(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	jobid := strings.TrimSpace(c.Query("jobid"))
	if jobid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing jobid parameter"})
		return
	}

	steps, err := client.GetStepsOfJob(c.Request.Context(), jobid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, response.Response{Count: len(steps), Results: steps})
}

// HandlerGetAllPartitions 获取所有分区详情（可分页）。
//
// @Summary 获取分区列表
// @Description 通过 scontrol show partition 获取所有分区信息；支持分页返回
// @Tags slurm-scheduling, partition
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页号(从1开始)" example("1") default(1) minimum(1)
// @Param page_size query int false "每页数量" example("20") default(20) minimum(1)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/scheduling/partition/all?paging=xxx&page=xxx&page_size=xxx [get]
func HandlerGetAllPartitions(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	parts, err := client.GetPartitions(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	total := len(parts)

	// 分页开关，默认 true
	var pagingFlag struct {
		Paging *bool `form:"paging"`
	}
	_ = c.ShouldBindQuery(&pagingFlag)
	paging := true
	if pagingFlag.Paging != nil {
		paging = *pagingFlag.Paging
	}

	if paging {
		var pq model.PagingQuery
		_ = c.ShouldBindQuery(&pq)
		pq.SetDefaults(1, 20, 100)
		if err := pq.Validate(); err != nil {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
			return
		}
		start := pq.Offset()
		if start > total {
			start = total
		}
		end := start + pq.Limit()
		if end > total {
			end = total
		}
		pageSlice := parts[start:end]
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
		c.JSON(http.StatusOK, response.Response{Count: total, Previous: prevURL, Next: nextURL, Results: pageSlice})
		return
	}

	c.JSON(http.StatusOK, response.Response{Count: total, Results: parts})
}

// HandlerGetPartition 获取指定名称的分区详情。
//
// @Summary 获取分区详情
// @Description 通过 name 调用 scontrol show partition <name>，返回该分区的字段信息
// @Tags slurm-scheduling, partition
// @Produce json
// @Param name query string true "分区名称"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/scheduling/partition?name=xxx [get]
func HandlerGetPartition(c *gin.Context) {
	client := slurmctl.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmclt client not initialized"})
		return
	}

	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing name parameter"})
		return
	}

	part, err := client.GetPartition(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, response.Response{Results: part})
}
