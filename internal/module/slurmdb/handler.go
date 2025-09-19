package slurmdb

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	slurmdbc "solid/internal/pkg/client/slurmdb"
	"solid/internal/pkg/common/response"
	"solid/internal/pkg/model"
)

// HandlerGetUserByName 获取指定用户名的信息。
//
// @Summary 获取用户信息
// @Description 通过路径参数 name 查询 user_table 中 deleted=0 的用户信息
// @Tags slurm-accounting, user
// @Produce json
// @Param name path string true "用户名"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accountting/user/:name [get]
func HandlerGetUserByName(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}
	name := c.Param("name")
	if strings.TrimSpace(name) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing user name"})
		return
	}
	users, err := client.GetUserByName(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	if len(users) == 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "user not found or deleted"})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: users[0]})
}

// HandlerGetUserAll 获取用户列表（分页）。
//
// @Summary 获取用户列表
// @Description 从 user_table 查询 deleted=0 的用户；当 paging=true 时按 page/page_size 分页返回，当 paging=false 时返回全部
// @Tags slurm-accounting, user
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页码，从 1 开始（仅当 paging=true 生效）" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100（仅当 paging=true 生效）" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accountting/user/all [get]
func HandlerGetUserAll(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	// Parse paging flag (default true)
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
		rows, total, err := client.GetUsersPaged(c.Request.Context(), true, pq.Page, pq.PageSize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
			return
		}
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
		c.JSON(http.StatusOK, response.Response{Count: int(total), Previous: prevURL, Next: nextURL, Results: rows})
		return
	}

	rows, total, err := client.GetUsersPaged(c.Request.Context(), false, 0, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Count: int(total), Results: rows})
}

// HandlerGetQoS 获取指定的 QoS 信息。
//
// @Summary 获取指定的 QoS
// @Description 通过 id 查询 qos_table 中的 QoS（deleted=0）
// @Tags slurm-accounting, qos
// @Produce json
// @Param id query int true "QoS ID"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/qos [get]
func HandlerGetQoS(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}
	idStr := c.Query("id")
	if strings.TrimSpace(idStr) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing id parameter"})
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid id parameter"})
		return
	}
	row, err := client.GetQos(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "qos not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerGetQoSAll 获取 QoS 列表（分页）。
//
// @Summary 获取 QoS 列表
// @Description 从 qos_table 查询 deleted=0 的 QoS，按 id 降序排序并分页返回
// @Tags slurm-accounting, qos
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页码，从 1 开始（仅当 paging=true 生效）" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100（仅当 paging=true 生效）" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/qos/all [get]
func HandlerGetQoSAll(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	// Parse paging flag (default true)
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
		rows, total, err := client.GetQosAll(c.Request.Context(), true, pq.Page, pq.PageSize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
			return
		}
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
		c.JSON(http.StatusOK, response.Response{Count: int(total), Previous: prevURL, Next: nextURL, Results: rows})
		return
	}

	// Not paged: return all QoS
	rows, total, err := client.GetQosAll(c.Request.Context(), false, 0, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Count: int(total), Results: rows})
}

// HandlerGetAccountByName 获取指定账户信息。
//
// @Summary 获取账户信息
// @Description 通过路径参数 name 获取 acct_table 中 deleted=0 的账户信息
// @Tags slurm-accounting, account
// @Produce json
// @Param name path string true "账户名称"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/account/:name [get]
func HandlerGetAccountByName(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}
	name := c.Param("name")
	if strings.TrimSpace(name) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing account name"})
		return
	}
	acct, err := client.GetAcctByName(c.Request.Context(), name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "account not found or deleted"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: acct})
}

// HandlerGetAccountAll 获取账户列表（分页）。
//
// @Summary 获取账户列表
// @Description 从 acct_table 查询 deleted=0 的账户；当 paging=true 时按 page/page_size 分页返回，当 paging=false 时返回全部
// @Tags slurm-accounting, account
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页码，从 1 开始（仅当 paging=true 生效）" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100（仅当 paging=true 生效）" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/account/all [get]
func HandlerGetAccountAll(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	// Parse paging flag (default true)
	var pagingFlag struct {
		Paging *bool `form:"paging"`
	}
	_ = c.ShouldBindQuery(&pagingFlag)
	paging := true
	if pagingFlag.Paging != nil {
		paging = *pagingFlag.Paging
	}

	if paging {
		// Validate page/page_size
		var pq model.PagingQuery
		_ = c.ShouldBindQuery(&pq)
		pq.SetDefaults(1, 20, 100)
		if err := pq.Validate(); err != nil {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
			return
		}

		accts, total, err := client.GetAccounts(c.Request.Context(), paging, pq.Offset(), pq.Limit())
		if err != nil {
			c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
			return
		}
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
		totalInt := int(total)
		c.JSON(http.StatusOK, response.Response{
			Count:    totalInt,
			Previous: prevURL,
			Next:     nextURL,
			Results:  accts,
		})
		return
	}

	// Not paged: return all accounts (deleted=0)
	accts, total, err := client.GetAccounts(c.Request.Context(), false, 0, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, response.Response{Count: int(total), Results: accts})
}

// HandlerChildNodesOfAccount 返回指定账户的子账户树信息。
//
// @Summary 获取子账户树
// @Description 根据 account 查询子账户树, 获取其直接子用户节点与子账户节点
// @Tags slurm-accounting, account
// @Produce json
// @Param name path string true "账户名称"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accouting/account/:name/childnodes
func HandlerChildNodesOfAccount(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	account := c.Param("name")
	if account == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing account parameter"})
		return
	}

	tree, err := client.GetChildNodesOfAccount(c.Request.Context(), account)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "account not found or deleted"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: tree})
}

// HandlerGetAssociationChildNodesOfAccount 获取某账户的关联子节点信息。
//
// @Summary 获取账户关联子节点
// @Description 根据 account 获取默认分区、子账户列表以及子用户关联的分区列表
// @Tags 用户管理
// @Produce json
// @Param account path string true "账户名称"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/association/:account/childnodes
func HandlerGetAssociationChildNodesOfAccount(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	account := c.Param("account")
	if strings.TrimSpace(account) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing account parameter"})
		return
	}

	node, err := client.GetAssociationChildNodesOfAccount(c.Request.Context(), account)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "account not found or deleted"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: node})
}

type AssociationDetail struct {
	IDAssoc        uint32 `json:"id_assoc"`          // 关联 ID
	ClusterName    string `json:"cluster_name"`      // 集群名称
	Acct           string `json:"acct"`              // 账户名
	Partition      string `json:"partition"`         // 分区名
	Shares         int32  `json:"shares"`            // 公平份额权重
	MaxJobs        int32  `json:"max_jobs"`          // 单账户运行作业上限
	MaxSubmitJobs  int32  `json:"max_submit_jobs"`   // 单账户提交作业上限
	MaxWallPJ      int32  `json:"max_wall_pj"`       // 单账户作业最大运行时间
	GrpTres        string `json:"grp_tres"`          // 组级总TRES资源限制
	GrpWall        int32  `json:"grp_wall"`          // 组级总运行时间限制
	GrpTresMins    string `json:"grp_tres_mins"`     // 组级TRES时间限制
	GrpJobs        int32  `json:"grp_jobs"`          // 组级运行作业总数上限
	GrpSubmitJobs  int32  `json:"grp_submit_jobs"`   // 组级提交作业总数上限
	Priority       uint32 `json:"priority"`          // 账户调度优先级
	MinPrioThresh  int32  `json:"min_prio_thresh"`   // 优先级阈值
	MaxJobsAccrue  int32  `json:"max_jobs_accrue"`   // 累计优先级作业上限
	GrpJobsAccrue  int32  `json:"grp_jobs_accrue"`   // 组级累计优先级作业上限
	MaxTresPJ      string `json:"max_tres_pj"`       // 单作业TRES上限
	MaxTresPN      string `json:"max_tres_pn"`       // 单节点TRES上限
	MaxTresMinsPJ  string `json:"max_tres_mins_pj"`  // 单作业TRES时间上限
	MaxTresRunMins string `json:"max_tres_run_mins"` // 单作业运行中TRES时间上限
	GrpTresRunMins string `json:"grp_tres_run_mins"` // 组级运行中TRES时间上限
	DefQosID       string `json:"def_qos_id"`        // 默认 QoS ID
	QoS            string `json:"qos"`               // Qos 列表
}

// HandlerGetTreeAssociationDetail 获取某个关联信息的详情
//
// @Summary 获取某个关联信息的详情
// @Description 获取某个关联信息的详情
// @Tags 用户管理
// @Produce json
// @Param account query string true "账户名称"
// @Param user query string false "用户名称"
// @Param partition query string false "分区名称"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/associations/detail [get]
func HandlerGetTreeAssociationsDetail(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	acct := c.Query("account")
	if acct == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing account parameter"})
		return
	}

	userPtr := c.DefaultQuery("user", "")
	partPtr := c.DefaultQuery("partition", "")

	row, err := client.GetAssociation(c.Request.Context(), acct, userPtr, partPtr)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "association not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerGetAccountingJobs 获取作业列表（分页）。
//
// @Summary 获取作业列表
// @Description 从 <cluster>_job_table 查询 deleted=0 的作业；按 jobid 降序排序并分页返回
// @Tags slurm-accounting, job
// @Produce json
// @Param page query int false "页码，从 1 开始" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/jobs [get]
func HandlerGetAccountingJobs(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	var pq model.PagingQuery
	_ = c.ShouldBindQuery(&pq)
	pq.SetDefaults(1, 20, 100)
	if err := pq.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
		return
	}

	rows, total, err := client.GetJobsDetail(c.Request.Context(), pq.Page, pq.PageSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
	c.JSON(http.StatusOK, response.Response{
		Count:    int(total),
		Previous: prevURL,
		Next:     nextURL,
		Results:  rows,
	})
}

// HandlerGetAccountingJobsSteps
// @Summary 获取 Slurm 账户中作业列表
// @Description 获取 Slurm 账户中作业列表
// @Tags slurm-accounting, account
// @Produce json
// @Param jobid query int true "作业ID"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/job/steps [get]
func HandlerGetAccountingJobsSteps(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	jobidStr := c.Query("jobid")
	if strings.TrimSpace(jobidStr) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing jobid parameter"})
		return
	}
	jobid, err := strconv.ParseInt(jobidStr, 10, 64)
	if err != nil || jobid <= 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid jobid parameter"})
		return
	}

	steps, err := client.GetJobSteps(c.Request.Context(), jobid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Count: len(steps), Results: steps})
}

// HandlerGetAccountingJobDetail 获取指定作业的详细信息。
//
// @Summary 获取作业详情
// @Description 通过 jobid 查询 <cluster>_job_table 中对应作业（deleted=0），返回作业详情
// @Tags slurm-accounting, job
// @Produce json
// @Param jobid query int true "作业ID"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/job [get]
func HandlerGetJobFromAccounting(c *gin.Context) {
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}
	jobidStr := c.Query("jobid")
	if strings.TrimSpace(jobidStr) == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing jobid parameter"})
		return
	}
	jobid, err := strconv.ParseInt(jobidStr, 10, 64)
	if err != nil || jobid <= 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid jobid parameter"})
		return
	}
	row, err := client.GetJobDetail(c.Request.Context(), jobid)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, response.Response{Detail: "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}
