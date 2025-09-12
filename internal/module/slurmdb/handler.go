package slurmdb

import (
    "net/http"

    "github.com/gin-gonic/gin"

    slurmdbc "solid/client/slurmdb"
    "solid/internal/pkg/common/response"
    "solid/internal/pkg/model"
)

// @Summary 获取 Slurm 账户系统中用户列表（分页）
// @Description 获取 Slurm 账户系统中用户列表，按 deleted=1 过滤，并支持分页参数 page、page_size
// @Tags slurm-accounting, user
// @Produce json
// @Param page query int false "页码，从 1 开始"
// @Param page_size query int false "每页数量，1-1000"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/users [get]
func HandlerListUsers(c *gin.Context) {
	// Use package-level default client from slurmdb package.
	client := slurmdbc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "slurmdb client not initialized"})
		return
	}

	// Parse and validate paging
	var pq model.PagingQuery
	_ = c.ShouldBindQuery(&pq)
	pq.SetDefaults(1, 20, 100)
	if err := pq.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
		return
	}

	// deleted = 1
	deleted := 0
	users, total, err := client.GetUsersPaged(c.Request.Context(), &deleted, nil, pq.Offset(), pq.Limit())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
	totalInt := int(total)
	c.JSON(http.StatusOK, response.Response{
		Count:    &totalInt,
		Previous: prevURL,
		Next:     nextURL,
		Results:  users,
	})
}

// HandlerListAccts 获取 Slurm 账户系统中的账户列表（分页）。
//
// 业务说明：
//  - 仅返回未删除的账户（deleted = 0）。
//  - 支持分页参数 page 与 page_size，默认 page=1、page_size=20，最大 page_size=100。
//
// @Summary 获取 Slurm 账户列表（分页）
// @Description 获取 deleted=0 的账户，支持分页参数 page、page_size
// @Tags slurm-accounting, account
// @Produce json
// @Param page query int false "页码，从 1 开始"
// @Param page_size query int false "每页数量，1-1000"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/slurm/accounting/acounts [get]
func HandlerListAccts(c *gin.Context) {
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

    deleted := 0
    accts, total, err := client.GetAcctsPaged(c.Request.Context(), &deleted, pq.Offset(), pq.Limit())
    if err != nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
        return
    }

    prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
    totalInt := int(total)
    c.JSON(http.StatusOK, response.Response{
        Count:    &totalInt,
        Previous: prevURL,
        Next:     nextURL,
        Results:  accts,
    })
}
