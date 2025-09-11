package user

import (
	"net/http"

	"github.com/gin-gonic/gin"

	ldapc "solid/client/ldap"
	slurmdbc "solid/client/slurmdb"
	"solid/internal/pkg/common/response"
	"solid/internal/pkg/model"
)

// HandlerListUsers 列出用户（分页），并附带 LDAP 属性。
//
// 流程：
//  1. 从查询参数读取 page, page_size，进行校验（默认 page=1, page_size=20，上限 100）。
//  2. 通过 SlurmDB 客户端按分页获取用户基础信息（仅用到用户名）。
//  3. 将用户名列表传给 LDAP 客户端批量查询，获取所有属性。
//  4. 将 LDAP 属性合并进入用户结果并返回分页响应。
//
// @Summary 列出 (Slurm And LDAP)用户（含 LDAP 属性）
// @Description 从 SlurmDB 获取用户列表，并查询 LDAP 属性后返回（分页）
// @Tags users
// @Produce json
// @Param page query int false "页码，从 1 开始"
// @Param page_size query int false "每页数量，1-1000"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/users [get]
func HandlerListUsers(c *gin.Context) {
	scli := slurmdbc.Default()
	lcli := ldapc.Default()
	if scli == nil || lcli == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "backend clients not initialized"})
		return
	}

	// Paging
	var pq model.PagingQuery
	_ = c.ShouldBindQuery(&pq)
	pq.SetDefaults(1, 20, 100)
	if err := pq.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "invalid paging parameters"})
		return
	}

	// Fetch slurm users (no deleted/admin filters for this endpoint)
	users, total, err := scli.GetUsersPaged(c.Request.Context(), nil, nil, pq.Offset(), pq.Limit())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	// Build username list
	names := make([]string, 0, len(users))
	for _, u := range users {
		if u.Name != "" {
			names = append(names, u.Name)
		}
	}
	// Query LDAP attributes
	ldapUsers, err := lcli.GetUserAttributesByUIDs(c.Request.Context(), names)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	// Index LDAP results by name
	ldapMap := make(map[string]model.User, len(ldapUsers))
	for _, u := range ldapUsers {
		ldapMap[u.Name] = u
	}
	// Merge into slurm users
	for i := range users {
		if lu, ok := ldapMap[users[i].Name]; ok {
			users[i].LDAPAttrs = lu.LDAPAttrs
		}
	}

	prev, next := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
	totalInt := int(total)
	c.JSON(http.StatusOK, response.Response{
		Count:    &totalInt,
		Previous: prev,
		Next:     next,
		Results:  users,
	})
}

// 其他 handler ...
