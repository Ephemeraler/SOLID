package ldap

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	ldapc "solid/internal/pkg/client/ldap"
	"solid/internal/pkg/common/paging"
	"solid/internal/pkg/common/response"
)

// HandlerGetUsers 列出 LDAP 用户（全部属性）。
//
// @Summary 列出 LDAP 用户（全部属性）
// @Description 在 LDAP 中搜索用户对象（ou=People,<baseDN>，uid=*），返回其全部属性，结果按 uid 升序排序并支持分页
// @Tags ldap, users
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页码，从 1 开始（仅当 paging=true 生效）" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100（仅当 paging=true 生效）" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/users [get]
func HandlerGetUsers(c *gin.Context) {
	var pq paging.PagingQuery
	if err := c.ShouldBindQuery(&pq); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: fmt.Sprintf("参数请求错误: %s", err)})
		return
	}

	fmt.Printf("%+v\n", pq)

	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}

	// 首先取全量用于稳定排序与分页（uid 升序）
	allUsers, err := client.GetUsers(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	// 构造响应：根据 pq.Paging 决定是否分页
	total := len(allUsers)
	if pq.Paging {
		// 规范化页码与页大小边界
		if pq.Page < 1 {
			pq.Page = 1
		}
		if pq.PageSize < 1 {
			pq.PageSize = 20
		} else if pq.PageSize > 100 {
			pq.PageSize = 100
		}
		start := (pq.Page - 1) * pq.PageSize
		if start > total {
			start = total
		}
		end := start + pq.PageSize
		if end > total {
			end = total
		}
		pageSlice := allUsers[start:end]
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
		c.JSON(http.StatusOK, response.Response{Count: total, Previous: prevURL, Next: nextURL, Results: pageSlice})
		return
	}

	// 不分页：直接返回全量
	c.JSON(http.StatusOK, response.Response{Count: total, Results: allUsers})
}

// HandlerGetUser 获取某个用户的信息.
//
// @Summary 获取指定 LDAP 用户
// @Description 通过路径参数 uid 搜索 ou=Peoples,<baseDN> 下的用户对象，返回其全部属性
// @Tags ldap, user
// @Produce json
// @Param uid path string true "用户 uid"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/user/:uid [get]
func HandlerGetUser(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	uid := strings.TrimSpace(c.Param("uid"))
	if uid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing uid parameter"})
		return
	}
	row, err := client.GetUser(c.Request.Context(), uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	if row == nil || len(row) == 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "user not found"})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerGetUserGroups 返回用户附加组
// @Router /api/v1/ldap/user/:user/groups [get]
func HandlerGetUserGroups(c *gin.Context) {
    client := ldapc.Default()
    if client == nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
        return
    }
    // 路由使用 :uid，但注释使用 :user，这里同时兼容
    uid := strings.TrimSpace(c.Param("uid"))
    if uid == "" {
        uid = strings.TrimSpace(c.Param("user"))
    }
    if uid == "" {
        c.JSON(http.StatusBadRequest, response.Response{Detail: "missing uid parameter"})
        return
    }
    groups, err := client.GetAdditionalGroupsOfUser(c.Request.Context(), uid)
    if err != nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
        return
    }
    c.JSON(http.StatusOK, response.Response{Count: len(groups), Results: groups})
}

// HandlerCreateUser 创建用户, 默认情况下创建用户类型为 "inetOrgPerson", "posixAccount", "shadowAccount"
// @Router /api/v1/ldap/user [post]
func HandlerCreateUser(c *gin.Context) {
	user := make(map[string]string)
	if err := c.BindJSON(&user); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: fmt.Sprintf("invalid json: %s", err)})
		return
	}

	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}

	// Extract uid from payload
	var uid string
	if vals, ok := user["uid"]; ok && len(vals) > 0 {
		uid = strings.TrimSpace(vals)
	}
	if uid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing uid in payload"})
		return
	}

	if err := client.AddUser(c.Request.Context(), uid, user); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: user})
}

// @Router /api/v1/ldap/user/:uid [put]
func HandlerUpdateUser(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	uid := strings.TrimSpace(c.Param("uid"))
	if uid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing uid parameter"})
		return
	}

	// Accept body as map[string][]string, then convert to Attribute (map[string]string)
	var attrs map[string]string
	if err := c.BindJSON(&attrs); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: fmt.Sprintf("invalid json: %s", err)})
		return
	}

	if err := client.UpdateUser(c.Request.Context(), uid, attrs); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	// Read back updated entry for response
	row, err := client.GetUser(c.Request.Context(), uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerDeteleUser 删除 LDAP 某个用户
// @Summary 删除指定 LDAP 用户
// @Description 通过路径参数 uid 删除 ou=Peoples,<baseDN> 下的用户；删除前会先查询确认存在，并返回被删除的用户属性
// @Tags ldap, user
// @Produce json
// @Param uid path string true "用户 uid"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/user/:uid [delete]
func HandlerDeteleUser(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	uid := strings.TrimSpace(c.Param("uid"))
	if uid == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing uid parameter"})
		return
	}
	// 先查询，确认用户存在并获取其属性（作为返回）
	row, err := client.GetUser(c.Request.Context(), uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	if row == nil || len(row) == 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "user not found"})
		return
	}
	// 执行删除
	if err := client.DelUser(c.Request.Context(), uid); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerGetGroups 列出 LDAP 用户组（全部属性）。
//
// @Summary 列出 LDAP 用户组（全部属性）
// @Description 在 LDAP 中搜索组对象（ou=Groups,<baseDN>，cn=*），返回其全部属性，结果按 gidNumber 升序排序并支持分页
// @Tags ldap, groups
// @Produce json
// @Param paging query bool false "是否开启分页" default(true)
// @Param page query int false "页码，从 1 开始（仅当 paging=true 生效）" minimum(1) default(1)
// @Param page_size query int false "每页数量，1-100（仅当 paging=true 生效）" minimum(1) maximum(100) default(20)
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/groups [get]
func HandlerGetGroups(c *gin.Context) {
	var pq paging.PagingQuery
	if err := c.ShouldBindQuery(&pq); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: fmt.Sprintf("参数请求错误: %s", err)})
		return
	}
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	allGroups, err := client.GetGroups(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	total := len(allGroups)
	if pq.Paging {
		if pq.Page < 1 {
			pq.Page = 1
		}
		if pq.PageSize < 1 {
			pq.PageSize = 20
		} else if pq.PageSize > 100 {
			pq.PageSize = 100
		}
		start := (pq.Page - 1) * pq.PageSize
		if start > total {
			start = total
		}
		end := start + pq.PageSize
		if end > total {
			end = total
		}
		pageSlice := allGroups[start:end]
		prevURL, nextURL := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
		c.JSON(http.StatusOK, response.Response{Count: total, Previous: prevURL, Next: nextURL, Results: pageSlice})
		return
	}
	c.JSON(http.StatusOK, response.Response{Count: total, Results: allGroups})
}

// HandlerGetGroup 获取指定 LDAP 组（全部属性）。
//
// @Summary 获取指定 LDAP 组
// @Description 通过路径参数 cn 搜索 ou=Groups,<baseDN> 下的组对象，返回其全部属性
// @Tags ldap, group
// @Produce json
// @Param cn path string true "组名 cn"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/group/:cn [get]
func HandlerGetGroup(c *gin.Context) {
	// 注意：该接口按你的要求执行删除操作（调用 DelGroup），并返回被删除组的属性
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	cn := strings.TrimSpace(c.Param("cn"))
	if cn == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing cn parameter"})
		return
	}
	// 先查询，确认存在并用于返回
	row, err := client.GetGroup(c.Request.Context(), cn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	if row == nil || len(row) == 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "group not found"})
		return
	}
	// 删除
	if err := client.DelGroup(c.Request.Context(), cn); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// HandlerDeteleGroup 删除指定 LDAP 组。
//
// @Summary 删除指定 LDAP 组
// @Description 通过路径参数 cn 删除 ou=Groups,<baseDN> 下的组；删除前会先查询确认存在，并返回被删除的组属性
// @Tags ldap, group
// @Produce json
// @Param cn path string true "组名 cn"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/group/:cn [delete]
func HandlerDeteleGroup(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	cn := strings.TrimSpace(c.Param("cn"))
	if cn == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing cn parameter"})
		return
	}
	row, err := client.GetGroup(c.Request.Context(), cn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	if row == nil || len(row) == 0 {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "group not found"})
		return
	}
	if err := client.DelGroup(c.Request.Context(), cn); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// @Router /api/v1/ldap/group [post]
func HandlerCreateGroup(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	// Accept JSON as map[string][]string
	var attrs map[string]string
	if err := c.BindJSON(&attrs); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: fmt.Sprintf("invalid json: %s", err)})
		return
	}
	// Extract cn from body
	var cn string
	if vals, ok := attrs["cn"]; ok && len(vals) > 0 {
		cn = strings.TrimSpace(vals)
	}
	if cn == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing cn in payload"})
		return
	}

	if err := client.AddGroup(c.Request.Context(), cn, attrs); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	// Read back created group
	row, err := client.GetGroup(c.Request.Context(), cn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}

// @Router /api/v1/ldap/group/:cn [put]
func HandlerUpdateGroup(c *gin.Context) {
	client := ldapc.Default()
	if client == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
		return
	}
	cn := strings.TrimSpace(c.Param("cn"))
	if cn == "" {
		c.JSON(http.StatusBadRequest, response.Response{Detail: "missing cn parameter"})
		return
	}
	var attrs map[string]string
	if err := c.BindJSON(&attrs); err != nil {
		c.JSON(http.StatusBadRequest, response.Response{Detail: fmt.Sprintf("invalid json: %s", err)})
		return
	}

	if err := client.UpdateGroup(c.Request.Context(), cn, attrs); err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	row, err := client.GetGroup(c.Request.Context(), cn)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}
	c.JSON(http.StatusOK, response.Response{Results: row})
}
