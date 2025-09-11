package ldap

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	gldap "github.com/go-ldap/ldap/v3"

	ldapc "solid/client/ldap"
	"solid/internal/pkg/common/response"
	"solid/internal/pkg/model"
)

// HandlerListUsers 列出 LDAP 中创建的所有用户（分页），返回所有属性。
//
// 说明：
//   - 在 BaseDN 下搜索用户对象（objectClass: inetOrgPerson/person/posixAccount），返回全部属性。
//   - 为保证幂等，结果按用户名（uid 或 cn）字典序排序后分页切片。
//
// @Summary 列出 LDAP 用户（全部属性）
// @Description 在 LDAP 中搜索用户对象，返回其全部属性，结果按名称排序并分页
// @Tags ldap, users
// @Produce json
// @Param page query int false "页码，从 1 开始"
// @Param page_size query int false "每页数量，1-1000"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/ldap/users [get]
func HandlerListUsers(c *gin.Context) {
	cli := ldapc.Default()
	if cli == nil || cli.Conn == nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: "ldap client not initialized"})
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

	// Search LDAP for user entries
	// Match common user objectClasses; adjust as needed for your directory schema.
	filter := "(|(objectClass=inetOrgPerson)(objectClass=person)(objectClass=posixAccount))"
	req := ldapBuildSearchAll(cli, filter)
	resp, err := cli.Conn.Search(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
		return
	}

	// Map entries to model.User and collect attributes
	results := make(model.Users, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		attrs := make(map[string][]string, len(e.Attributes))
		for _, a := range e.Attributes {
			vv := make([]string, len(a.Values))
			copy(vv, a.Values)
			attrs[a.Name] = vv
		}
		name := e.GetAttributeValue(cli.UsernameAttr)
		if name == "" {
			name = e.GetAttributeValue("cn")
		}
		results = append(results, model.User{
			Name:      name,
			LDAPAttrs: attrs,
		})
	}

	// Deterministic order for idempotency
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

	total := len(results)
	// Apply pagination by slicing
	start := pq.Offset()
	if start > total {
		start = total
	}
	end := start + pq.PageSize
	if end > total {
		end = total
	}
	pageItems := results[start:end]

	prev, next := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, total)
	c.JSON(http.StatusOK, response.Response{
		Count:    &total,
		Previous: prev,
		Next:     next,
		Results:  pageItems,
	})
}

// ldapcBuildSearchAll builds a search request to fetch all attributes for entries
// matching the provided filter under the client's BaseDN.
func ldapBuildSearchAll(cli *ldapc.Client, filter string) *gldap.SearchRequest {
	// Use scope whole subtree, no attribute list to fetch all attributes
	return gldap.NewSearchRequest(
		cli.BaseDN,
		gldap.ScopeWholeSubtree,
		gldap.NeverDerefAliases,
		0, 0, false,
		filter,
		[]string{},
		nil,
	)
}
