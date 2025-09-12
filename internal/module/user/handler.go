package user

import (
    "net/http"
    "strconv"

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

// HandlerListAccts 列出账户（分页），返回账户的 gidNumber、描述信息，以及账户下的用户清单与其 uidNumber。
//
// 实现流程：
//  1. 根据分页参数从 slurmdb.acct_table 获取账户（仅 deleted=0）。
//  2. 从 LDAP 按账户名批量获取 gidNumber。
//  3. 从 slurmdb.<ClusterName>_assoc_table 为每个账户获取所属用户名称（仅非删除、user 不为空）。
//  4. 将所有用户名去重后，从 LDAP 批量获取 uidNumber。
//  5. 组装返回数据并附带分页信息。
//
// @Summary 列出账户（含 gid 与成员 uid）（分页）
// @Description 从 SlurmDB 获取账户（deleted=0），并从 LDAP 查询 gidNumber 与用户 uidNumber 后返回（分页）
// @Tags accounts
// @Produce json
// @Param page query int false "页码，从 1 开始"
// @Param page_size query int false "每页数量，1-1000"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
// @Router /api/v1/accounts [get]
func HandlerListAccts(c *gin.Context) {
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

    // Step 1: slurmdb.acct_table (deleted=0)
    deleted := 0
    accts, total, err := scli.GetAcctsPaged(c.Request.Context(), &deleted, pq.Offset(), pq.Limit())
    if err != nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
        return
    }

    // Collect account names
    acctNames := make([]string, 0, len(accts))
    for _, a := range accts {
        if a.Name != "" {
            acctNames = append(acctNames, a.Name)
        }
    }

    // Step 2: LDAP gidNumbers by account names
    gidMap, err := lcli.GetGIDNumberByAccountNames(c.Request.Context(), acctNames)
    if err != nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
        return
    }

    // Step 3: users under each account; also gather all usernames for step 4
    acctUsers := make(map[string][]string, len(accts))
    userSet := make(map[string]struct{})
    for _, a := range accts {
        if a.Name == "" {
            continue
        }
        names, err := scli.GetUserNamesByAccount(c.Request.Context(), a.Name)
        if err != nil {
            c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
            return
        }
        acctUsers[a.Name] = names
        for _, n := range names {
            if n == "" {
                continue
            }
            userSet[n] = struct{}{}
        }
    }

    // Step 4: LDAP uidNumbers by usernames
    allUsers := make([]string, 0, len(userSet))
    for name := range userSet {
        allUsers = append(allUsers, name)
    }
    ldapUsers, err := lcli.GetUserAttributesByUIDs(c.Request.Context(), allUsers)
    if err != nil {
        c.JSON(http.StatusInternalServerError, response.Response{Detail: err.Error()})
        return
    }
    // Build uidNumber map
    uidMap := make(map[string]uint32, len(ldapUsers))
    for _, u := range ldapUsers {
        if v := u.LDAPAttrs["uidNumber"]; len(v) > 0 {
            // parse to uint32; ignore parse errors silently
            if n, err := strconv.ParseUint(v[0], 10, 32); err == nil {
                uidMap[u.Name] = uint32(n)
            }
        }
    }

    // Step 5: assemble response
    type userItem struct {
        Name string `json:"name"`
        UID  uint32 `json:"uid"`
    }
    type acctItem struct {
        GID      uint32     `json:"gid"`
        Acct     string     `json:"acct"`
        Desc     string     `json:"desc"`
        Userlist []userItem `json:"userlist"`
    }

    out := make([]acctItem, 0, len(accts))
    for _, a := range accts {
        users := acctUsers[a.Name]
        ui := make([]userItem, 0, len(users))
        for _, n := range users {
            ui = append(ui, userItem{
                Name: n,
                UID:  uidMap[n],
            })
        }
        out = append(out, acctItem{
            GID:      gidMap[a.Name],
            Acct:     a.Name,
            Desc:     a.Description,
            Userlist: ui,
        })
    }

    prev, next := response.BuildPageLinks(c.Request.URL, pq.Page, pq.PageSize, int(total))
    totalInt := int(total)
    c.JSON(http.StatusOK, response.Response{
        Count:    &totalInt,
        Previous: prev,
        Next:     next,
        Results:  out,
    })
}
