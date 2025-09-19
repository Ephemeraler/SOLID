package ldap

import (
	"github.com/gin-gonic/gin"
)

type Router struct{}

func (Router) Register(r *gin.Engine) {
	v1 := r.Group("/api/v1/ldap")
	{
		v1.GET("/users", HandlerGetUsers)                 // GET /api/v1/ldap/users?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/user/:uid", HandlerGetUser)              // GET /api/v1/ldap/user/:uid
		v1.GET("/user/:uid/groups", HandlerGetUserGroups) // /api/v1/ldap/user/:uid/groups
		v1.POST("/user", HandlerCreateUser)               // POST /api/v1/ldap/user
		v1.PUT("/user/:uid", HandlerUpdateUser)           // PUT /api/v1/ldap/user/:uid
		v1.DELETE("/user/:uid", HandlerDeteleUser)        // DELETE /api/v1/ldap/user/:uid
		v1.GET("/groups", HandlerGetGroups)               // GET /api/v1/ldap/groups?paging=xxx&page=xxx&page_size=xxx
		v1.GET("/group/:cn", HandlerGetGroup)             // GET /api/v1/ldap/group/:cn
		v1.POST("/group", HandlerCreateGroup)             // POST /api/v1/ldap/group
		v1.PUT("/group/:cn", HandlerUpdateGroup)          // PUT /api/v1/ldap/group/:cn
		v1.DELETE("/group/:cn", HandlerGetGroup)          // DELETE /api/v1/ldap/group/:cn
	}
}
