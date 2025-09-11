package ldap

import "github.com/gin-gonic/gin"

type Router struct{}

func (Router) Register(r *gin.Engine) {
    v1 := r.Group("/api/v1")
    {
        g := v1.Group("/ldap")
        g.GET("/users", HandlerListUsers) // GET /api/v1/ldap/users
    }
}

