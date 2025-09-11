package user

import "github.com/gin-gonic/gin"

type Router struct{}

func (Router) Register(r *gin.Engine) {
    v1 := r.Group("/api/v1")
    {
        g := v1.Group("/users")
        g.GET("", HandlerListUsers) // GET /api/v1/users
        // g.POST("", CreateUser) // POST /api/v1/users
        // g.GET("/:id", GetUser) // GET /api/v1/users/:id
    }
}
