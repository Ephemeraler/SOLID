package slurmdb

import "github.com/gin-gonic/gin"

type Router struct{}

func (Router) Register(r *gin.Engine) {
    v1 := r.Group("/api/v1")
    {
        g := v1.Group("/slurm/accounting")
        g.GET("/users", HandlerListUsers) // GET /api/v1//slurm/accounting/users
        g.GET("/acounts", HandlerListAccts) // GET /api/v1/slurm/accounting/acounts
        // g.POST("", CreateUser) // POST /api/v1/users
        // g.GET("/:id", GetUser) // GET /api/v1/users/:id
    }
}
