package router

import (
	"github.com/gin-gonic/gin"
)

func New() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	// TODO: 日志、鉴权、CORS、trace、中间件
	return r
}
