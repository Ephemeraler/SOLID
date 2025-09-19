package router

import "github.com/gin-gonic/gin"

// 每个模块提供一个 Register(Route) 函数，实现下面签名：
type Registrar interface{ Register(r *gin.Engine) }

// 全局注册表（集中声明要装配的模块）
var registrars []Registrar

// Register 向全局注册表中注册模块
func Register(rs ...Registrar) { registrars = append(registrars, rs...) }

// Mount 挂载所有模块
func Mount(r *gin.Engine) {
	for _, rg := range registrars {
		rg.Register(r)
	}
}
