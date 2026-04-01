package main

import (
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

func registerDashboardRoutes(r *gin.Engine) error {
	webFS, err := fs.Sub(dashboardAssets, "web")
	if err != nil {
		return err
	}

	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusTemporaryRedirect, "/dashboard")
	})

	r.GET("/dashboard", func(c *gin.Context) {
		c.FileFromFS("dashboard.html", http.FS(webFS))
	})

	r.StaticFS("/dashboard/assets", http.FS(webFS))
	return nil
}
