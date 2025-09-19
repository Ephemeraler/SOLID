package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"solid/config"
	"solid/internal/app/router"

	"solid/internal/module/ldap"
	"solid/internal/module/slurmctld"
	"solid/internal/module/slurmdb"
	ldapc "solid/internal/pkg/client/ldap"
	"solid/internal/pkg/client/slurmctl"
	"solid/internal/pkg/log"

	docs "solid/internal/app/docs"
	slurmdbc "solid/internal/pkg/client/slurmdb"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/common/version"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title           SOLID
// @version         0.0.1-alpha
// @description     Slurm + OpenLDAP Identity Daemon
// @schema			http
// @BasePath        /api/v1
// @contact.email	hecheng@nscc-tj.cn
func main() {
	var (
		logOutput       string
		logFormat       string
		logFile         string
		logLevel        string
		lisenAddr       string
		configFile      string
		shutdownTimeout time.Duration
	)
	app := kingpin.New(filepath.Base(os.Args[0]), "slurm + ldap server.")
	app.HelpFlag.Short('h')
	app.Flag("log.level", "Log level, one of [debug, info, warn, error].").Default("info").EnumVar(&logLevel, "debug", "info", "warn", "error")
	app.Flag("log.output", "Log output, one of [stdout, stderr, file].").Default("stderr").EnumVar(&logOutput, "stdout", "stderr", "file")
	app.Flag("log.format", "Log format, one of [json, text].").Default("text").EnumVar(&logFormat, "json", "text")
	app.Flag("log.file", "Log file path when --output=file.").StringVar(&logFile)
	app.Flag("server.listen-addr", "Server listen address (e.g. :8080 or 127.0.0.1:8080)").Default(":8080").StringVar(&lisenAddr)
	app.Flag("config", "Path to YAML config file").Short('c').Default("config.yaml").StringVar(&configFile)
	app.Flag("server.shutdown-timeout", "Graceful shutdown timeout (e.g. 10s)").Default("10s").DurationVar(&shutdownTimeout)
	app.Version(version.Print("SOLID"))

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("failed to parse commandline arguments: %w", err))
		app.Usage(os.Args[1:])
		os.Exit(2)
	}

	// Internal helper to create configured logger
	logger, logClose, err := log.NewLogger(logOutput, logFormat, logFile, logLevel)
	if err != nil {
		fmt.Fprintf(os.Stdout, "unable to create logger: %w", err)
		return
	}
	defer logClose()

	// Load config
	cfg, err := config.Load(configFile)
	if err != nil {
		logger.Error("failed to load config", slog.String("path", configFile), slog.Any("err", err))
		os.Exit(1)
	}

	// Init slurmdb client and set as default

	scli, err := slurmdbc.New(cfg.Server.Slurmdb, logger.With("client", "slurmdb"))
	if err != nil {
		logger.Error("failed to initialize slurmdb client", slog.Any("err", err))
		os.Exit(1)
	}
	slurmdbc.SetDefault(scli)
	// // Init LDAP client and set as default

	lcli, err := ldapc.New(cfg.Server.LDAP)
	if err != nil {
		logger.Error("failed to initialize ldap client", slog.Any("err", err))
		os.Exit(1)
	}
	ldapc.SetDefault(lcli)

	slurmctlClient := &slurmctl.Client{}
	slurmctlClient.Set(exec.CommandContext, logger)
	slurmctl.SetDefault(slurmctlClient)

	// Build router
	r := router.New()
	docs.SwaggerInfo.BasePath = "/api/v1"
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// 注册所有模块（也可做“按需编译”或通过 build tag 控制）
	// router.Register(
	// 	user.Router{},
	// 	slurmdbmod.Router{},
	// 	ldapmod.Router{},
	// )
	router.Register(
		slurmdb.Router{},
		slurmctld.Router{},
		ldap.Router{},
	)
	router.Mount(r)
	srv := &http.Server{
		Addr:              lisenAddr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start server in background
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("server listening", slog.String("addr", lisenAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-serverErr:
		if err != nil {
			logger.Error("server failed", slog.Any("err", err))
			os.Exit(1)
		}
	case <-quit:
		// proceed to shutdown
	}
	logger.Info("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", slog.Any("err", err))
	}
	logger.Info("server exiting")
}
