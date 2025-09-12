package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ldapc "solid/client/ldap"
	slurmdbc "solid/client/slurmdb"
	"solid/config"
	"solid/internal/app/router"
	ldapmod "solid/internal/module/ldap"
	slurmdbmod "solid/internal/module/slurmdb"
	"solid/internal/module/user"

	docs "solid/internal/app/docs"

	kingpin "github.com/alecthomas/kingpin/v2"
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
	// CLI flags
	var (
		addrFlag        = kingpin.Flag("addr", "Server listen address (e.g. :8080 or 127.0.0.1:8080)").Default(":8080").Envar("SOLID_ADDR").String()
		shutdownTimeout = kingpin.Flag("shutdown-timeout", "Graceful shutdown timeout (e.g. 10s)").Default("10s").Envar("SOLID_SHUTDOWN_TIMEOUT").String()
		logFormat       = kingpin.Flag("log-format", "Log format").Default("text").Envar("SOLID_LOG_FORMAT").Enum("text", "json")
		logOutput       = kingpin.Flag("log-output", "Log output destination").Default("stdout").Envar("SOLID_LOG_OUTPUT").Enum("stdout", "stderr", "file")
		logFile         = kingpin.Flag("log-file", "Log file path (used when --log-output=file)").Envar("SOLID_LOG_FILE").String()
		configFile      = kingpin.Flag("config", "Path to YAML config file").Short('c').Default("config.yaml").Envar("SOLID_CONFIG").String()
	)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	// Internal helper to create configured logger
	logger, cleanup, err := newLogger(*logOutput, *logFormat, func() string {
		if logFile == nil {
			return ""
		}
		return *logFile
	}())
	if err != nil {
		// Fallback to stderr if logger setup fails
		fmt.Fprintf(os.Stderr, "failed to setup logger: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	// Load config
	cfg, err := config.Load(*configFile)
	if err != nil {
		logger.Error("failed to load config", slog.String("path", *configFile), slog.Any("err", err))
		os.Exit(1)
	}

	// Init slurmdb client and set as default
	scli, err := slurmdbc.New(cfg.Server.Slurmdb)
	if err != nil {
		logger.Error("failed to initialize slurmdb client", slog.Any("err", err))
		os.Exit(1)
	}
	slurmdbc.SetDefault(scli)

	// Init LDAP client and set as default
	lcli, err := ldapc.New(cfg.Server.LDAP)
	if err != nil {
		logger.Error("failed to initialize ldap client", slog.Any("err", err))
		os.Exit(1)
	}
	ldapc.SetDefault(lcli)

	// Build router
	r := router.New()
	docs.SwaggerInfo.BasePath = "/api/v1"
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// 注册所有模块（也可做“按需编译”或通过 build tag 控制）
	router.Register(
		user.Router{},
		slurmdbmod.Router{},
		ldapmod.Router{},
	)
	router.MountAll(r)

	// Address (only --addr is used)
	addr := *addrFlag

	// HTTP server with graceful shutdown
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start server in background
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("server listening", slog.String("addr", addr))
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

	// Parse shutdown timeout
	to, err := time.ParseDuration(*shutdownTimeout)
	if err != nil || to <= 0 {
		to = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", slog.Any("err", err))
	}
	// Close slurmdb client
	// if scli != nil {
	// 	_ = scli.Close()
	// }
	// // Close LDAP client
	// if lcli != nil {
	// 	lcli.Close()
	// }
	logger.Info("server exiting")
}

func newLogger(logOutput, logFormat, logFile string) (*slog.Logger, func(), error) {
	var w io.Writer
	var closer io.Closer
	switch logOutput {
	case "stdout", "":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	case "file":
		if logFile == "" {
			return nil, nil, fmt.Errorf("--log-file is required when --log-output=file")
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, err
		}
		w = f
		closer = f
	default:
		return nil, nil, fmt.Errorf("unsupported log output: %s", logOutput)
	}

	var handler slog.Handler
	switch logFormat {
	case "json":
		handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: false})
	case "text":
		handler = slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: false})
	default:
		return nil, nil, fmt.Errorf("unsupported log format: %s", logFormat)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	cleanup := func() {
		if closer != nil {
			_ = closer.Close()
		}
	}
	return logger, cleanup, nil
}
