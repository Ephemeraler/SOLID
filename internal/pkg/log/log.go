package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger 创建 Logger. output 为日志输出类型当前支持 "stderr", "stdout", "file". 当
// output 为 file 时, 需要设定 filename 参数指定日志输出文件位置及名称. format 用于设定日志输出格式, 当前
// 支持 "json", "text" 类型. level 用于设定日志输出的最低级别.
func NewLogger(output, format, filename, level string) (*slog.Logger, func(), error) {
	var w io.Writer
	var closer io.Closer
	switch strings.ToLower(output) {
	case "stdout":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	case "file":
		if filename == "" {
			return nil, nil, fmt.Errorf("unable to create log file which name is null(\"\")")
		}
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create log file(%s): %w", filename, err)
		}
		w = f
		closer = f
	default:
		return nil, nil, fmt.Errorf("unsupported log output: %s", output)
	}

	ho := &slog.HandlerOptions{
		AddSource: true,
	}
	switch strings.ToLower(level) {
	case "debug":
		ho.Level = slog.LevelDebug
	case "info":
		ho.Level = slog.LevelInfo
	case "warn":
		ho.Level = slog.LevelWarn
	case "error":
		ho.Level = slog.LevelError
	default:
		return nil, nil, fmt.Errorf("unsupported log level: %s", level)
	}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(w, ho)
	case "text":
		handler = slog.NewTextHandler(w, ho)
	default:
		return nil, nil, fmt.Errorf("unsupported log format: %s", format)
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
