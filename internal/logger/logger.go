package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.SugaredLogger

func Initialize(logFile, level string) error {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	var cores []zapcore.Core

	consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)
	consoleCore := zapcore.NewCore(
		consoleEncoder,
		zapcore.AddSync(os.Stdout),
		zapLevel,
	)
	cores = append(cores, consoleCore)

	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", logFile, err)
		}

		fileEncoderConfig := zap.NewProductionEncoderConfig()
		fileEncoderConfig.TimeKey = "timestamp"
		fileEncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		fileEncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		fileEncoder := zapcore.NewJSONEncoder(fileEncoderConfig)
		fileCore := zapcore.NewCore(
			fileEncoder,
			zapcore.AddSync(file),
			zapLevel,
		)
		cores = append(cores, fileCore)
	}

	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	globalLogger = logger.Sugar()

	return nil
}

func Info(template string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Infof(template, args...)
	}
}

func Debug(template string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Debugf(template, args...)
	}
}

func Warn(template string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Warnf(template, args...)
	}
}

func Error(template string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Errorf(template, args...)
	}
}

func Sync() {
	if globalLogger != nil {
		_ = globalLogger.Sync()
	}
}
