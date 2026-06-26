package config

import (
	"os"
	"strconv"
	"strings"
)

// AppConfig is the global singleton accessed by all packages
var AppConfig *Config

type Config struct {
	Port         string
	BacktestPort string
	Mode         string
	LogLevel     string
	BarIntervals []string

	// Kite Connect Configuration
	KiteAPIKey      string
	KiteAccessToken string

	// Live Database
	LiveDBURL string

	// Backtest Database
	BacktestDBURL string

	// Backtest Configuration
	BacktestDataDir      string
	BacktestBackupDir    string
	BacktestDate         string
	BacktestSpeedFactor  float64
	TruncateBacktestData bool
	SkipLiveExecution    bool
}

func Load() {

	mode := getEnv("MODE", "backtest")

	livePort := getEnv("LIVE_PORT", "8080")
	backtestPort := getEnv("BACKTEST_PORT", "8081")

	activePort := livePort
	if mode == "backtest" {
		activePort = backtestPort
	}

	AppConfig = &Config{
		Port:                 activePort,
		Mode:                 mode,
		LogLevel:             getEnv("LOG_LEVEL", "info"),
		BarIntervals:         getEnvSlice("BAR_INTERVALS", []string{"1m", "3m", "5m", "10m", "15m"}),
		KiteAPIKey:           getEnv("KITE_API_KEY", ""),
		KiteAccessToken:      getEnv("KITE_ACCESS_TOKEN", ""),
		LiveDBURL:            getEnv("LIVE_DB_URL", ""),
		BacktestDBURL:        getEnv("BACKTEST_DB_URL", ""),
		BacktestDataDir:      getEnv("BACKTEST_DATA_DIR", ""),
		BacktestBackupDir:    getEnv("BACKTEST_BACKUP_DIR", ""),
		BacktestDate:         getEnv("BACKTEST_DATE", ""),
		BacktestSpeedFactor:  getEnvFloat("BACKTEST_SPEED_FACTOR", 0),
		TruncateBacktestData: getEnvBool("TRUNCATE_BACKTEST_DATA", false),
		SkipLiveExecution:    getEnvBool("SKIP_LIVE_EXEC", true),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		return strings.ToLower(value) == "true"
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if value, exists := os.LookupEnv(key); exists {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return fallback
}

func (c *Config) OverrideMode(mode string) {
	if mode != "" {
		c.Mode = mode
	}
}

// Helper to read slices from ENV (e.g. BAR_INTERVALS=1m,3m,5m)
func getEnvSlice(key string, fallback []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.Split(value, ",")
	}
	return fallback
}
