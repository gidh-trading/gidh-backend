package stream

import (
	"fmt"
	"gidh-backend/pkg/logger"
	"os"
	"os/exec"
	"path/filepath"
)

// PrepareBacktestData replaces the 'make prepare-backtest' logic.
// It extracts the .tar.xz backup for a specific date if the directory doesn't exist.
func PrepareBacktestData(backupDir, dataDir, dateStr string) error {
	targetFolder := filepath.Join(dataDir, dateStr)

	// 1. Check if already extracted
	if _, err := os.Stat(targetFolder); !os.IsNotExist(err) {
		logger.Infof("Backtest data for %s already exists. Skipping extraction.", dateStr)
		return nil
	}

	// 2. Validate backup exists
	tarPath := filepath.Join(backupDir, fmt.Sprintf("backup_%s.tar.xz", dateStr))
	if _, err := os.Stat(tarPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file %s not found in %s", tarPath, backupDir)
	}

	logger.Infof("Extracting backtest data for %s...", dateStr)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	// 3. Execute tar extraction
	cmd := exec.Command("tar", "-xJf", tarPath, "-C", dataDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("tar extraction failed: %v, output: %s", err, string(output))
	}

	logger.Info("Preparation complete.")
	return nil
}
