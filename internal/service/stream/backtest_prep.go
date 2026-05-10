package stream

import (
	"archive/tar"
	"fmt"
	"gidh-backend/pkg/logger"
	"io"
	"os"
	"path/filepath"

	"github.com/ulikunitz/xz" // Add this: go get github.com/ulikunitz/xz
)

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

	// 3. Open the .tar.xz file
	file, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("failed to open tar.xz file: %w", err)
	}
	defer file.Close()

	// Create xz reader
	xzReader, err := xz.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create xz reader: %w", err)
	}

	// Create tar reader
	tarReader := tar.NewReader(xzReader)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		targetPath := filepath.Join(dataDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return fmt.Errorf("failed to create dir: %w", err)
			}
		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent dir: %w", err)
			}

			outFile, err := os.Create(targetPath)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to write file: %w", err)
			}
			outFile.Close()
		}
	}

	logger.Info("Preparation complete.")
	return nil
}
