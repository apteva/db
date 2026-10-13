package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var ErrRestoreIncomplete = errors.New("aptevadb: incomplete restore; remove the incomplete destination and restore again")
var ErrDestinationExists = errors.New("aptevadb: restore destination already exists")

const restoreMarker = ".restore-incomplete"

// Restore verifies a backup and creates a new database directory. The
// destination must not exist. A marker prevents Open from accepting an
// interrupted restore. Existing directories are never overwritten.
func Restore(ctx context.Context, backupDir, destination string) error {
	absoluteDestination, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	destination = absoluteDestination
	absoluteSource, err := filepath.Abs(backupDir)
	if err != nil {
		return err
	}
	backupDir = absoluteSource
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(backupDir, "data")); err != nil {
		return err
	}
	src, err := Open(backupDir, DurableOptions())
	if err != nil {
		return err
	}
	defer src.Close()
	if err = src.Verify(ctx); err != nil {
		return fmt.Errorf("backup verification: %w", err)
	}
	if err = src.VerifyIndexes(ctx); err != nil {
		return fmt.Errorf("backup index verification: %w", err)
	}
	parent := filepath.Dir(destination)
	if err = os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	// Mkdir reserves a fresh destination without replacing anything.
	if err = os.Mkdir(destination, 0755); err != nil {
		if os.IsExist(err) {
			return ErrDestinationExists
		}
		return err
	}
	marker := filepath.Join(destination, restoreMarker)
	if err = os.WriteFile(marker, []byte("AptevaDB restore in progress\n"), 0600); err != nil {
		return err
	}
	if err = syncRestoreDirectory(destination); err != nil {
		return err
	}
	stage := filepath.Join(destination, "staged")
	if err = src.Backup(ctx, stage); err != nil {
		return err
	}
	copyDB, err := Open(stage, DurableOptions())
	if err != nil {
		return err
	}
	verifyErr := copyDB.Verify(ctx)
	if verifyErr == nil {
		verifyErr = copyDB.VerifyIndexes(ctx)
	}
	closeErr := copyDB.Close()
	if verifyErr != nil {
		return verifyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return publishStagedRestore(stage, destination)
}

func syncRestoreDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
