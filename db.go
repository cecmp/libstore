package libstore

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// dbOps provides database operations for interacting with a PostgreSQL database.
type dbOps struct {
	db *sql.DB
}

// NewDBOps initializes a new dbOps instance with a connection to a PostgreSQL database.
//
// Parameters:
//   - ctx: Context for managing request lifecycles.
//   - conn: Connection string for connecting to the PostgreSQL database.
//
// Returns:
//   - An initialized dbOps instance that implements the Ops interface.
//   - An error if the database connection fails or if the table cannot be created.
//
// The function opens a connection to the PostgreSQL database using the provided connection string,
// and ensures that the necessary table ('FILES') exists by creating it if it does not.
//
// Note:
// The function returns an OpsInternalError if any step of the initialization fails.
func NewDBOps(ctx context.Context, conn string) (Ops, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to open database connection"), err)
	}
	query := `
		CREATE TABLE IF NOT EXISTS FILES (
				id SERIAL PRIMARY KEY,
				key TEXT NOT NULL,
				value BYTEA,
				version BIGINT NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
	`
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to create table"), err)
	}

	return dbOps{
		db: db,
	}, nil
}

// Create implements Ops.
func (d dbOps) Create(ctx context.Context, key string) error {
	// Check if the key already exists
	var existingKey string
	err := d.db.QueryRowContext(ctx, "SELECT key FROM FILES WHERE key = $1", key).Scan(&existingKey)
	if err != nil && err != sql.ErrNoRows {
		return (fmt.Errorf("%w: %w", OpsInternalError("failed to check existing key"), err))
	}
	if existingKey != "" {
		return KeyError("key already exists: " + key)
	}

	_, err = d.db.ExecContext(ctx, "INSERT INTO FILES (key, value, version) VALUES ($1, NULL, 0)", key)
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to create key"), err)
	}
	return nil
}

// Delete implements Ops.
func (d dbOps) Delete(ctx context.Context, key string) error {
	result, err := d.db.ExecContext(ctx, "DELETE FROM FILES WHERE key = $1", key)
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to delete key"), err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to determine rows affected"), err)
	}
	if rowsAffected == 0 {
		return KeyNotFoundError("key not found: " + key)
	}
	return nil
}

// List implements Ops.
func (d dbOps) List(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT key FROM FILES")
	if err != nil {
		return nil, fmt.Errorf("%w : %w", OpsInternalError("failed to list keys"), err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("%w : %w", OpsInternalError("failed to scan key"), err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", OpsInternalError("rows iteration error"), err)
	}
	return keys, nil
}

// Read implements Ops.
func (d dbOps) Read(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := d.db.QueryRowContext(ctx, "SELECT value FROM FILES WHERE key = $1 ORDER BY version DESC LIMIT 1", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, KeyNotFoundError("key not found: " + key)
		}
		return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to read last entry"), err)
	}
	return value, nil
}

// ReadAll implements Ops.
func (d dbOps) ReadAll(ctx context.Context, key string) ([][]byte, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT value FROM FILES WHERE key = $1 ORDER BY version ASC", key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to read whole content"), err)
	}
	defer rows.Close()

	var values [][]byte
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("%w: %w", OpsInternalError("failed to scan value"), err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", OpsInternalError("rows iteration error"), err)
	}
	return values, nil
}

// Put implements Ops.
func (d dbOps) Put(ctx context.Context, key string, entry []byte) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to begin transaction"), err)
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// Increment the version
	var maxVersion int64
	err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM FILES WHERE key = $1", key).Scan(&maxVersion)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to get max version"), err)
	}

	// Insert the new version
	_, err = tx.ExecContext(ctx, "INSERT INTO FILES (key, value, version) VALUES ($1, $2, $3)", key, entry, maxVersion+1)
	if err != nil {
		return fmt.Errorf("%w: %w", OpsInternalError("failed to replace entry"), err)
	}

	return nil
}

var _ Ops = dbOps{}
