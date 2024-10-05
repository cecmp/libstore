package libstore

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// fileOps implements the Ops interface for file operations.
type fileOps struct {
	location string
}

// NewFileOps initializes a new Ops instance with an OS filesystem-based implementation.
// It returns an error if the provided location is invalid.
func NewFileOps(location string) (Ops, error) {
	fileInfo, err := os.Stat(location)
	if os.IsNotExist(err) {
		// Directory doesn't exist, create it
		err = os.MkdirAll(location, 0755)
		if err != nil {
			return fileOps{}, fmt.Errorf("file: error creating directory: %w", err)
		}
	} else if err != nil {
		return fileOps{}, fmt.Errorf("file: error checking directory info: %w", err)
	} else if !fileInfo.IsDir() {
		return fileOps{}, fmt.Errorf("file: %s is not a directory", location)
	}

	return fileOps{location: location}, nil
}

// Create creates a new file with the given key.
// It returns an error if the file already exists or if there is an issue creating the file.
func (fops fileOps) Create(ctx context.Context, key string) error {
	path := filepath.Join(fops.location, key)
	if _, err := os.Stat(path); err == nil {
		return KeyError(fmt.Sprintf("file: file %s already exists", key))
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("%w: %w", KeyError("file: checking if file exists"), err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: creating file %s", key)), err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Debug("closing file", "error", cerr)
		}
	}()
	return nil
}

// ReadAll reads the entire content of the file with the given key.
// It returns the content as a byte slice or an error if the file cannot be read.
func (fops fileOps) ReadAll(ctx context.Context, key string) ([][]byte, error) {
	path := filepath.Join(fops.location, key)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, KeyNotFoundError(fmt.Sprintf("file: key not found %s", key))
		}
		return nil, fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: reading file %s", key)), err)
	}
	defer file.Close()

	var lines [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: reading file %s lines", key)), err)
	}

	return lines, nil
}

// Read reads the last line of the file with the given key.
// It returns the last line as a byte slice or an error if the file cannot be read.
func (fops fileOps) Read(ctx context.Context, key string) ([]byte, error) {
	path := filepath.Join(fops.location, key)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, KeyNotFoundError(fmt.Sprintf("file: key not found %s", key))
		}
		return nil, fmt.Errorf("file: opening file %s: %w", key, err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Debug("closing file", "error", cerr)
		}
	}()

	scanner := bufio.NewScanner(file)
	var lastLine []byte

	for scanner.Scan() {
		lastLine = scanner.Bytes()
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: reading file %s", key)), err)
	}
	if len(lastLine) == 0 {
		return nil, EntryError(fmt.Sprintf("file: file is empty for name %s", path))
	}
	return lastLine, nil
}

// Put appends an entry to the file with the given key.
// It returns an error if the file cannot be opened or written to.
func (fops fileOps) Put(ctx context.Context, key string, entry []byte) error {
	path := filepath.Join(fops.location, key)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: opening file %s", key)), err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Debug("closing file", "error", cerr)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: getting file info %s", key)), err)
	}

	if stat.Size() > 0 {
		entry = append([]byte("\n"), entry...)
	}

	if _, err = file.Write(entry); err != nil {
		return fmt.Errorf("%w: %w", KeyError(fmt.Sprintf("file: writing to file %s", key)), err)
	}
	return nil
}

// Delete deletes the file with the given key.
// It returns an error if the file cannot be deleted.
func (fops fileOps) Delete(ctx context.Context, key string) error {
	path := filepath.Join(fops.location, key)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return KeyNotFoundError(fmt.Sprintf("file: key not found %s", key))
		}
		return fmt.Errorf("%w: %w", LocationError(fmt.Sprintf("file: deleting file %s", key)), err)
	}
	return nil
}

// List lists all regular files in the directory.
// It returns a slice of file names or an error if the directory cannot be read.
func (fops fileOps) List(ctx context.Context) ([]string, error) {
	var res []string
	err := filepath.WalkDir(fops.location, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%w: %w", LocationError(fmt.Sprintf("file: walking directory %s", path)), err)
		}
		if d.Type().IsRegular() {
			res = append(res, d.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
