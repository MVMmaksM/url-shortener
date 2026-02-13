package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/MVMmaksM/url-shortener/cmd/internal/storage"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	fn := "storage.sqlite.new"
	db, err := sql.Open("sqlite", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}

	stmt, err := db.Prepare(`
		CREATE TABLE IF NOT EXISTS url(
		id INTEGER PRIMARY KEY,
		alias TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL UNIQUE);
		CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}

	_, err = stmt.Exec()

	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}

	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) SaveURL(urlToSave string, alias string) (int64, error) {
	fn := "storage.sqlite.saveurl"

	stmt, err := s.db.Prepare(`INSERT INTO url(url, alias) VALUES (?,?);`)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", fn, err)
	}

	result, err := stmt.Exec(urlToSave, alias)
	if err != nil {
		if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
			return 0, fmt.Errorf("%s: %w", fn, storage.ErrURLExists)
		}

		return 0, fmt.Errorf("%s: %w", fn, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", fn, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	fn := "storage.sqlite.geturl"

	stmt, err := s.db.Prepare(`SELECT url FROM url WHERE alias = ?;`)
	if err != nil {
		return "", fmt.Errorf("%s: %w", fn, err)
	}

	var resUrl string
	err = stmt.QueryRow(alias).Scan(&resUrl)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%s: %w", fn, storage.ErrURLNotFound)
		}

		return "", fmt.Errorf("%s: %w", fn, err)
	}

	return resUrl, nil
}
