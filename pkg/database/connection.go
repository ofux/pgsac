package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// Config holds the database connection configuration
type Config struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
}

// Connect establishes a connection to the PostgreSQL database
func Connect(config Config) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		config.Host,
		config.Port,
		config.DBName,
		config.User,
		config.Password,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	return db, nil
}
