# PGSAC (PostgreSQL Schema As Code)

A CLI tool to extract and manage PostgreSQL database schemas as code.

## Features

- Extract database schema information from PostgreSQL databases
- Generate SQL DDL files organized by schema and object type:
  - Tables
  - Views
  - Materialized Views
  - Functions
- Each database object is stored in its own file for better version control and management

## Installation

```bash
go install github.com/ofux/pgsac/cmd/pgsac@latest
```

## Usage

```bash
# Extract schema from a database
pgsac extract --host localhost --port 5432 --dbname mydb --user myuser --output ./schemas

# More commands coming soon...
```

## Project Structure

```
.
├── cmd/pgsac        # Main CLI application
├── pkg/
│   ├── database/    # Database connection and queries
│   ├── schema/      # Schema models and operations
│   └── exporter/    # SQL file generation and organization
```

## License

MIT License 