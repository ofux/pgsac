package schema

import (
	"bytes"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"

	"github.com/ofux/pgsac/pkg/database"
)

// Extractor handles the extraction of schema information from the database
type Extractor struct {
	db     *sql.DB
	config database.Config
}

// NewExtractor creates a new schema extractor
func NewExtractor(db *sql.DB, config database.Config) *Extractor {
	return &Extractor{
		db:     db,
		config: config,
	}
}

// execPsql executes a psql command and returns its output
func (e *Extractor) execPsql(command string) (string, error) {
	args := []string{
		"-h", e.config.Host,
		"-p", fmt.Sprintf("%d", e.config.Port),
		"-U", e.config.User,
		"-d", e.config.DBName,
		"-c", command,
		"--no-align",    // Unaligned output mode
		"--tuples-only", // Print rows only
		"-q",            // Run quietly (no messages, only query output)
	}

	cmd := exec.Command("psql", args...)

	// Set PGPASSWORD environment variable
	cmd.Env = append(cmd.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.config.Password))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("psql error: %w\nstderr: %s", err, stderr.String())
	}

	return stdout.String(), nil
}

// ExtractSchemas extracts all objects from the specified schemas
func (e *Extractor) ExtractSchemas(schemaNames []string) ([]Schema, error) {
	var schemas []Schema
	for _, schemaName := range schemaNames {
		schema := Schema{Name: schemaName}

		// Extract tables
		tables, err := e.extractTables(schemaName)
		if err != nil {
			return nil, fmt.Errorf("error extracting tables from schema %s: %w", schemaName, err)
		}
		schema.Objects = append(schema.Objects, tables...)

		// Extract views
		views, err := e.extractViews(schemaName)
		if err != nil {
			return nil, fmt.Errorf("error extracting views from schema %s: %w", schemaName, err)
		}
		schema.Objects = append(schema.Objects, views...)

		// Extract materialized views
		matViews, err := e.extractMaterializedViews(schemaName)
		if err != nil {
			return nil, fmt.Errorf("error extracting materialized views from schema %s: %w", schemaName, err)
		}
		schema.Objects = append(schema.Objects, matViews...)

		// Extract functions
		functions, err := e.extractFunctions(schemaName)
		if err != nil {
			return nil, fmt.Errorf("error extracting functions from schema %s: %w", schemaName, err)
		}
		schema.Objects = append(schema.Objects, functions...)

		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (e *Extractor) extractTables(schemaName string) ([]Object, error) {
	// First, get the list of tables
	listCmd := fmt.Sprintf(`\dt %s.*`, schemaName)
	tableList, err := e.execPsql(listCmd)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}

	var objects []Object
	for _, line := range strings.Split(strings.TrimSpace(tableList), "\n") {
		if line == "" {
			continue
		}

		// Parse the table name from the output
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		tableName := fields[2]

		// Get the table definition
		defCmd := fmt.Sprintf(`\d+ %s.%s`, schemaName, tableName)
		definition, err := e.execPsql(defCmd)
		if err != nil {
			return nil, fmt.Errorf("error getting table definition for %s: %w", tableName, err)
		}

		obj := Object{
			Schema:     schemaName,
			Name:       tableName,
			Type:       TableType,
			Definition: definition,
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

func (e *Extractor) extractViews(schemaName string) ([]Object, error) {
	// List views
	listCmd := fmt.Sprintf(`\dv %s.*`, schemaName)
	viewList, err := e.execPsql(listCmd)
	if err != nil {
		return nil, fmt.Errorf("error listing views: %w", err)
	}

	var objects []Object
	for _, line := range strings.Split(strings.TrimSpace(viewList), "\n") {
		if line == "" {
			continue
		}

		// Parse the view name from the output
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		viewName := fields[2]

		// Get the view definition
		defCmd := fmt.Sprintf(`\d+ %s.%s`, schemaName, viewName)
		definition, err := e.execPsql(defCmd)
		if err != nil {
			return nil, fmt.Errorf("error getting view definition for %s: %w", viewName, err)
		}

		obj := Object{
			Schema:     schemaName,
			Name:       viewName,
			Type:       ViewType,
			Definition: definition,
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

func (e *Extractor) extractMaterializedViews(schemaName string) ([]Object, error) {
	// List materialized views
	listCmd := fmt.Sprintf(`\dm %s.*`, schemaName)
	matViewList, err := e.execPsql(listCmd)
	if err != nil {
		return nil, fmt.Errorf("error listing materialized views: %w", err)
	}

	var objects []Object
	for _, line := range strings.Split(strings.TrimSpace(matViewList), "\n") {
		if line == "" {
			continue
		}

		// Parse the materialized view name from the output
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		matViewName := fields[2]

		// Get the materialized view definition
		defCmd := fmt.Sprintf(`\d+ %s.%s`, schemaName, matViewName)
		definition, err := e.execPsql(defCmd)
		if err != nil {
			return nil, fmt.Errorf("error getting materialized view definition for %s: %w", matViewName, err)
		}

		obj := Object{
			Schema:     schemaName,
			Name:       matViewName,
			Type:       MaterializedView,
			Definition: definition,
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

func (e *Extractor) extractFunctions(schemaName string) ([]Object, error) {
	// List functions
	listCmd := fmt.Sprintf(`\df %s.*`, schemaName)
	funcList, err := e.execPsql(listCmd)
	fmt.Println(funcList)
	if err != nil {
		return nil, fmt.Errorf("error listing functions: %w", err)
	}

	var objects []Object
	for _, line := range strings.Split(strings.TrimSpace(funcList), "\n") {
		if line == "" {
			continue
		}

		// Parse the function name from the output
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		funcName := fields[2]

		// Get the function definition
		defCmd := fmt.Sprintf(`\sf %s.%s`, schemaName, funcName)
		definition, err := e.execPsql(defCmd)
		if err != nil {
			return nil, fmt.Errorf("error getting function definition for %s: %w", funcName, err)
		}

		obj := Object{
			Schema:     schemaName,
			Name:       funcName,
			Type:       FunctionType,
			Definition: definition,
		}
		objects = append(objects, obj)
	}

	return objects, nil
}
