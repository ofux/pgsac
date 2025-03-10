package schema

import (
	"database/sql"
	"fmt"
)

// Extractor handles the extraction of schema information from the database
type Extractor struct {
	db *sql.DB
}

// NewExtractor creates a new schema extractor
func NewExtractor(db *sql.DB) *Extractor {
	return &Extractor{db: db}
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
	query := `
		SELECT table_name,
			   pg_get_tabledef(format('%I.%I', schemaname, tablename)) as definition
		FROM pg_tables
		WHERE schemaname = $1
		ORDER BY table_name;
	`

	rows, err := e.db.Query(query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		obj.Schema = schemaName
		obj.Type = TableType
		if err := rows.Scan(&obj.Name, &obj.Definition); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

func (e *Extractor) extractViews(schemaName string) ([]Object, error) {
	query := `
		SELECT viewname,
			   pg_get_viewdef(format('%I.%I', schemaname, viewname), true) as definition
		FROM pg_views
		WHERE schemaname = $1
		ORDER BY viewname;
	`

	rows, err := e.db.Query(query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		obj.Schema = schemaName
		obj.Type = ViewType
		if err := rows.Scan(&obj.Name, &obj.Definition); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

func (e *Extractor) extractMaterializedViews(schemaName string) ([]Object, error) {
	query := `
		SELECT matviewname,
			   pg_get_viewdef(format('%I.%I', schemaname, matviewname), true) as definition
		FROM pg_matviews
		WHERE schemaname = $1
		ORDER BY matviewname;
	`

	rows, err := e.db.Query(query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		obj.Schema = schemaName
		obj.Type = MaterializedView
		if err := rows.Scan(&obj.Name, &obj.Definition); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

func (e *Extractor) extractFunctions(schemaName string) ([]Object, error) {
	query := `
		SELECT p.proname,
			   pg_get_functiondef(p.oid) as definition
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = $1
		ORDER BY p.proname;
	`

	rows, err := e.db.Query(query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		obj.Schema = schemaName
		obj.Type = FunctionType
		if err := rows.Scan(&obj.Name, &obj.Definition); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}
