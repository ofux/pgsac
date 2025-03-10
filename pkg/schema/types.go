package schema

// ObjectType represents the type of database object
type ObjectType string

const (
	TableType        ObjectType = "table"
	ViewType         ObjectType = "view"
	MaterializedView ObjectType = "materialized_view"
	FunctionType     ObjectType = "function"
)

// Object represents a database object (table, view, materialized view, or function)
type Object struct {
	Schema     string
	Name       string
	Type       ObjectType
	Definition string
	Depends    []string // Names of objects this object depends on
}

// Schema represents a database schema and its objects
type Schema struct {
	Name    string
	Objects []Object
}
