package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"pgsac/pkg/schema"
)

// Exporter handles the export of schema objects to files
type Exporter struct {
	baseDir string
}

// NewExporter creates a new exporter
func NewExporter(baseDir string) *Exporter {
	return &Exporter{baseDir: baseDir}
}

// Export writes all schema objects to files
func (e *Exporter) Export(schemas []schema.Schema) error {
	for _, s := range schemas {
		if err := e.exportSchema(s); err != nil {
			return fmt.Errorf("error exporting schema %s: %w", s.Name, err)
		}
	}
	return nil
}

func (e *Exporter) exportSchema(s schema.Schema) error {
	// Create schema directory if it doesn't exist
	schemaDir := filepath.Join(e.baseDir, s.Name)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return fmt.Errorf("error creating schema directory: %w", err)
	}

	// Group objects by type
	objectsByType := make(map[schema.ObjectType][]schema.Object)
	for _, obj := range s.Objects {
		objectsByType[obj.Type] = append(objectsByType[obj.Type], obj)
	}

	// Export each type of object
	for objType, objects := range objectsByType {
		typeDir := filepath.Join(schemaDir, string(objType))
		if err := os.MkdirAll(typeDir, 0755); err != nil {
			return fmt.Errorf("error creating type directory: %w", err)
		}

		for _, obj := range objects {
			if err := e.exportObject(typeDir, obj); err != nil {
				return fmt.Errorf("error exporting object %s: %w", obj.Name, err)
			}
		}
	}

	return nil
}

func (e *Exporter) exportObject(typeDir string, obj schema.Object) error {
	// Create file name with .sql extension
	fileName := fmt.Sprintf("%s.sql", obj.Name)
	filePath := filepath.Join(typeDir, fileName)

	// Create file
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer f.Close()

	// Write header comment
	header := fmt.Sprintf("-- Object: %s.%s\n-- Type: %s\n\n", obj.Schema, obj.Name, obj.Type)
	if _, err := f.WriteString(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write definition
	definition := strings.TrimSpace(obj.Definition) + ";\n"
	if _, err := f.WriteString(definition); err != nil {
		return fmt.Errorf("error writing definition: %w", err)
	}

	return nil
}
