package main

import (
	"fmt"
	"os"

	"github.com/ofux/pgsac/pkg/database"
	"github.com/ofux/pgsac/pkg/exporter"
	"github.com/ofux/pgsac/pkg/schema"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pgsac",
	Short: "PostgreSQL Schema As Code - A tool to manage database schemas",
	Long: `PGSAC is a CLI tool that helps you manage PostgreSQL database schemas as code.
It extracts schema information and generates SQL DDL files organized by schema and object type.`,
}

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Extract schema from a PostgreSQL database",
	Long: `Extract schema information from a PostgreSQL database and generate SQL DDL files.
Each database object (table, view, materialized view, function) will be stored in its own file,
organized by schema and object type.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		dbname, _ := cmd.Flags().GetString("dbname")
		user, _ := cmd.Flags().GetString("user")
		password, _ := cmd.Flags().GetString("password")
		sslmode, _ := cmd.Flags().GetString("sslmode")
		output, _ := cmd.Flags().GetString("output")
		schemas, _ := cmd.Flags().GetStringSlice("schemas")

		// Create database connection
		config := database.Config{
			Host:     host,
			Port:     port,
			DBName:   dbname,
			User:     user,
			Password: password,
			SSLMode:  sslmode,
		}

		db, err := database.Connect(config)
		if err != nil {
			return fmt.Errorf("error connecting to database: %w", err)
		}
		defer db.Close()

		// Extract schemas
		extractor := schema.NewExtractor(db)
		extractedSchemas, err := extractor.ExtractSchemas(schemas)
		if err != nil {
			return fmt.Errorf("error extracting schemas: %w", err)
		}

		// Export to files
		exp := exporter.NewExporter(output)
		if err := exp.Export(extractedSchemas); err != nil {
			return fmt.Errorf("error exporting schemas: %w", err)
		}

		fmt.Printf("Successfully exported %d schemas to %s\n", len(extractedSchemas), output)
		return nil
	},
}

func init() {
	// Extract command flags
	extractCmd.Flags().StringP("host", "H", "localhost", "Database host")
	extractCmd.Flags().IntP("port", "p", 5432, "Database port")
	extractCmd.Flags().StringP("dbname", "d", "", "Database name")
	extractCmd.Flags().StringP("user", "u", "", "Database user")
	extractCmd.Flags().StringP("password", "P", "", "Database password")
	extractCmd.Flags().String("sslmode", "disable", "SSL mode (disable, require, verify-ca, verify-full)")
	extractCmd.Flags().StringP("output", "o", "./schemas", "Output directory for SQL files")
	extractCmd.Flags().StringSliceP("schemas", "s", []string{"public"}, "Schemas to extract (comma-separated)")

	// Mark required flags
	extractCmd.MarkFlagRequired("dbname")
	extractCmd.MarkFlagRequired("user")

	// Add commands to root
	rootCmd.AddCommand(extractCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
