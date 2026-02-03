package storage

import (
	"fmt"
	"regexp"
	"time"
)

// Info: Separate file for Symbol Registration logic specific to Postgres

// SymbolMetadata defines the structure for symbol registration
type SymbolMetadata struct {
	Symbol     string
	Type       string // "classic" or "postgres_ref"
	RefSchema  string
	RefTable   string
	RefField   string
	SourceName string
}

// -----------------------------------------------------------------------------

// FilterAndRegisterSymbols processes raw symbols, separates Postgres refs, registers all to DB, and returns classic symbols.
func (d *PostgresDB) FilterAndRegisterSymbols(sourceName string, rawSymbols []string) ([]string, error) {
	pgSymbolRegex := regexp.MustCompile(`^(\w+)\.(\w+)\.(\w+)$`)

	var classicSymbols []string
	var pgSymbols []SymbolMetadata

	// 1. Separate Symbols
	for _, sym := range rawSymbols {
		matches := pgSymbolRegex.FindStringSubmatch(sym)
		if len(matches) == 4 {
			// It's a postgres ref (schema.table.field)
			ref := SymbolMetadata{
				Symbol:     sym,
				Type:       "postgres_ref",
				RefSchema:  matches[1],
				RefTable:   matches[2],
				RefField:   matches[3],
				SourceName: sourceName,
			}
			pgSymbols = append(pgSymbols, ref)

			// Load symbols from this reference
			loadedSymbols, err := d.GetSymbolsFromTable(ref.RefSchema, ref.RefTable, ref.RefField)
			if err != nil {
				// We log error but proceed? Or return error?
				// Since we can't log easily here without logger, let's wrap error
				return classicSymbols, fmt.Errorf("failed to load symbols from %s: %w", sym, err)
			}

			// Merge loaded symbols
			for _, loadedSym := range loadedSymbols {
				classicSymbols = append(classicSymbols, loadedSym)
				pgSymbols = append(pgSymbols, SymbolMetadata{
					Symbol:     loadedSym,
					Type:       "classic",
					SourceName: sourceName,
				})
			}

		} else {
			// Classic
			classicSymbols = append(classicSymbols, sym)
			pgSymbols = append(pgSymbols, SymbolMetadata{
				Symbol:     sym,
				Type:       "classic",
				SourceName: sourceName,
			})
		}
	}

	// 2. Register to Postgres
	if err := d.RegisterSymbols(pgSymbols); err != nil {
		return classicSymbols, fmt.Errorf("failed to register symbols: %w", err)
	}

	return classicSymbols, nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) RegisterSymbols(symbols []SymbolMetadata) error {
	if len(symbols) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tableName := fmt.Sprintf(`"%s"."symbols"`, d.Schema)
	query := fmt.Sprintf(`
		INSERT INTO %s (symbol, type, ref_schema, ref_table, ref_field, source_name, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (symbol) DO UPDATE SET
			type = EXCLUDED.type,
			ref_schema = EXCLUDED.ref_schema,
			ref_table = EXCLUDED.ref_table,
			ref_field = EXCLUDED.ref_field,
			source_name = EXCLUDED.source_name,
			updated_at = EXCLUDED.updated_at
	`, tableName)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, s := range symbols {
		_, err := stmt.Exec(s.Symbol, s.Type, s.RefSchema, s.RefTable, s.RefField, s.SourceName, time.Now().UTC())
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) GetSymbolsFromTable(schema, table, field string) ([]string, error) {
	// 1. Validate inputs to prevent basic SQL injection if regex didn't catch weirdness
	// Since regex \w+ allows alphanumeric and underscore, it is relatively safe for identifiers if quoted.

	query := fmt.Sprintf(`SELECT "%s" FROM "%s"."%s"`, field, schema, table)

	rows, err := d.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s != "" {
			symbols = append(symbols, s)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return symbols, nil
}
