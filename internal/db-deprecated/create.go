package db

import "fmt"

func (h *Handler) HandleCreate(filename string, newTable *Table) error {
	// ReadJSON
	localData, err := ReadJSON(filename)
	if err != nil {
		return fmt.Errorf("failed to read JSON: %w", err)
	}

	// CheckTableExists
	if CheckTableExists(newTable.TableName, localData) {
		return fmt.Errorf("table %s already exists", newTable.TableName)
	}

	// PersistNewTable
	err = PersistNewTable(localData, filename, newTable)
	if err != nil {
		return fmt.Errorf("failed to persist new table: %w", err)
	}

	return nil
}
