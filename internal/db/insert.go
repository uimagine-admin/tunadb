package db

import (
	"fmt"
	"time"
)

func (h *Handler) HandleInsert(tableName string, partitionKey int64, partitionKeyValues []string, newCells []Cell) error {
	filename := fmt.Sprintf("data/%s.json", h.Node.ID)
	localData, err := ReadJSON(filename)
	if err != nil {
		return fmt.Errorf("Failed to read JSON: %v", err)
	}
	table := GetTable(tableName, localData)
	if table == nil {
		err := fmt.Sprintf("Table %s does not exist.", tableName)
		return fmt.Errorf("Failed to insert: %s", err)
	}
	updatedPartition := GetPartition(table, partitionKey)
	if updatedPartition == nil {
		if err = createNewPartition(partitionKey, partitionKeyValues, table, newCells); err != nil {
			return err
		}
	} else {
		if err = updatePartition(updatedPartition, newCells); err != nil {
			return err
		}
	}
	if err = PersistTable(localData, filename, table); err != nil {
		return err
	}

	return nil
}

func createNewPartition(partitionKey int64, partitionKeyValues []string, table *Table, newCells []Cell) error {
	metadata := &Metadata{
		PartitionKey:       partitionKey,
		PartitionKeyValues: partitionKeyValues,
	}
	rows := make([]*Row, 0)
	cells := make([]*Cell, 0)
	for i := range newCells {
		cell := &Cell{
			Name:  newCells[i].Name,
			Value: newCells[i].Value,
		}
		cells = append(cells, cell)
	}
	row := &Row{
		CreatedAt: EpochTime(time.Now()),
		Cells:     cells,
	}
	rows = append(rows, row)
	partition := &Partition{
		Metadata: metadata,
		Rows:     rows,
	}
	table.Partitions = append(table.Partitions, partition)
	return nil
}

func updatePartition(partition *Partition, newCells []Cell) error {
	cells := make([]*Cell, 0)
	for i := range newCells {
		cell := &Cell{
			Name:  newCells[i].Name,
			Value: newCells[i].Value,
		}
		cells = append(cells, cell)
	}
	newRow := &Row{
		CreatedAt: EpochTime(time.Now()),
		Cells:     cells,
	}
	partition.Rows = append(partition.Rows, newRow)
	return nil
}
