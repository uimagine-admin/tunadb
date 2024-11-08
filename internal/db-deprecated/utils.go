package db

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

func ReadJSON(filename string) (LocalData, error) {
	var localData LocalData

	jsonFile, err := os.Open(filename)
	// if os.Open returns an error then handle it
	if err != nil {
		log.Printf("Error reading JSON file: %s\n", err.Error())
		return LocalData{}, err
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Printf("Error reading file content: %s\n", err.Error())
		return LocalData{}, err
	}

	err = json.Unmarshal(byteValue, &localData)
	if err != nil {
		log.Printf("Error unmarshalling JSON: %s\n", err.Error())
		return LocalData{}, err
	}
	return localData, nil
}

func CheckTableExists(tableName string, data LocalData) bool {
	for _, table := range data {
		if table.TableName == tableName {
			return true
		}
	}
	return false
}

func GetTable(tableName string, data LocalData) *Table {
	for _, table := range data {
		if table.TableName == tableName {
			return table
		}
	}
	return nil
}

func GetPartition(table *Table, hashedPK int64) *Partition {
	for _, partition := range table.Partitions {
		if partition.Metadata.PartitionKey == hashedPK {
			return partition
		}
	}
	return nil
}

func PersistNewTable(data LocalData, filename string, table *Table) error {
	data = append(data, table)
	// MarshalIndent instead of Marshal for legibility during debug
	jsonFile, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		log.Printf("Error in marshalling data: %s\n", err.Error())
		return err
	}
	// Set permission to readable by all, writable by user
	err = os.WriteFile(filename, jsonFile, 0644)
	if err != nil {
		log.Printf("Error in writing file: %s\n", err.Error())
		return err
	}
	log.Println("Successfully persisted table")
	return nil
}

func PersistTable(data LocalData, filename string, table *Table) error {
	for i, tableData := range data {
		if tableData.TableName == table.TableName {
			// Update the existing table data
			data[i] = table
		}
	}
	jsonFile, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		log.Printf("Error in marshalling data: %s\n", err.Error())
		return err
	}

	err = os.WriteFile(filename, jsonFile, 0644)
	if err != nil {
		log.Printf("Error in writing file: %s\n", err.Error())
		return err
	}
	log.Println("Successfully persisted table")
	return nil
}
