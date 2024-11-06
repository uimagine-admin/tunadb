package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uimagine-admin/tunadb/internal/types"
)

// Handler is for each individual node to handle local read/write to file
type Handler struct {
	Node *types.Node
}

// EpochTime defines a timestamp encoded as epoch nanoseconds in JSON
type EpochTime time.Time

type LocalData []*Table
type Table struct {
	TableName         string       `json:"table_name"`
	PartitionKeyNames []string     `json:"partition_key_names"`
	Partitions        []*Partition `json:"partitions"`
}

type Partition struct {
	Metadata *Metadata `json:"partition_metadata"`
	Rows     []*Row    `json:"rows"`
}

/*
	Metadata

PartitionKey: hash value of the concatenated partition keys
PartitionKeyValues: values of the table's partition keys
*/
type Metadata struct {
	PartitionKey       int64    `json:"partition_key"`
	PartitionKeyValues []string `json:"partition_key_values"`
}

type Row struct {
	CreatedAt EpochTime `json:"created_at"`
	UpdatedAt EpochTime `json:"updated_at"`
	DeletedAt EpochTime `json:"deleted_at"`
	Cells     []*Cell   `json:"cells"`
}

type Cell struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (ld LocalData) String() string {
	var sb strings.Builder
	for _, table := range ld {
		sb.WriteString(fmt.Sprintf("Table: %s\n", table.TableName))
		sb.WriteString(fmt.Sprintf("Partition Keys: %v\n", table.PartitionKeyNames))
		for _, partition := range table.Partitions {
			sb.WriteString(fmt.Sprintf("  Partition Key: %d\n", partition.Metadata.PartitionKey))
			sb.WriteString(fmt.Sprintf("  Partition Key Values: %v\n", partition.Metadata.PartitionKeyValues))
			for _, row := range partition.Rows {
				sb.WriteString(fmt.Sprintf("    Row Created At: %s\n", row.CreatedAt))
				sb.WriteString(fmt.Sprintf("    Row Updated At: %s\n", row.UpdatedAt))
				sb.WriteString(fmt.Sprintf("    Row Deleted At: %s\n", row.DeletedAt))
				for _, cell := range row.Cells {
					sb.WriteString(fmt.Sprintf("      Cell Name: %s, Cell Value: %s\n", cell.Name, cell.Value))
				}
			}
		}
	}
	return sb.String()
}

// MarshalJSON is used to convert the timestamp to JSON
func (t EpochTime) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(time.Time(t).UnixNano(), 10)), nil
}

// UnmarshalJSON is used to convert the timestamp from JSON
func (t *EpochTime) UnmarshalJSON(s []byte) (err error) {
	r := string(s)
	q, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		return err
	}
	*(*time.Time)(t) = time.Unix(0, q)
	return nil
}

// Unix returns t as a Unix time, the number of seconds elapsed
// since January 1, 1970 UTC. The result does not depend on the
// location associated with t.
func (t EpochTime) Unix() int64 {
	return time.Time(t).Unix()
}

// UnixNano returns t as a Unix time, the number of nanoseconds elapsed
// since January 1, 1970 UTC. The result does not depend on the
// location associated with t.
func (t EpochTime) UnixNano() int64 {
	return time.Time(t).UnixNano()
}

// Time returns the JSON time as a time.Time instance in UTC
func (t EpochTime) Time() time.Time {
	return time.Time(t).UTC()
}

// String returns t as a formatted string
func (t EpochTime) String() string {
	return t.Time().String()
}

type ReadResponse struct {
	SourceNode *types.Node
	Rows       []*Row
}
