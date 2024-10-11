package types

import "fmt"

type Node struct {
	IPAddress string
	ID        string
	Port      uint64
	Name      string
}

// for printing purposes and debug
func (n *Node) String() string {
	return fmt.Sprintf("Node(ID = %s) with (PORT= %d) with IP = %s, with Name = %s", n.ID, n.Port, n.IPAddress, n.Name)
}
