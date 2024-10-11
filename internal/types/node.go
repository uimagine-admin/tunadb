package types

import "fmt"

type Node struct {
	IPAddress string
	id        string
	Port      string
	Name      string
}

// for printing purposes and debug
func (n *Node) String() string {
	return fmt.Sprintf("Node(ID = %s) with (PORT= %s) with IP = %s, with Name = %s", n.id, n.Port, n.IPAddress, n.Name)
}
