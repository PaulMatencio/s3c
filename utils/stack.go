package utils

import "fmt"

// Stack's node
type Node struct {
	Address int64
}

// Stack is a basic LIFO stack that resizes as needed.
type Stack struct {
	Nodes []*Node
	Count int
}


func (n *Node)  String()  string {
	  return fmt.Sprintf("%s",n.Address)
}

func (n *Node)  HexString()  string {
	return fmt.Sprintf("%x",n.Address)
}

// Push -> adds a node to the stack
func (s *Stack) Push(n *Node) {
	s.Nodes = append(s.Nodes[:s.Count], n)
	s.Count++
}

// Pop removes and returns a node from the stack in last to first order.

func (s *Stack) Pop() *Node {
	if s.Count == 0 {
		return nil
	}
	s.Count--
	return s.Nodes[s.Count]
}

func NewStack(size int) Stack {
	return  Stack {
		Nodes:  make([]*Node,size),
		Count: size,
	}
}

func DelStack(s Stack) {
	s.Nodes = nil
	s.Count = 0
}