package backend

import (
	"fmt"
	"os"
	"tiny-rdb/util"
	"unsafe"
)

// Difference between B Tree and B+ Tree： http://www.differencebetween.info/difference-between-b-tree-and-b-plus-tree

// B+tree nodes with children are called “internal” nodes. Internal nodes and leaf nodes are structured differently:

// For an order-m tree… 	Internal Node 	                   Leaf Node
// Stores 	              keys and pointers to children 	keys and values
// Number of keys 	          up to m-1 	                as many as will fit
// Number of pointers 	  number of keys + 1 	                  none
// Number of values 	       none 	                      number of keys
// Key purpose 	          used for routing 	                 paired with value
// Stores values? 	            No 	                               Yes

// 1. An empty B-tree has a single node: the root node. The root node starts as a leaf node with zero key/value pairs
// 2. If we insert a couple key/value pairs, they are stored in the leaf node in sorted order.
// 3. The depth of the tree only increases when we split the root node. Every leaf node has the same depth and close to the same number of key/value pairs,
//    so the tree remains balanced and quick to search.

// Each node will correspond to one page. The root node will exist in page 0.
// Child pointers will simply be the page number that contains the child node.

// B+tree node type, Leaf nodes and internal nodes have different layouts.
const (
	InternalNode = iota
	LeafNode     = iota
)

// NodeType typdef B+tree node type
type NodeType = int

// const one page size is equal to node size
const (
	NodeSize = PageSize // 4k bytes
)

// Node Header Format, Nodes need to store some metadata in a header at the beginning of the page.
// Every node will store what type of node it is, whether or not it is the root node, and a pointer to its parent(to allow finding a node’s siblings)
const (
	NodeTypeSize            = 1 // 1 byte
	NodeTypeOffset          = 0
	IsRootNodeSize          = 1 // 1 byte
	IsRootNodeOffset        = NodeTypeSize
	ParentNodePointerSize   = 4 // 4 bytes
	ParentNodePointerOffset = IsRootNodeOffset + IsRootNodeSize
	NodeHeaderSize          = NodeTypeSize + IsRootNodeSize + ParentNodePointerSize
)

// Leaf Node Header Format  leaf nodes need to store how many “cells” they contain. A cell is a key/value pair. Value is actual row data
const (
	LeafNodeCellsNumSize   = 4 // 4 bytes
	LeafNodeCellsNumOffset = NodeHeaderSize
	LeafNodeHeaderSize     = NodeHeaderSize + LeafNodeCellsNumSize
)

// Leaf Node Body Format. The body of a leaf node is an array of cells. Each cell is a key followed by a value (a serialized row).
const (
	LeafNodeKeySize        = 4 // 4 bytes
	LeafNodeKeyOffset      = 0
	LeafNodeValueSize      = RowSize
	LeafNodeValueOffset    = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize       = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeCellsSpaceSize = NodeSize - LeafNodeHeaderSize
	LeafNodeMaxCells       = LeafNodeCellsSpaceSize / LeafNodeCellSize
)

// Leaf node layout schema

// #__byte 0__#__byte 1__#_________________byte 2-5_________________#_________________byte 6-9_________________#
// byte 0: NodeType(1 byte), byte 1: IsRootNode(1 byte), byte 2-5:ParentNodePointer(4 bytes), byte 6-9: LeafNodeCellsNum(4 bytes)
// #_________________byte 10-13_________________#___________________________________________byte 14-305_____________________________________________________________#
// byte 10-13: Key0(4 bytes), byte 14-306: Value0(292 BYTES)
// ............
// ............
// Cell format layout repeat until LeafNodeCellsNum like above
// ............
// ............
// #_________________byte 3562-3565_________________#___________________________________________byte 3566-3857_____________________________________________________________#
// byte 3562-3565: Key12(4 bytes), byte 3566-3857: Value12(292)
// #_________________________________________________byte 3858-4095______________________________________________________#
// byte 3858-4095: specific-byte(0x00) filled space (leave it empty to avoid splitting cells between nodes)

// Accessing Leaf node, setter and getter for leaf node

// InitializeLeafNode Initialize Leaf node
func InitializeLeafNode(node []byte) {
	var numCells *uint32 = LeafNodeNumCells(node)
	*numCells = 0
}

// LeafNodeNumCells Get Number of cells in leaf node
func LeafNodeNumCells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LeafNodeCellsNumOffset]))
}

// LeafNodeCell Get specific cell bytes array in Leaf node
func LeafNodeCell(node []byte, cellNum uint32) []byte {
	var offsetCell uint32 = LeafNodeHeaderSize + LeafNodeCellSize*cellNum
	return node[offsetCell : offsetCell+LeafNodeCellSize]
}

// LeafNodeKey Get specific cell key in leaf node
func LeafNodeKey(node []byte, cellNum uint32) *uint32 {
	cellSlice := LeafNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cellSlice[0]))
}

// LeafNodeValue Get specific cell value in leaf node
func LeafNodeValue(node []byte, cellNum uint32) []byte {
	cellSlice := LeafNodeCell(node, cellNum)
	return cellSlice[LeafNodeKeySize:]
}

// InsertLeafNode Inserting a key/value pair into a leaf node.
// It will take a cursor as input to represent the position where the pair should be inserted.
func InsertLeafNode(cursor *Cursor, key uint32, value *Row) {
	var page *Page = GetPage(cursor.table.Pager, cursor.pageNum)
	var numCells uint32 = *LeafNodeNumCells(page.Mem[:])
	if numCells >= LeafNodeMaxCells {
		fmt.Println("Need to implemented splitting node")
		os.Exit(util.ExitFailure)
	}

	if cursor.cellNum < numCells {
		// Move rest of cells spaces for making a cell-size space
		for i := numCells; i > cursor.cellNum; i-- {
			copy(LeafNodeCell(page.Mem[:], i), LeafNodeCell(page.Mem[:], i-1))
		}
	}

	*LeafNodeKey(page.Mem[:], cursor.cellNum) = key
	SerializeRow(value, LeafNodeValue(page.Mem[:], cursor.cellNum))
	*LeafNodeNumCells(page.Mem[:]) = numCells + 1
}
