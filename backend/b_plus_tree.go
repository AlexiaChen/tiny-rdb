package backend

import (
	"fmt"
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
	TypeInternalNode = iota
	TypeLeafNode     = iota
)

// NodeType typdef B+tree node type
type NodeType = uint8

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
	SetNodeType(node, TypeLeafNode)
	var numCells *uint32 = LeafNodeNumCells(node)
	*numCells = 0
}

// SetNodeType Set the type of node
func SetNodeType(node []byte, nodeType NodeType) {
	typet := (*NodeType)(unsafe.Pointer(&node[NodeTypeOffset]))
	*typet = nodeType
}

// GetNodeType Get the type of node
func GetNodeType(node []byte) NodeType {
	typet := (*NodeType)(unsafe.Pointer(&node[NodeTypeOffset]))
	return *typet
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

// SplitAndInsertLeafNode split a leaf node in two nodes. And after that, we need to create an internal node to act as a parent node for the two leaf nodes.
// If there is no space on the leaf node, we would split the existing entries residing there and the new one (being inserted) into two equal halves:
// lower and upper halves. (Keys on the upper half are strictly greater than those on the lower half.) We allocate a new leaf node, and move the upper half into the new node.
func SplitAndInsertLeafNode(cursor *Cursor, key uint32, value *Row) {
	// Create a new node and move half the cells over.
	// Insert the new value in one of the two nodes.
	// Update parent or create a new parent.
	// var oldPage *Page = GetPage(cursor.TablePtr.Pager, cursor.PageNum)
	var newPageNum uint32 = GetUnallocatedPageNum(cursor.TablePtr.Pager)
	var newPage *Page = GetPage(cursor.TablePtr.Pager, newPageNum)
	InitializeLeafNode(newPage.Mem[:])

}

// InsertLeafNode Inserting a key/value pair into a leaf node.
// It will take a cursor as input to represent the position where the pair should be inserted.
func InsertLeafNode(cursor *Cursor, key uint32, value *Row) {
	var page *Page = GetPage(cursor.TablePtr.Pager, cursor.PageNum)
	var numCells uint32 = *LeafNodeNumCells(page.Mem[:])
	if numCells >= LeafNodeMaxCells {
		// Leaf node full, need to split
		SplitAndInsertLeafNode(cursor, key, value)
		return
	}

	if cursor.CellNum < numCells {
		// Move rest of cells spaces for making a cell-size space
		for i := numCells; i > cursor.CellNum; i-- {
			copy(LeafNodeCell(page.Mem[:], i), LeafNodeCell(page.Mem[:], i-1))
		}
	}

	*LeafNodeKey(page.Mem[:], cursor.CellNum) = key
	SerializeRow(value, LeafNodeValue(page.Mem[:], cursor.CellNum))
	*LeafNodeNumCells(page.Mem[:]) = numCells + 1
}

// FindLeafNode Search the leaf node with binary search.
func FindLeafNode(table *Table, pageNum uint32, key uint32) *Cursor {
	var page *Page = GetPage(table.Pager, pageNum)
	var numCells uint32 = *LeafNodeNumCells(page.Mem[:])

	var cursor *Cursor = new(Cursor)
	cursor.TablePtr = table
	cursor.PageNum = pageNum

	// Binary Search
	var minIndex uint32 = 0
	var maxIndex uint32 = numCells
	for maxIndex != minIndex {
		var index uint32 = (minIndex + maxIndex) / 2
		var indexKey uint32 = *LeafNodeKey(page.Mem[:], index)
		if indexKey == key {
			cursor.CellNum = index
			return cursor
		}

		if key < indexKey {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.CellNum = minIndex
	return cursor
}

// PrintLeafNode Print detailed info from leaf node binary
func PrintLeafNode(node []byte) uint32 {
	var numCells uint32 = *LeafNodeNumCells(node)
	fmt.Printf("Leaf num of cells: %v\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		fmt.Printf("(cell num: %v, key: %v)\n", i, *LeafNodeKey(node, i))
	}
	return numCells
}
