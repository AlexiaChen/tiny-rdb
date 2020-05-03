package backend

import (
	"fmt"
	"os"
	"tiny-rdb/util"
	"unsafe"
)

// Difference between B-Tree and B+Tree： http://www.differencebetween.info/difference-between-b-tree-and-b-plus-tree

// B-tree nodes with children are called “internal” nodes. Internal nodes and leaf nodes are structured differently:

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

// B-tree node type, Leaf nodes and internal nodes have different layouts.
const (
	TypeInternalNode = iota
	TypeLeafNode     = iota
)

// NodeType typdef B-tree node type
type NodeType = uint8

// If a leaf node can hold Max cells, then during a split we need to distribute Max+1 cells between two nodes (Max original cells plus one new one)
const (
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSplitCount  = (LeafNodeMaxCells + 1) - LeafNodeRightSplitCount //  choosing the left node to get one more cell if Max+1 is odd.
)

// const one page size is equal to node size
const (
	NodeSize = PageSize // 4k bytes
)

// Common Node Header Format
// Nodes need to store some metadata in a header at the beginning of the page.
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

// Internal Node Header Format
// It starts with the common header, then the number of keys it contains, then the page number of its rightmost child.
// Internal nodes always have one more child pointer than they have keys. That extra child pointer is stored in the header.
const (
	InternalNodeNumKeysSize      = 4 // 4 bytes
	InternalNodeNumKeysOffset    = NodeHeaderSize
	InternalNodeRightChildSize   = 4 // 4 bytes
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       = NodeHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

// Interal Node Body Format
// The body is an array of cells where each cell contains a child pointer and a key. Every key should be the maximum key contained in the child to its left.
const (
	InternalNodeKeySize   = 4 // 4 bytes
	InternalNodeChildSize = 4 // 4 bytes
	InternalNodeCellSize  = InternalNodeKeySize + InternalNodeChildSize
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
// Leaf node cell format layout repeat until LeafNodeCellsNum like above
// ............
// ............
// #_________________byte 3562-3565_________________#___________________________________________byte 3566-3857_____________________________________________________________#
// byte 3562-3565: Key12(4 bytes), byte 3566-3857: Value12(292)
// #_________________________________________________byte 3858-4095______________________________________________________#
// byte 3858-4095: specific-byte(0x00) filled space (leave it empty to avoid splitting cells between nodes)

// #########################################################################################################################################################################

// Internal node layout schema

// #__byte 0__#__byte 1__#_________________byte 2-5_________________#_________________byte 6-9_________________#_________________byte 10-13_________________#
// byte 0: NodeType(1 byte), byte 1: IsRootNode(1 byte), byte 2-5:ParentNodePointer(4 bytes), byte 6-9: InteranlNodeKeysNum(4 bytes), byte 10-13: RightChildPointer(4 bytes)
// #_________________byte 14-17_________________#_________________byte 18-21_________________#
// byte 14-17: ChildPointer0(4 bytes), byte 18-21: Key0(4 bytes)
// #_________________byte 22-25_________________#_________________byte 26-29_________________#
// byte 14-17: ChildPointer1(4 bytes), byte 18-21: Key1(4 bytes)
// ............
// ............
// Internal node cell format layout repeat until InteranlNodeKeysNum like above
// ............
// ............
// #_________________byte 4086-4089_________________#_________________byte 4090-4093_________________#
// byte 4086-4089: ChildPointer509(4 bytes), byte 4090-4093: Key509(4 bytes)
// #_________________________________________________byte 4094-4095______________________________________________________#
// byte 4094-4095: specific-byte(0x00) filled space (2 bytes)

// Notice our huge branching factor. Because each child pointer / key pair is so small
// it can fit 510 keys and 511 child pointers in each internal node.
// That means it never have to traverse many layers of the tree to find a given key.

// Internal node layers             max of leaf nodes        size of all leaf nodes
//       0                               511^0=1                      4kB
//       1                               511^1=511                 511 * 4k = 2MB
//       2                               511^2=261121              1020MB = 1GB
//       3                               511^3=133432831           510GB
//       N                               511^N                     (511)^N * 4kB

// In actuality, It can’t store a full 4 KB of data per leaf node due to the overhead of the header, keys, and wasted space.
// But it can search through something like 510 GB of data with 3-level B-tree by loading only 4 pages(file seeks is 4 times) from disk.
// This is why the B-Tree is a useful data structure for databases, It reduce the number of random I/O from read aspect.

// Accessing Internal node, setter and getter for internal node

// InitializeInternalNode Initialize internal nonde
func InitializeInternalNode(node []byte) {
	SetNodeType(node, TypeInternalNode)
	SetRootNode(node, true)
	*InternalNodeNumKeys(node) = 0
}

// InternalNodeNumKeys Get or set Number of keys in internal node
func InternalNodeNumKeys(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[InternalNodeNumKeysOffset]))
}

// internalNodeRightChildPtr Get or set child ptr.
// Child ptr is child page number
func internalNodeRightChildPtr(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[InternalNodeRightChildOffset]))
}

// InternalNodeCell Get or set Internal node cell from cellNum
// node cell = child pointer + key
func InternalNodeCell(node []byte, cellNum uint32) []byte {
	var cellOffset uint32 = InternalNodeHeaderSize + InternalNodeCellSize*cellNum
	return node[cellOffset : cellOffset+InternalNodeCellSize]
}

func internalNodeChildPtr(node []byte, cellNum uint32) *uint32 {
	cellSlice := InternalNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cellSlice[0]))
}

// InternalNodeKey Get or set Internal node key from cellNum
func InternalNodeKey(node []byte, cellNum uint32) *uint32 {
	cellSlice := InternalNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cellSlice[InternalNodeChildSize]))
}

// InternalNodeChild Get or set Internal node child
func InternalNodeChild(node []byte, childNum uint32) *uint32 {
	var numKeys uint32 = *InternalNodeNumKeys(node)
	if childNum > numKeys {
		fmt.Printf("Tried to access child_num %v > num_keys %v\n", childNum, numKeys)
		os.Exit(util.ExitFailure)
		return nil
	} else if childNum == numKeys {
		return internalNodeRightChildPtr(node)
	} else {
		return internalNodeChildPtr(node, childNum)
	}
}

// Accessing Leaf node, setter and getter for leaf node

// InitializeLeafNode Initialize Leaf node
func InitializeLeafNode(node []byte) {
	SetNodeType(node, TypeLeafNode)
	SetRootNode(node, false)
	*LeafNodeNumCells(node) = 0
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

// GetNodeMaxKeys Get max key in bunch of keys in the node
func GetNodeMaxKeys(node []byte) uint32 {
	switch GetNodeType(node) {
	case TypeInternalNode:
		// For an internal node, the maximum key is always its right key.
		return *InternalNodeKey(node, *InternalNodeNumKeys(node)-1)
	case TypeLeafNode:
		// For a leaf node, it’s the key at the maximum index
		return *LeafNodeKey(node, *LeafNodeNumCells(node)-1)
	default:
		fmt.Printf("Unkown node type\n")
		os.Exit(util.ExitFailure)
		return 0
	}
}

// CreateNewRootNode Take the right child node as input and allocates a new page to store the left child.
func CreateNewRootNode(table *Table, rightNodePageNum uint32) {
	// Handle splitting the root.
	// Old root copied to new page, becomes left child.
	// Address of right child passed in.
	// Re-initialize root page to contain the new root node.
	// New root node points to two children.
	var rootPage *Page = GetPage(table.Pager, table.RootPageNum)
	//var rightPage *Page = GetPage(table.Pager, rightNodePageNum)
	var leftNodePageNum uint32 = GetUnallocatedPageNum(table.Pager)
	var leftPage *Page = GetPage(table.Pager, leftNodePageNum)

	// The old root page is copied to the left node so we can reuse the root page
	// Left child has data copied from old root
	if copy(leftPage.Mem[:], rootPage.Mem[:]) != PageSize {
		os.Exit(util.ExitFailure)
	}
	SetRootNode(leftPage.Mem[:], false)

	// Finally we initialize the root page as a new internal node with two children.
	// Root node is a new internal node with one key and two children
	InitializeInternalNode(rootPage.Mem[:])
	SetRootNode(rootPage.Mem[:], true)
	*InternalNodeNumKeys(rootPage.Mem[:]) = 1
	*InternalNodeChild(rootPage.Mem[:], 0) = leftNodePageNum
	var leftChildMaxKey uint32 = GetNodeMaxKeys(leftPage.Mem[:])
	*InternalNodeKey(rootPage.Mem[:], 0) = leftChildMaxKey
	*internalNodeRightChildPtr(rootPage.Mem[:]) = rightNodePageNum
}

// IsRootNode Check if it is root node
func IsRootNode(node []byte) bool {
	var IsRootNodeField uint8 = *(*uint8)(unsafe.Pointer(&node[IsRootNodeOffset]))
	if IsRootNodeField == 1 {
		return true
	}
	return false
}

// SetRootNode Set the node to root node type
func SetRootNode(node []byte, isRoot bool) {
	var IsRootNodeField *uint8 = (*uint8)(unsafe.Pointer(&node[IsRootNodeOffset]))
	if isRoot {
		*IsRootNodeField = 1
	} else {
		*IsRootNodeField = 0
	}
}

// LeafNodeNumCells Get or set Number of cells in leaf node
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
	var oldPage *Page = GetPage(cursor.TablePtr.Pager, cursor.PageNum)
	var newPageNum uint32 = GetUnallocatedPageNum(cursor.TablePtr.Pager)
	var newPage *Page = GetPage(cursor.TablePtr.Pager, newPageNum)
	InitializeLeafNode(newPage.Mem[:])

	// All existing keys and new key should be divided
	// evenly between old (left) and new (right) nodes to rebalance
	// Starting from the right, move each key to correct position.
	for i := uint32(LeafNodeMaxCells); i >= 0; i-- {
		var destinationPage *Page = nil
		if i >= LeafNodeLeftSplitCount {
			destinationPage = newPage
		} else {
			destinationPage = oldPage
		}

		var indexWithinNode uint32 = i % LeafNodeLeftSplitCount
		var destinationCell []byte = LeafNodeCell(destinationPage.Mem[:], indexWithinNode)
		if i == cursor.CellNum {
			SerializeRow(value, LeafNodeValue(destinationPage.Mem[:], indexWithinNode))
			*LeafNodeKey(destinationPage.Mem[:], indexWithinNode) = key
		} else if i > cursor.CellNum {
			copy(destinationCell, LeafNodeCell(oldPage.Mem[:], i-1))
		} else {
			copy(destinationCell, LeafNodeCell(oldPage.Mem[:], i))
		}
	}

	// update leaf and right nodes num cells
	*LeafNodeNumCells(oldPage.Mem[:]) = LeafNodeLeftSplitCount
	*LeafNodeNumCells(newPage.Mem[:]) = LeafNodeRightSplitCount

	if IsRootNode(oldPage.Mem[:]) {
		CreateNewRootNode(cursor.TablePtr, newPageNum)
	} else {
		// TODO: Need to implement updating parent after split
		os.Exit(util.ExitFailure)
	}

}

// InsertLeafNode Inserting a key/value pair into a leaf node.
// It will take a cursor as input to represent the position where the pair should be inserted.
func InsertLeafNode(cursor *Cursor, key uint32, value *Row) {
	var page *Page = GetPage(cursor.TablePtr.Pager, cursor.PageNum)
	var numCells uint32 = *LeafNodeNumCells(page.Mem[:])
	if numCells >= LeafNodeMaxCells {
		// Leaf node full, need to split into two leaf node
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

// FindLeafNode Search the cursor in the leaf node with binary search.
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

// FindInternalNode Search the cursor in the internal node with binary search.
func FindInternalNode(table *Table, pageNum uint32, key uint32) *Cursor {
	var page *Page = GetPage(table.Pager, pageNum)
	var numKeys uint32 = *InternalNodeNumKeys(page.Mem[:])

	// Binary search to find index of child to search
	var minIndex uint32 = 0
	var maxIndex uint32 = numKeys
	for maxIndex != minIndex {
		var index uint32 = (minIndex + maxIndex) / 2
		var indexKey uint32 = *InternalNodeKey(page.Mem[:], index)
		if indexKey >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	var childNum uint32 = *InternalNodeChild(page.Mem[:], minIndex)
	var childPage *Page = GetPage(table.Pager, childNum)
	switch GetNodeType(childPage.Mem[:]) {
	case TypeLeafNode:
		return FindLeafNode(table, childNum, key)
	case TypeInternalNode:
		return FindInternalNode(table, childNum, key)
	}

	return nil
}

// indent the numbers of level for B-tree
func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("	")
	}
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

// PrintTree Print B-Tree recursively
func PrintTree(pager *Pager, pageNum uint32, indentLevel uint32) {
	var page *Page = GetPage(pager, pageNum)
	var numKeys, child uint32
	switch GetNodeType(page.Mem[:]) {
	case TypeLeafNode:
		numKeys = *LeafNodeNumCells(page.Mem[:])
		indent(indentLevel)
		fmt.Printf("- Leaf num of cells: %v\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentLevel + 1)
			fmt.Printf("- (Leaf cell num: %v, key: %v)\n", i, *LeafNodeKey(page.Mem[:], i))
		}
	case TypeInternalNode:
		numKeys = *InternalNodeNumKeys(page.Mem[:])
		indent(indentLevel)
		fmt.Printf("- Internal num of cells: %v\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = *InternalNodeChild(page.Mem[:], i)
			PrintTree(pager, child, indentLevel+1)

			indent(indentLevel + 1)
			fmt.Printf("- (Internal cell num: %v, key: %v)\n", i, *InternalNodeKey(page.Mem[:], i))
		}
		child = *internalNodeRightChildPtr(page.Mem[:])
		PrintTree(pager, child, indentLevel+1)
	}
}
