package backend

import (
	"strconv"
	"testing"
	"tiny-rdb/util"
	"unsafe"
)

func TestLeafNode(t *testing.T) {
	leafNodeBytes := make([]byte, NodeSize)
	InitializeLeafNode(leafNodeBytes)

	numCells := (*uint32)(unsafe.Pointer(&leafNodeBytes[LeafNodeCellsNumOffset]))
	if *numCells != 0 {
		t.Errorf("numCells before fail: %v", numCells)
	}

	*LeafNodeNumCells(leafNodeBytes) = 12
	numCells = (*uint32)(unsafe.Pointer(&leafNodeBytes[LeafNodeCellsNumOffset]))

	if *numCells != 12 {
		t.Errorf("numCells after fail: %v", *numCells)
	}

	for i := uint32(0); i < *numCells; i++ {
		*LeafNodeKey(leafNodeBytes, i) = i
		value := LeafNodeValue(leafNodeBytes, i)
		numStr := strconv.FormatUint(uint64(i), 10)
		copy(value[:], "value"+numStr)
	}

	for i := uint32(0); i < *numCells; i++ {
		if *LeafNodeKey(leafNodeBytes, i) != i {
			t.Errorf("key is wrong: %v", *LeafNodeKey(leafNodeBytes, i))
		}
		value := LeafNodeValue(leafNodeBytes, i)
		numStr := strconv.FormatUint(uint64(i), 10)
		if util.ToString(value) != "value"+numStr {
			t.Errorf("value is wrong: %v", util.ToString(value))
		}
	}
}

func TestPrintBPlusTree(t *testing.T) {
	leafNodeBytes := make([]byte, NodeSize)
	InitializeLeafNode(leafNodeBytes)

	*LeafNodeNumCells(leafNodeBytes) = 12

	for i := uint32(0); i < *LeafNodeNumCells(leafNodeBytes); i++ {
		*LeafNodeKey(leafNodeBytes, i) = i
		value := LeafNodeValue(leafNodeBytes, i)
		numStr := strconv.FormatUint(uint64(i), 10)
		copy(value[:], "value"+numStr)
	}

	if PrintLeafNode(leafNodeBytes) != *LeafNodeNumCells(leafNodeBytes) {
		t.Errorf("Print Leaf node num cells is Wrong")
	}

}
