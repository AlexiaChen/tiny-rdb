package table

import (
	"testing"
	"unsafe"
)

func TestTable(t *testing.T) {

	table := NewTable()
	if table.NumRows != 0 {
		t.Errorf("Num rows must be 0")
	}

	if len(table.Pages) != TableMaxPages {
		t.Errorf("Table Max Pages is error")
	}

	var row Row
	if unsafe.Sizeof(row) != RowSize {
		t.Errorf("Row Size is error %v %v", unsafe.Sizeof(row), RowSize)
	}

	var page *Page
	page = new(Page)
	if unsafe.Sizeof(*page) != PageSize {
		t.Errorf("Page size is error %v", unsafe.Sizeof(*page))
	}

}
