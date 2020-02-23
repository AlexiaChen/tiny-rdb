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

func TestSerialize(t *testing.T) {

	var row Row
	row.PrimaryID = 12
	copy(row.UserName[:], "Jhone")
	copy(row.Email[:], "jhone@google.com")

	if len(string(row.UserName[:5])) != 5 {
		t.Errorf("User name: %v size is: %v", string(row.UserName[:5]), len(string(row.UserName[:])))
	}

	bytes := make([]byte, 500)
	copied := SerializeRow(&row, &bytes)

	if copied != RowSize {
		t.Errorf("seriliaze copied size: %v", copied)
	}

	var newRow Row
	copied = DeserializeRow(&bytes, &newRow)
	if copied != RowSize {
		t.Errorf("deserialize copied size: %v", copied)
	}
}

func TestRowSlot(t *testing.T) {
	var table Table
	bytesSlice := RowSlot(&table, 12)

	if len(bytesSlice) == 0 {
		t.Errorf("bytes must be not null.")
	}

	if table.Pages[0] == nil {
		t.Errorf("Page 0 must not be null.")
	}

}
