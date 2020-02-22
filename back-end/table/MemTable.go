package table

import (
	"fmt"
	"unsafe"
)

// Row Table Row
type Row struct {
	PrimaryID uint32
	UserName  [32]byte
	Email     [256]byte
}

// const var
const (
	PrimaryIDSize = 4
	UserNameSize  = 32
	EmailSize     = 256

	IDOffSet       = 0
	UserNameOffSet = IDOffSet + PrimaryIDSize
	EmailOffSet    = UserNameOffSet + UserNameSize
	RowSize        = PrimaryIDSize + UserNameSize + EmailSize

	TableMaxPages = 100
	PageSize      = 4 * 1024 // 4KB
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

// Page  one page = 4kB
type Page struct {
	Mem [PageSize]byte
}

// Table  table is consist of pages
type Table struct {
	NumRows uint32
	Pages   [TableMaxPages]*Page
}

// Tables a set of tables
type Tables struct {
	TableMap map[string]*Table
}

// NewTable Make a new table
func NewTable() *Table {
	var table *Table
	table = new(Table)
	table.NumRows = 0
	for i := 0; i < TableMaxPages; i++ {
		table.Pages[i] = nil
	}
	return table
}

// SerializeRow Serialize Row
func SerializeRow(src *Row, dst *[]byte) int {
	var copied int = 0
	unsafeID := unsafe.Pointer(&src.PrimaryID)
	ID := (*[PrimaryIDSize]byte)(unsafeID)
	copied = copied + copy((*dst)[IDOffSet:IDOffSet+PrimaryIDSize], (*ID)[0:])

	unsafeUserName := unsafe.Pointer(&src.UserName)
	UserName := (*[UserNameSize]byte)(unsafeUserName)
	copied = copied + copy((*dst)[UserNameOffSet:UserNameOffSet+UserNameSize], (*UserName)[0:])

	unsafeEmail := unsafe.Pointer(&src.Email)
	Email := (*[EmailSize]byte)(unsafeEmail)
	copied = copied + copy((*dst)[EmailOffSet:EmailOffSet+EmailSize], (*Email)[0:])
	return copied
}

// DeserializeRow Deserialize row
func DeserializeRow(src *[]byte, dst *Row) int {
	var copied int = 0
	unsafeID := unsafe.Pointer(&dst.PrimaryID)
	ID := (*[PrimaryIDSize]byte)(unsafeID)
	copied = copied + copy((*ID)[0:], (*src)[IDOffSet:IDOffSet+PrimaryIDSize])

	unsafeUserName := unsafe.Pointer(&dst.UserName)
	UserName := (*[UserNameSize]byte)(unsafeUserName)
	copied = copied + copy((*UserName)[0:], (*src)[UserNameOffSet:UserNameOffSet+UserNameSize])

	unsafeEmail := unsafe.Pointer(&dst.Email)
	Email := (*[EmailSize]byte)(unsafeEmail)
	copied = copied + copy((*Email)[0:], (*src)[EmailOffSet:EmailOffSet+EmailSize])
	return copied
}

// RowSlot returned address of rownum in specific table
func RowSlot(table *Table, rowNum uint32) *byte {
	return nil
}

// PrintRow print row
func PrintRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.PrimaryID, row.UserName, row.Email)
}
