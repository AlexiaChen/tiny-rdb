package table

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

// RowSlot returned address of rownum in specific table
func RowSlot(table *Table, rowNum uint32) *byte {
	return nil
}
