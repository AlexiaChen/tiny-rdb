package backend

import (
	"fmt"
	"os"
	"tiny-rdb/util"
	"unsafe"
)

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

// Row Table Row
type Row struct {
	PrimaryID uint32
	UserName  [UserNameSize]byte
	Email     [EmailSize]byte
}

// VisualRow readable row
type VisualRow struct {
	PrimaryID uint32
	UserName  string
	Email     string
}

// Page  one page = 4kB
type Page struct {
	Mem [PageSize]byte
}

// Pager Accesses the page cache and the file. The Table object makes requests for pages through the pager
type Pager struct {
	FilePtr    *os.File
	FileLength int64
	Pages      [TableMaxPages]*Page
}

// Table  table is consist of pages
type Table struct {
	NumRows uint32
	Pager   *Pager
}

// Tables a set of tables
type Tables struct {
	TableMap map[string]*Table
}

// Cursor a cursor point to a row of the table, likes a iterator of other language for containor
type Cursor struct {
	table        *Table
	rowNum       uint32
	IsEndOfTable bool
}

// CursorBegin create a cursor point to begin of the table
func CursorBegin(table *Table) *Cursor {
	var cursor *Cursor = new(Cursor)
	cursor.table = table
	cursor.rowNum = 0

	if table.NumRows == 0 {
		cursor.IsEndOfTable = true
	} else {
		cursor.IsEndOfTable = false
	}
	return cursor
}

// CursorEnd create a cursor point to end of the table
func CursorEnd(table *Table) *Cursor {
	var cursor *Cursor = new(Cursor)
	cursor.table = table
	cursor.rowNum = table.NumRows
	cursor.IsEndOfTable = true
	return cursor
}

func openPager(filename string) *Pager {
	filePtr, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("Unable to open DB file: %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	fileInf, err := os.Stat(filename)
	if err != nil {
		fmt.Printf("Unable to get file size: %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	var pager *Pager = new(Pager)
	pager.FilePtr = filePtr
	pager.FileLength = fileInf.Size()

	for i := 0; i < TableMaxPages; i++ {
		pager.Pages[i] = nil
	}

	return pager
}

// OpenDB Open a new table from DB file
func OpenDB(filename string) *Table {
	var table *Table = new(Table)
	var pager *Pager = openPager(filename)
	table.NumRows = uint32(pager.FileLength / RowSize)
	table.Pager = pager
	return table
}

// FlushPage Flush a page from page num in specific size
func FlushPage(pager *Pager, pageNum uint32, size uint32) {
	if pager.Pages[pageNum] == nil {
		fmt.Printf("Error: Flush null page: %v\n", pageNum)
		os.Exit(util.ExitFailure)
	}

	_, err := pager.FilePtr.Seek(int64(pageNum)*int64(PageSize), 0)
	if err != nil {
		fmt.Printf("Error: Seeking file %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	writeBytes, err := pager.FilePtr.Write(pager.Pages[pageNum].Mem[:size])
	if err != nil {
		fmt.Printf("Error writing DB file: %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	if uint32(writeBytes) > size {
		fmt.Printf("Write bytes size %v over promised size %v\n", writeBytes, size)
		os.Exit(util.ExitFailure)
	}
}

// CloseDB Flushes the page cache to disk and close the DB file
func CloseDB(table *Table) {
	var pager *Pager = table.Pager

	// Flush fulled pages
	var numFulledPage uint32 = table.NumRows / RowsPerPage
	for i := uint32(0); i < numFulledPage; i++ {
		if pager.Pages[i] != nil {
			FlushPage(pager, i, PageSize)
			pager.Pages[i] = nil
		}
	}

	// Flush last not fulled page
	var notFulledPageRowsNum uint32 = table.NumRows % RowsPerPage
	if notFulledPageRowsNum > 0 {
		var lastPageNum uint32 = numFulledPage
		if pager.Pages[lastPageNum] != nil {
			FlushPage(pager, lastPageNum, notFulledPageRowsNum*RowSize)
			pager.Pages[lastPageNum] = nil
		}
	}

	pager.FilePtr.Sync()

	// Close DB file
	err := pager.FilePtr.Close()
	if err != nil {
		fmt.Printf("Error closing DB file: %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}
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

func getPage(pager *Pager, pageNum uint32) *Page {
	if pageNum > TableMaxPages {
		fmt.Printf("page number out of bound: %v\n", pageNum)
		os.Exit(util.ExitFailure)
	}

	if pager.Pages[pageNum] == nil {
		var page *Page = new(Page)
		var numPages uint32 = uint32(pager.FileLength / PageSize)
		// Last page not fulled
		if pager.FileLength%PageSize != 0 {
			numPages++
		}

		if pageNum <= numPages {
			var fileOffSet int64 = int64(pageNum) * int64(PageSize)
			pager.FilePtr.Seek(fileOffSet, 0)

			var restOfSize int64 = pager.FileLength - fileOffSet
			if restOfSize >= PageSize {
				readBytes, err := pager.FilePtr.Read(page.Mem[:])
				if err != nil {
					fmt.Printf("Error reading file: %s\n", err.Error())
					os.Exit(util.ExitFailure)
				}

				if readBytes != PageSize {
					fmt.Printf("Read Bytes %v not equal to PageSize(4kB)\n", readBytes)
					os.Exit(util.ExitFailure)
				}
			}

			if restOfSize < PageSize {
				readBytes, err := pager.FilePtr.Read(page.Mem[:restOfSize])
				if err != nil {
					fmt.Printf("Error reading file: %s\n", err.Error())
					os.Exit(util.ExitFailure)
				}

				if int64(readBytes) != restOfSize {
					fmt.Printf("Read Bytes %v not equal to restOfSize %v\n", readBytes, restOfSize)
					os.Exit(util.ExitFailure)
				}
			}

			pager.Pages[pageNum] = page
		}
	}

	return pager.Pages[pageNum]
}

// CursorValue returned address of a cursor pointed to specific row
func CursorValue(cursor *Cursor) []byte {
	var pageNum uint32 = cursor.rowNum / RowsPerPage

	var page *Page = getPage(cursor.table.Pager, pageNum)
	var rowOffset uint32 = cursor.rowNum % RowsPerPage
	var byteOffset uint32 = rowOffset * RowSize
	var offsetSlice []byte = page.Mem[byteOffset : byteOffset+RowSize]

	return offsetSlice
}

// CursorNext next cursor
func CursorNext(cursor *Cursor) {
	cursor.rowNum++
	if cursor.rowNum >= cursor.table.NumRows {
		cursor.IsEndOfTable = true
	}
}

// PrintRow print row
func PrintRow(row *VisualRow) {
	fmt.Printf("(%d, %v, %v)\n", row.PrimaryID, row.UserName, row.Email)
}
