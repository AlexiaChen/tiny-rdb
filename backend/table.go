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
	NumPages   uint32
	Pages      [TableMaxPages]*Page
}

// Table  table is consist of pages
type Table struct {
	RootPageNum uint32
	Pager       *Pager
}

// Tables a set of tables
type Tables struct {
	TableMap map[string]*Table
}

// Cursor a cursor point to a row of the table, likes a iterator of other language for containor
type Cursor struct {
	TablePtr     *Table
	PageNum      uint32
	CellNum      uint32
	PassedCells  uint32
	IsEndOfTable bool
}

// CursorBegin create a cursor point to begin of the table
func CursorBegin(table *Table) *Cursor {
	var cursor *Cursor = Find(table, 0)
	var page *Page = GetPage(table.Pager, cursor.PageNum)
	var numCells uint32 = *LeafNodeNumCells(page.Mem[:])
	cursor.PassedCells = 0
	if numCells == 0 {
		cursor.IsEndOfTable = true
	} else {
		cursor.IsEndOfTable = false
	}
	return cursor
}

// CursorEnd create a cursor point to end of the table
func CursorEnd(table *Table) *Cursor {
	var cursor *Cursor = CursorBegin(table)
	for !cursor.IsEndOfTable {
		CursorNext(cursor)
	}
	return cursor
}

// Find Search the tree for a given key
// If the key is not present, return the position where it should be inserted
func Find(table *Table, key uint32) *Cursor {
	var rootPageNum uint32 = table.RootPageNum
	var rootPage *Page = GetPage(table.Pager, rootPageNum)
	if GetNodeType(rootPage.Mem[:]) == TypeLeafNode {
		return FindLeafNode(table, rootPageNum, key)
	} else if GetNodeType(rootPage.Mem[:]) == TypeInternalNode {
		return FindInternalNode(table, rootPageNum, key)
	} else {
		os.Exit(util.ExitFailure)
		return nil
	}
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
	pager.NumPages = uint32(fileInf.Size() / PageSize)

	if pager.FileLength%PageSize != 0 {
		fmt.Printf("DB File is not contains a whole mumber of pages, Corrupt File.\n")
		os.Exit(util.ExitFailure)
	}

	for i := 0; i < TableMaxPages; i++ {
		pager.Pages[i] = nil
	}

	return pager
}

// OpenDB Open a new table from DB file
func OpenDB(filename string) *Table {
	var table *Table = new(Table)
	var pager *Pager = openPager(filename)
	table.RootPageNum = 0
	table.Pager = pager

	if pager.NumPages == 0 {
		// New DB file. Initialize page 0 as leaf node.
		var page *Page = GetPage(pager, 0)
		InitializeLeafNode(page.Mem[:])
		SetRootNode(page.Mem[:], true)
	}
	return table
}

// FlushPage Flush a page from page num
func FlushPage(pager *Pager, pageNum uint32) {
	if pager.Pages[pageNum] == nil {
		fmt.Printf("Error: Flush null page: %v\n", pageNum)
		os.Exit(util.ExitFailure)
	}

	_, err := pager.FilePtr.Seek(int64(pageNum)*int64(PageSize), 0)
	if err != nil {
		fmt.Printf("Error: Seeking file %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	writeBytes, err := pager.FilePtr.Write(pager.Pages[pageNum].Mem[:PageSize])
	if err != nil {
		fmt.Printf("Error writing DB file: %s\n", err.Error())
		os.Exit(util.ExitFailure)
	}

	if uint32(writeBytes) > PageSize {
		fmt.Printf("Write bytes size %v over promised size %v\n", writeBytes, PageSize)
		os.Exit(util.ExitFailure)
	}

	pager.FilePtr.Sync()
}

// CloseDB Flushes the page cache to disk and close the DB file
func CloseDB(table *Table) {
	var pager *Pager = table.Pager

	// Flush fulled pages
	for i := uint32(0); i < pager.NumPages; i++ {
		if pager.Pages[i] != nil {
			FlushPage(pager, i)
			pager.Pages[i] = nil
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
func SerializeRow(src *Row, dst []byte) int {
	var copied int = 0
	unsafeID := unsafe.Pointer(&src.PrimaryID)
	ID := (*[PrimaryIDSize]byte)(unsafeID)
	copied = copied + copy(dst[IDOffSet:IDOffSet+PrimaryIDSize], (*ID)[0:])

	unsafeUserName := unsafe.Pointer(&src.UserName)
	UserName := (*[UserNameSize]byte)(unsafeUserName)
	copied = copied + copy(dst[UserNameOffSet:UserNameOffSet+UserNameSize], (*UserName)[0:])

	unsafeEmail := unsafe.Pointer(&src.Email)
	Email := (*[EmailSize]byte)(unsafeEmail)
	copied = copied + copy(dst[EmailOffSet:EmailOffSet+EmailSize], (*Email)[0:])

	return copied
}

// DeserializeRow Deserialize row
func DeserializeRow(src []byte, dst *Row) int {
	var copied int = 0
	unsafeID := unsafe.Pointer(&dst.PrimaryID)
	ID := (*[PrimaryIDSize]byte)(unsafeID)
	copied = copied + copy((*ID)[0:], src[IDOffSet:IDOffSet+PrimaryIDSize])

	unsafeUserName := unsafe.Pointer(&dst.UserName)
	UserName := (*[UserNameSize]byte)(unsafeUserName)
	copied = copied + copy((*UserName)[0:], src[UserNameOffSet:UserNameOffSet+UserNameSize])

	unsafeEmail := unsafe.Pointer(&dst.Email)
	Email := (*[EmailSize]byte)(unsafeEmail)
	copied = copied + copy((*Email)[0:], src[EmailOffSet:EmailOffSet+EmailSize])

	return copied
}

// GetUnallocatedPageNum in a database with N pages, page numbers 0 through N-1 are allocated. Therefore we can always allocate page number N for new pages.
// Until we start recycling free pages, new pages will always go onto the end of the database file
func GetUnallocatedPageNum(pager *Pager) uint32 {
	return pager.NumPages
}

func flushAllocatedPages(pager *Pager) {
	for i, page := range pager.Pages {
		if page != nil {
			FlushPage(pager, uint32(i))
		} else {
			break
		}
	}
	fileInf, err := pager.FilePtr.Stat()
	if err != nil {
		fmt.Printf("Cannot get lastest file state\n")
		os.Exit(util.ExitFailure)
	}
	pager.FileLength = fileInf.Size()

	fmt.Printf("Current File Length: %v", pager.FileLength)
}

// GetPage Get the page that pageNum specific
func GetPage(pager *Pager, pageNum uint32) *Page {
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
		}

		pager.Pages[pageNum] = page

		if pageNum >= pager.NumPages {
			pager.NumPages = pageNum + 1
		}
	}

	return pager.Pages[pageNum]
}

// CursorValue returned address of a cursor pointed to specific row
func CursorValue(cursor *Cursor) []byte {
	var pageNum uint32 = cursor.PageNum
	var page *Page = GetPage(cursor.TablePtr.Pager, pageNum)
	return LeafNodeValue(page.Mem[:], cursor.CellNum)
}

// CursorNext next cursor
func CursorNext(cursor *Cursor) {
	var pageNum uint32 = cursor.PageNum
	var page *Page = GetPage(cursor.TablePtr.Pager, pageNum)
	cursor.PassedCells++
	cursor.CellNum++
	if cursor.CellNum >= *LeafNodeNumCells(page.Mem[:]) {
		var nextLeafPageNum uint32 = *LeafNodeNextLeaf(page.Mem[:])
		if nextLeafPageNum == 0 {
			// Rightmost leaf node's signle-linked list
			cursor.IsEndOfTable = true
		} else {
			cursor.PageNum = nextLeafPageNum
			cursor.CellNum = 0
		}
	}
}

// PrintRow print row
func PrintRow(row *VisualRow) {
	fmt.Printf("(%d, %v, %v)\n", row.PrimaryID, row.UserName, row.Email)
}
