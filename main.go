package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const PROMPT = ">>> "

const (
	SELECT_STATEMENT = iota
	INSERT_STATEMENT
)

const (
	MAX_ID_LEN                 = 8
	MAX_NAME_LEN               = 56
	MAX_EMAIL_LEN              = 64
	MAX_PAGE_PER_TABLE         = 1024
	PAGE_SZ                    = 4096
	ROW_SIZE                   = 128
	CELL_SIZE                  = ROW_SIZE + 4
	INTERNAL_CELL_SIZE         = 8
	MAX_CELL_PER_LEAF_NODE     = (PAGE_SZ - 10) / CELL_SIZE
	MAX_CELL_PER_INTERNAL_NODE = (PAGE_SZ - 14) / INTERNAL_CELL_SIZE
	LEAF_NODE                  = 1
	INTERNAL_NODE              = 2
)

type statement_type uint

type Row struct {
	id    uint64
	name  [MAX_NAME_LEN]byte
	email [MAX_EMAIL_LEN]byte
}

func (r Row) String() string {
	return fmt.Sprintf("%v %v %v", r.id, string(r.name[:]), string(r.email[:]))
}

type Pager struct {
	fd       *os.File
	pages    [MAX_PAGE_PER_TABLE]*LeafNode
	page_num uint32
}

type Table struct {
	root_page uint32
	pager     *Pager
}

type Node struct {
	node_type uint8
	is_root   bool
	parent    uint32
}

type Cell struct {
	key   uint32
	value Row
}

type InternalCell struct {
	child uint32
	key   uint32
}

type NodeInterface interface {
	bi_search(K uint32) uint32
}

type LeafNode struct {
	node      Node
	cell_nums uint32
	cells     [MAX_CELL_PER_LEAF_NODE]Cell
}

type InternalNode struct {
	node            Node
	key_nums        uint32
	rightmost_child uint32
	cells           [MAX_CELL_PER_INTERNAL_NODE]InternalCell
}

func (l *LeafNode) bi_search(K uint32) uint32 {
	lower, upper := 0, int(l.cell_nums)-1
	if upper < 0 {
		return 0
	}
	if K > l.cells[upper].key {
		return uint32(upper) + 1
	}
	for lower <= upper {
		mid := (upper + lower/2)
		if l.cells[mid].key == K {
			return uint32(mid)
		} else if l.cells[mid].key < K {
			lower = mid + 1
		} else if l.cells[mid].key > K {
			upper = mid - 1
		}
	}
	return uint32(lower)
}

func serialize_into_leaf_node(page []byte) *LeafNode {
	res := LeafNode{}
	res.node.node_type = page[0]
	if page[1] == 0 {
		res.node.is_root = false
	} else {
		res.node.is_root = true
	}
	header_size := 10
	cell_size := unsafe.Sizeof(Row{}) + 4
	res.node.parent = binary.LittleEndian.Uint32(page[2:6])
	res.cell_nums = binary.LittleEndian.Uint32(page[6:10])
	for i := 0; i < int(res.cell_nums); i++ {
		cur := header_size + i*int(cell_size)
		res.cells[i].key = binary.LittleEndian.Uint32(page[cur : cur+4])
		res.cells[i].value = *(*Row)(unsafe.Pointer(&page[cur+4]))
	}
	return &res
}

func deserialize_leaf_node_into_page(node *LeafNode) []byte {
	res := make([]byte, PAGE_SZ)
	res[0] = node.node.node_type
	if node.node.is_root {
		res[1] = 1
	} else {
		res[0] = 0
	}
	binary.LittleEndian.PutUint32(res[2:6], node.node.parent)
	binary.LittleEndian.PutUint32(res[6:10], node.cell_nums)
	header_size := 10
	cell_size := unsafe.Sizeof(Row{}) + 4
	for i := 0; i < int(node.cell_nums); i++ {
		cur := header_size + i*int(cell_size)
		binary.LittleEndian.PutUint32(res[cur:cur+4], node.cells[i].key)
		arr_copy(res[cur+4:cur+int(cell_size)], ((*[PAGE_SZ]byte)(unsafe.Pointer(&(node.cells[i].value))))[0:cell_size-4])
	}
	return res
}

func (tab *Table) create_new_root(right_child_page uint) {
	old_root, err := tab.pager.get_page(uint(tab.root_page))
	check(err)
	right_child, err := tab.pager.get_page(uint(right_child_page))
	check(err)
	new_root_page := tab.pager.get_avail_page()
	new_root, err := tab.pager.get_page(uint(new_root_page))
	check(err)
	old_root.node.is_root = false
	new_root.node.is_root = true

}

func (tab *Table) bi_search(K uint32) *Cursor {
	page, err := tab.pager.get_page(uint(tab.root_page))
	check(err)
	index := page.bi_search(uint32(K))
	res := Cursor{t: tab, page_num: tab.root_page, cell_num: index}
	return &res
}

func (p *Pager) get_avail_page() uint32 {
	res := p.page_num
	p.page_num++
	return res
}

func (l *LeafNode) insert_cell(cursor *Cursor, cell *Cell) {
	cur := cursor.cell_num

	for i := int32(l.cell_nums - 1); i >= int32(cur); i-- {
		// fmt.Println("i", i, cur)
		l.cells[i+1] = l.cells[i]
	}
	l.cells[cur] = *cell
}

// * split a leaf node ,not the internal node
func (l *LeafNode) insert_cell_and_split(cursor *Cursor, cell *Cell) {
	oldpage, err := cursor.t.pager.get_page(uint(cursor.page_num))
	check(err)
	// * allocate a new page
	newpage, err := cursor.t.pager.get_page(uint(cursor.t.pager.get_avail_page()))
	check(err)

	var dst_node *LeafNode
	var index uint32
	left_cells := (MAX_CELL_PER_LEAF_NODE + 1) / 2
	right_cells := MAX_CELL_PER_LEAF_NODE + 1 - left_cells
	for i := MAX_CELL_PER_LEAF_NODE; i >= 0; i-- {
		if i >= left_cells {
			dst_node = newpage
			index = uint32(i - left_cells)
		} else {
			dst_node = oldpage
			index = uint32(i)
		}
		if uint32(i) == cursor.cell_num {
			dst_node.cells[index] = *cell
		} else if uint32(i) > cursor.cell_num {
			dst_node.cells[index] = oldpage.cells[i-1]
		} else {
			dst_node.cells[index] = oldpage.cells[i]
		}
	}
	oldpage.cell_nums = uint32(left_cells)
	newpage.cell_nums = uint32(right_cells)

	// * if old page is root node, a new root node is created
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type Statement struct {
	smt_type statement_type
	row      Row
}

type Cursor struct {
	t            *Table
	page_num     uint32
	cell_num     uint32
	end_of_table bool
}

func (c *Cursor) Value() (*Cell, error) {
	page, err := c.t.pager.get_page(uint(c.page_num))
	check(err)
	if page == nil {
		panic("nil")
	}
	if c.cell_num == MAX_CELL_PER_LEAF_NODE {
		return nil, errors.New("table full")
	} else {
		return &page.cells[c.cell_num], nil
	}
}

func (c *Cursor) Advance() {
	c.cell_num++
	page, err := c.t.pager.get_page(uint(c.page_num))
	check(err)
	if c.cell_num == page.cell_nums {
		c.end_of_table = true
	}
}

func (tab *Table) Start_cursor() *Cursor {
	page, err := tab.pager.get_page(uint(tab.root_page))
	check(err)
	res := Cursor{t: tab, page_num: tab.root_page, end_of_table: page.cell_nums == 0}
	return &res
}

// func to_byte(p unsafe.Pointer, n int) []byte {
// 	return (*[PAGE_SZ]byte)(p)[:n]
// }

func (p *Pager) get_page(page_num uint) (*LeafNode, error) {
	if page_num > MAX_PAGE_PER_TABLE {
		return nil, errors.New("requested page exceeds max page")
	}
	node := p.pages[page_num]
	if node == nil {
		info, err := p.fd.Stat()
		check(err)
		if info.Size()%PAGE_SZ != 0 {
			panic("bad file length")
		}
		tot_pages := info.Size() / PAGE_SZ
		if page_num < uint(tot_pages) {
			page := make([]byte, PAGE_SZ)
			_, err := p.fd.ReadAt(page, int64(page_num)*PAGE_SZ)
			check(err)
			node = serialize_into_leaf_node(page)
		} else {
			node = &LeafNode{node: Node{LEAF_NODE, false, 0}, cell_nums: 0}
			p.page_num = uint32(page_num + 1)
		}
		p.pages[page_num] = node
	}
	return node, nil
}

func (p *Pager) flush_page(page_num int) {
	if page_num > MAX_PAGE_PER_TABLE || p.pages[page_num] == nil {
		return
	}
	page, err := p.get_page(uint(page_num))
	check(err)
	n, err := p.fd.WriteAt(deserialize_leaf_node_into_page(page), int64(page_num*PAGE_SZ))
	check(err)
	if n != PAGE_SZ {
		panic("partially flush a page")
	}
	p.pages[page_num] = nil
}

func do_meta_command(T *Table, op string) {
	if op[0] != '.' {
		panic("meta operation should start with \".\"")
	}
	switch op {
	case ".exit":
		close_DB(T)
		fmt.Println("Database closed")
		os.Exit(1)
	default:
		fmt.Println("unrecognized command : " + op)
	}
}

func arr_copy(dst, src []byte) {
	for i := 0; i < len(src); i++ {
		dst[i] = src[i]
	}
}

// func row_copy(dst, src *Row) {
// 	dst.id = src.id
// 	arr_copy(dst.name[:], src.name[:])
// 	arr_copy(dst.email[:], src.email[:])
// }

func prepare_statement(smt string) (Statement, error) {
	var ret_smt Statement
	var err error
	tokens := strings.Fields(smt)
	ty := strings.ToLower(tokens[0])
	if ty == "insert" {
		if len(tokens) != 4 {
			fmt.Println("insert format : insert age name email")
			goto handle_err
		}
		ret_smt.smt_type = INSERT_STATEMENT
		ret_smt.row.id, err = strconv.ParseUint(tokens[1], 10, 32)
		if err != nil {
			fmt.Println("invalid age")
			goto handle_err
		}
		arr_copy(ret_smt.row.name[:], []byte(tokens[2]))
		arr_copy(ret_smt.row.email[:], []byte(tokens[3]))
	} else if ty == "select" {
		ret_smt.smt_type = SELECT_STATEMENT
	} else {
		return ret_smt, errors.New("unrecognized statement")
	}
	return ret_smt, nil

handle_err:
	return ret_smt, errors.New("unrecognized statement")

}

func execute_statement(t *Table, smt Statement) {
	insert_func := func() {
		cursor := t.bi_search(uint32(smt.row.id))
		// fmt.Println("cursor: ", cursor.page_num, cursor.cell_num)
		page, err := t.pager.get_page(uint(cursor.page_num))
		// * check the row at cursor position collide with key , watch out , 0 key value is not supported
		if cursor.cell_num < MAX_CELL_PER_LEAF_NODE && smt.row.id == uint64(page.cells[cursor.cell_num].key) {
			fmt.Println("duplicate row")
			return
		}
		check(err)
		// * split leaf node , insert this cell into appopiate page
		// fmt.Println(cursor.cell_num)
		if page.cell_nums == MAX_CELL_PER_LEAF_NODE {
			page.insert_cell_and_split(cursor, &Cell{uint32(smt.row.id), smt.row})
		} else {
			page.insert_cell(cursor, &Cell{uint32(smt.row.id), smt.row})
		}
		page.cell_nums++
	}

	select_func := func() {
		fmt.Println()
		cur := t.Start_cursor()
		for !cur.end_of_table {
			row, err := cur.Value()
			check(err)
			fmt.Println((*row).value)
			cur.Advance()
		}
		fmt.Print(PROMPT)
	}

	switch smt.smt_type {
	case INSERT_STATEMENT:
		insert_func()
	case SELECT_STATEMENT:
		select_func()
	default:
		panic("unknown statement")
	}
}

func open_DB(file string) *Table {
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		panic(err)
	}
	length, err := fd.Stat()
	check(err)
	if length.Size()%PAGE_SZ != 0 {
		panic("databse file error")
	}
	pager := Pager{fd: fd, page_num: uint32(length.Size()) / PAGE_SZ}
	db := Table{0, &pager}
	return &db
}

func close_DB(t *Table) {
	// * for now,only one node for a table
	// * flush rows not in a full page
	t.pager.flush_page(int(t.root_page))
	err := t.pager.fd.Close()
	check(err)
}

func handle_request(input string, T *Table) {
	if input[0] == '.' {
		do_meta_command(T, input)
	} else {
		smt, err := prepare_statement(input)
		if err != nil {
			fmt.Println(err)
			return
		}
		execute_statement(T, smt)
	}
}

func main() {
	scan := bufio.NewScanner(os.Stdin)
	T := open_DB("stu.db")
	fmt.Println(unsafe.Sizeof(LeafNode{}))
	for {
		fmt.Print(PROMPT)
		scan.Scan()
		input := scan.Text()
		go handle_request(input, T)
	}
}
