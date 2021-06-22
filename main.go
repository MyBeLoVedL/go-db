package main

import (
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
	MAX_CELL_PER_LEAF_NODE     = (PAGE_SZ - 16) / CELL_SIZE
	MAX_CELL_PER_INTERNAL_NODE = (PAGE_SZ - 16) / INTERNAL_CELL_SIZE
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

type Page [PAGE_SZ]byte

type Pager struct {
	fd          *os.File
	pages       [MAX_PAGE_PER_TABLE]*Page
	page_num    uint32
	tree_height uint32
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
	left_child uint32
	key        uint32
}

type NodeInterface interface {
	bi_search(K uint32) uint32
}

type LeafNode struct {
	node      Node
	cell_nums uint32
	sibling   uint32
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

// ! note the case that node has no cells,which cause an indexing exception
func (l *LeafNode) max_key() uint32 {
	max := l.cells[0].key
	for i := 1; i < int(l.cell_nums); i++ {
		if l.cells[i].key > max {
			max = l.cells[i].key
		}
	}
	return max
}

func (l *InternalNode) max_key() uint32 {
	max := l.cells[0].key
	for i := 1; i < int(l.key_nums); i++ {
		if l.cells[i].key > max {
			max = l.cells[i].key
		}
	}
	return max
}

// * return the pointer to child,not index
func (l *InternalNode) bi_search(K uint32) uint32 {
	lower, upper := 0, int(l.key_nums)-1
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
	if lower == int(l.key_nums) {
		return l.rightmost_child
	} else {
		return l.cells[lower].left_child
	}
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
	// * for the case that root node is leaf node
	var root_key uint32
	if tab.pager.tree_height == 0 {
		old_root, err := tab.pager.get_leaf_node(uint(tab.root_page))
		check(err)
		old_root.node.is_root = false
		root_key = old_root.max_key()
	} else {
		old_root, err := tab.pager.get_internal_node(uint(tab.root_page))
		check(err)
		old_root.node.is_root = false
		root_key = old_root.max_key()
		n := old_root.key_nums
		// * delete the rightmost cell from left child
		old_root.rightmost_child = old_root.cells[n-1].key
		old_root.key_nums--
	}
	new_root_page := tab.pager.get_avail_page()
	new_root, err := tab.pager.get_internal_node(uint(new_root_page))
	check(err)
	new_root.node.is_root = true
	new_root.rightmost_child = uint32(right_child_page)
	new_root.insert_cell(&Cursor{tab, new_root_page, 0, false}, &InternalCell{tab.root_page, root_key})
	tab.root_page = new_root_page

	tab.pager.tree_height++

}

// todo : add recursive search
func (tab *Table) bi_search(K uint32) (res *Cursor) {
	if tab.pager.tree_height == 0 {
		page, err := tab.pager.get_leaf_node(uint(tab.root_page))
		check(err)
		index := page.bi_search(uint32(K))
		res = &Cursor{t: tab, page_num: tab.root_page, cell_num: index}
	} else {
		height := tab.pager.tree_height
		root_page := uint(tab.root_page)
		for ; height > 0; height-- {
			root, err := tab.pager.get_internal_node(root_page)
			check(err)
			root_page = uint(root.bi_search(K))
		}
		leaf_node, err := tab.pager.get_leaf_node(root_page)
		check(err)
		index := leaf_node.bi_search(uint32(K))
		res = &Cursor{t: tab, page_num: uint32(root_page), cell_num: index}
	}
	return
}

func (p *Pager) get_avail_page() uint32 {
	res := p.page_num
	p.page_num++
	return res
}

func (l *LeafNode) insert_cell(cursor *Cursor, cell *Cell) {
	cell_num := cursor.cell_num
	// * branch for no splitting
	if cell_num < MAX_CELL_PER_LEAF_NODE {
		for i := int32(l.cell_nums - 1); i >= int32(cell_num); i-- {
			l.cells[i+1] = l.cells[i]
		}
		l.cells[cell_num] = *cell
		l.cell_nums++
	} else { // ! branch for splitting
		oldpage, err := cursor.t.pager.get_leaf_node(uint(cursor.page_num))
		check(err)
		// * allocate a new page
		right_child_page := cursor.t.pager.get_avail_page()
		newpage, err := cursor.t.pager.get_leaf_node(uint(right_child_page))
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
		oldpage.sibling = right_child_page
		if cursor.t.pager.tree_height == 0 {
			cursor.t.create_new_root(uint(right_child_page))
		} else {
			parent, err := cursor.t.pager.get_internal_node(uint(oldpage.node.parent))
			check((err))
			max := oldpage.max_key()
			idx := parent.bi_search(max)
			cur := Cursor{cursor.t, oldpage.node.parent, idx, false}
			parent.insert_cell(&cur, &InternalCell{cursor.page_num, max})
		}
	}
}

// todo : add code for splitting internal code
func (l *InternalNode) insert_cell(cursor *Cursor, cell *InternalCell) {
	cell_num := cursor.cell_num

	for i := int32(l.key_nums - 1); i >= int32(cell_num); i-- {
		l.cells[i+1] = l.cells[i]
	}
	l.cells[cell_num] = *cell
	l.key_nums++
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
	page, err := c.t.pager.get_leaf_node(uint(c.page_num))
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
	page, err := c.t.pager.get_leaf_node(uint(c.page_num))
	check(err)
	if page.sibling == 0 && c.cell_num == page.cell_nums {
		c.end_of_table = true
	}
	// * reach end of node,pointing to next node
	if c.cell_num == page.cell_nums {
		c.page_num = page.sibling
		c.cell_num = 0
	}

}

func (tab *Table) Start_cursor() *Cursor {
	return tab.bi_search(0)
}

func (p *Pager) get_raw_page(page_num uint) (*Page, error) {
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
		new_page := new(Page)
		if page_num < uint(tot_pages) {
			n, err := p.fd.ReadAt((*new_page)[:], int64(page_num)*PAGE_SZ)
			check(err)
			if n != PAGE_SZ {
				return nil, errors.New("error reading page")
			}
		}
		// * set total page num
		p.page_num = uint32(page_num + 1)
		p.pages[page_num] = new_page
		node = new_page
	}
	return node, nil
}

func (p *Pager) flush_page(page_num int) {
	if page_num > MAX_PAGE_PER_TABLE || p.pages[page_num] == nil {
		return
	}
	old_page, err := p.get_raw_page(uint(page_num))
	check(err)
	n, err := p.fd.WriteAt((*old_page)[:], int64(page_num*PAGE_SZ))
	check(err)
	if n != PAGE_SZ {
		panic("partially flush a page")
	}
	p.pages[page_num] = nil
}

func (p *Pager) get_leaf_node(page_num uint) (*LeafNode, error) {
	page, err := p.get_raw_page(uint(page_num))
	node := (*LeafNode)(unsafe.Pointer(page))
	return node, err
}

func (p *Pager) get_internal_node(page_num uint) (*InternalNode, error) {
	page, err := p.get_raw_page(uint(page_num))
	node := (*InternalNode)(unsafe.Pointer(page))
	return node, err
}

func (p *Pager) set_leaf_node(page_num int) {
	page, err := p.get_raw_page(uint(page_num))
	check(err)
	node := (*LeafNode)(unsafe.Pointer(page))
	node.node.node_type = LEAF_NODE
	node.cell_nums = 0
}

func (p *Pager) set_internal_node(page_num int) {
	page, err := p.get_raw_page(uint(page_num))
	check(err)
	node := (*InternalNode)(unsafe.Pointer(page))
	node.node.node_type = LEAF_NODE
	node.key_nums = 0

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
	case ".param":
		fmt.Println("leaf node header size : ", unsafe.Sizeof(Node{})+8)
		fmt.Println("internal node  header size : ", unsafe.Sizeof(Node{})+8)
		fmt.Println("max cell per leaf node : ", MAX_CELL_PER_LEAF_NODE)
		fmt.Println("max cell per internal  node : ", MAX_CELL_PER_INTERNAL_NODE)
		fmt.Println()
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
		page, err := t.pager.get_leaf_node(uint(cursor.page_num))
		// * check the row at cursor position collide with key , watch out , 0 key value is not supported
		if cursor.cell_num < MAX_CELL_PER_LEAF_NODE && smt.row.id == uint64(page.cells[cursor.cell_num].key) {
			fmt.Println("duplicate row")
			return
		}
		check(err)
		page.insert_cell(cursor, &Cell{uint32(smt.row.id), smt.row})
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
	for i := 0; i < int(t.pager.page_num); i++ {
		t.pager.flush_page(i)
	}
	t.pager.fd.Close()
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
	// scan := bufio.NewScanner(os.Stdin)
	T := open_DB("stu.db")
	input := "insert 21 aa bb"
	smt, err := prepare_statement(input)
	if err != nil {
		fmt.Println(err)
		return
	}
	execute_statement(T, smt)

	input2 := "insert 22 cc dd"
	smt, err = prepare_statement(input2)
	if err != nil {
		fmt.Println(err)
		return
	}
	execute_statement(T, smt)

	input3 := "insert 30 cc dd"
	smt, err = prepare_statement(input3)
	if err != nil {
		fmt.Println(err)
		return
	}
	execute_statement(T, smt)

	p, err := T.pager.get_leaf_node(0)
	check(err)
	fmt.Println(p.cell_nums)

	p1, err := T.pager.get_leaf_node(1)
	check(err)
	fmt.Println(p1.cell_nums)

	input4 := "select"
	smt, err = prepare_statement(input4)
	if err != nil {
		fmt.Println(err)
		return
	}
	execute_statement(T, smt)

	close_DB(T)
	// fmt.Println(unsafe.Sizeof(LeafNode{}))
	// for {
	// 	fmt.Print(PROMPT)
	// 	scan.Scan()
	// 	input := scan.Text()
	// 	go handle_request(input, T)
	// }
}
