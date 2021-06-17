package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	MAX_ID_LEN         = 8
	MAX_NAME_LEN       = 56
	MAX_EMAIL_LEN      = 64
	MAX_PAGE_PER_TABLE = 1024
	PAGE_SZ            = 4096
	MAX_ROW_PER_TABLE  = MAX_ROW_PER_PAGE * MAX_PAGE_PER_TABLE
	MAX_ROW_PER_PAGE   = PAGE_SZ / (MAX_ID_LEN + MAX_EMAIL_LEN + MAX_NAME_LEN)
	LEAF_NODE          = 1
	INTERNAL_NODE      = 2
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
	fd    *os.File
	pages [MAX_PAGE_PER_TABLE][]Row
}

type Table struct {
	num_rows uint32
	pager    *Pager
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

type LeafNode struct {
	node      Node
	cell_nums uint32
	cells     []Cell
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
	cell := Cell{}
	for i := 0; i < int(res.cell_nums); i++ {
		cur := header_size + i*int(cell_size)
		cell.key = binary.LittleEndian.Uint32(page[cur : cur+4])
		cell.value = *(*Row)(unsafe.Pointer(&page[cur+4]))
		res.cells = append(res.cells, cell)
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
	row_num      uint
	end_of_table bool
}

func (c *Cursor) Value() (*Row, error) {
	row_num := c.row_num
	page_num := row_num / MAX_ROW_PER_PAGE
	if page_num >= MAX_PAGE_PER_TABLE {
		return nil, errors.New("table full")
	}
	page, err := c.t.pager.get_page(row_num / MAX_ROW_PER_PAGE)
	if err != nil {
		panic(err)
	}
	return &page[row_num%MAX_ROW_PER_PAGE], nil
}

func (c *Cursor) Advance() {
	c.row_num++
	if c.row_num == uint(c.t.num_rows) {
		c.end_of_table = true
	}
}
func (tab *Table) Start_cursor() *Cursor {
	res := Cursor{t: tab, row_num: 0, end_of_table: tab.num_rows == 0}
	return &res
}

func (tab *Table) End_cursor() *Cursor {
	res := Cursor{t: tab, row_num: uint(tab.num_rows), end_of_table: true}
	return &res
}

func to_byte(p unsafe.Pointer, n int) []byte {
	return (*[PAGE_SZ]byte)(p)[:n]
}

func (p *Pager) get_page(page_num uint) ([]Row, error) {
	if page_num > MAX_PAGE_PER_TABLE {
		return nil, errors.New("requested page exceeds max page")
	}
	page := p.pages[page_num]
	if page == nil {
		p.pages[page_num] = make([]Row, MAX_ROW_PER_PAGE)
		file_len, err := p.fd.Stat()
		if err != nil {
			panic(err)
		}
		cur_page := file_len.Size() / PAGE_SZ
		if file_len.Size()%PAGE_SZ != 0 {
			cur_page++
		}
		page = p.pages[page_num]
		if int64(page_num) < cur_page {
			n, err := p.fd.ReadAt(to_byte(unsafe.Pointer(&page[0]), 4096), int64(page_num*PAGE_SZ))
			if err != nil && err != io.EOF {
				fmt.Println("read ", n)
				panic(err)
			}
		}
	}
	return page, nil
}

func (p *Pager) flush_page(page_num int) {
	if page_num > MAX_PAGE_PER_TABLE || p.pages[page_num] == nil {
		return
	}
	page := p.pages[page_num]
	n, err := p.fd.WriteAt(to_byte(unsafe.Pointer(&page[0]), 4096), int64(page_num*PAGE_SZ))
	// fmt.Printf("%v %v %v\n", page[0].id, page[0].email, page[0].name)
	if err != nil {
		panic(err)
	}
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

func row_copy(dst, src *Row) {
	dst.id = src.id
	arr_copy(dst.name[:], src.name[:])
	arr_copy(dst.email[:], src.email[:])
}

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
		if t.num_rows > MAX_ROW_PER_TABLE {
			panic("table full")
		}
		cur_row, err := t.End_cursor().Value()
		if err != nil {
			fmt.Println(err)
			return
		}
		row_copy(cur_row, &smt.row)
		t.num_rows++
	}

	select_func := func() {
		fmt.Println()
		cur := t.Start_cursor()
		for !cur.end_of_table {
			row, err := cur.Value()
			check(err)
			fmt.Println(*row)
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
	pager := Pager{fd: fd}
	length, err := fd.Stat()
	check(err)
	if length.Size()%int64(unsafe.Sizeof(Row{})) != 0 {
		panic("databse file error")
	}
	db := Table{uint32(length.Size()) / uint32(unsafe.Sizeof(Row{})), &pager}
	return &db
}

func close_DB(t *Table) {
	full_pages := t.num_rows / MAX_ROW_PER_PAGE
	for i := 0; i < int(full_pages); i++ {
		t.pager.flush_page(i)
	}
	// * flush rows not in a full page
	if t.num_rows%MAX_ROW_PER_PAGE != 0 {
		page := t.pager.pages[full_pages]
		_, err := t.pager.fd.WriteAt(to_byte(unsafe.Pointer(&page[0]), (int(t.num_rows)%MAX_ROW_PER_PAGE)*int(unsafe.Sizeof(Row{}))), int64(full_pages*PAGE_SZ))
		check(err)
	}
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
	// fmt.Println(unsafe.Sizeof(Row{}), MAX_ROW_PER_PAGE)
	for {
		fmt.Print(PROMPT)
		scan.Scan()
		input := scan.Text()
		go handle_request(input, T)
	}
}
