package main

import (
	"fmt"
	"os"
	"testing"
)

func TestRow(t *testing.T) {
	os.Truncate("stu.db", 0)
	table := open_DB("stu.db")
	for i := 0; i < 31; i++ {
		query := fmt.Sprintf("insert %d vega lee@qq.com", i)
		smt, err := prepare_statement(query)
		if err != nil {
			fmt.Println("invalid age")
		}
		execute_statement(table, smt)
	}
	close_DB(table)

	table = open_DB("stu.db")

	expect := Row{}
	expect.id = 20
	arr_copy(expect.name[:], []byte("vega"))
	arr_copy(expect.email[:], []byte("lee@qq.com"))

	cursor := table.Start_cursor()
	i := 0
	for !cursor.end_of_table {
		cur, err := cursor.Value()
		if err != nil {
			t.Fatal("error")
		}
		expect.id = uint64(i)
		i++
		if (*cur).value != expect {
			t.Errorf("[%d] expect %v\n		   got %v", i, expect, (*cur).value)
		}
		cursor.Advance()
	}
}

func TestSerialize(t *testing.T) {
	// table := open_DB("stu.db")
	root := LeafNode{node: Node{1, true, 0}, cell_nums: 2}
	root.cells[0] = Cell{7, Row{21, [56]byte{0, 1}, [64]byte{2, 3}}}
	root.cells[1] = Cell{9, Row{22, [56]byte{1, 1}, [64]byte{3, 3}}}
	// fmt.Println(data)
	data := deserialize_leaf_node_into_page(&root)
	ne := serialize_into_leaf_node(data)

	if root.node != ne.node {
		t.Fatalf("1")
	}
	if root.cell_nums != ne.cell_nums {
		t.Fatalf("2")
	}
	if root.cells[0] != ne.cells[0] {
		t.Fatalf("3")
	}
	if root.cells[1] != ne.cells[1] {
		t.Fatalf("4")
	}
}
