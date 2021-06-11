package main

import (
	"fmt"
	"testing"
)

func TestRow(t *testing.T) {
	table := Table{}
	for i := 0; i < 100000; i++ {
		query := fmt.Sprintf("insert %d vega lee@qq.com", i)
		smt, err := prepare_statement(query)
		if err != nil {
			fmt.Println("invalid age")
		}
		execute_statement(&table, smt)
	}
	expect := Row{}
	expect.id = 20
	arr_copy(expect.name[:], []byte("vega"))
	arr_copy(expect.email[:], []byte("lee@qq.com"))

	for i := 0; i < int(table.num_rows); i++ {
		cur, err := row_slot(&table, uint(i))
		if err != nil {
			t.Fatal("error")
		}
		expect.id = uint64(i)
		if *cur != expect {
			t.Errorf("[%d] expect %v\n		   got %v", i, expect, *cur)
		}
	}
}
