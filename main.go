package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const PROMPT = ">>> "

const (
	SELECT_STATEMENT = iota
	INSERT_STATEMENT
)

const (
	MAX_ID_LEN         = 8
	MAX_NAME_LEN       = 64
	MAX_EMAIL_LEN      = 64
	MAX_PAGE_PER_TABLE = 1024
	PAGE_SZ            = 4096
	MAX_ROW_PER_TABLE  = MAX_ROW_PER_PAGE * MAX_PAGE_PER_TABLE
	MAX_ROW_PER_PAGE   = PAGE_SZ / (MAX_ID_LEN + MAX_EMAIL_LEN + MAX_NAME_LEN)
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

type Table struct {
	num_rows uint32
	pages    [MAX_PAGE_PER_TABLE][]Row
}

type Statement struct {
	smt_type statement_type
	row      Row
}

func row_slot(t *Table, row_num uint) (*Row, error) {
	page_num := row_num / MAX_ROW_PER_PAGE
	if page_num >= MAX_PAGE_PER_TABLE {
		return nil, errors.New("table full")
	}

	page := t.pages[row_num/MAX_ROW_PER_PAGE]
	if page == nil {
		t.pages[row_num/MAX_ROW_PER_PAGE] = make([]Row, MAX_ROW_PER_PAGE)
		page = t.pages[row_num/MAX_ROW_PER_PAGE]
	}
	return &page[row_num%MAX_ROW_PER_PAGE], nil
}

func do_meta_command(op string) {
	if op[0] != '.' {
		panic("meta operation should start with \".\"")
	}
	switch op {
	case ".exit":
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
	switch smt.smt_type {
	case INSERT_STATEMENT:
		if t.num_rows > MAX_ROW_PER_TABLE {
			panic("table full")
		}
		cur_row, err := row_slot(t, uint(t.num_rows))
		if err != nil {
			// fmt.Println(err)
			return
		}
		row_copy(cur_row, &smt.row)
		t.num_rows++
	case SELECT_STATEMENT:
		for i := 0; i < int(t.num_rows); i++ {
			row, err := row_slot(t, uint(i))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(*row)
		}
	default:
		panic("unknown statement")
	}
}

func main() {
	scan := bufio.NewScanner(os.Stdin)
	T := Table{}
	// fmt.Println(unsafe.Sizeof(Row{}), MAX_ROW_PER_PAGE)
	for {
		fmt.Print(PROMPT)
		scan.Scan()
		input := scan.Text()
		if input[0] == '.' {
			do_meta_command(input)
		} else {
			smt, err := prepare_statement(input)
			fmt.Println(smt.row)
			if err != nil {
				continue
			}
			execute_statement(&T, smt)
		}
	}
}
