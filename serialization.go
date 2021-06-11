package main

import (
	"encoding/binary"
	"unsafe"
)

type Time struct {
	year  uint64
	month uint32
	day   uint8
}

func (t *Time) Marshal() ([]byte, error) {
	buf := make([]byte, unsafe.Sizeof(*t))
	binary.BigEndian.PutUint64(buf[:8], t.year)
	binary.BigEndian.PutUint32(buf[8:12], t.month)
	buf[12] = t.day
	return buf, nil
}

func UnMarshal(data []byte, t *Time) {
	t.day = data[12]
	t.year = binary.BigEndian.Uint64(data[:8])
	t.month = binary.BigEndian.Uint32(data[8:12])
}

func Time2Bytes(p unsafe.Pointer, n uint) []byte {
	return (*[4096]byte)(p)[:n]
}

// func main() {
// 	t := Time{110, 212, 33}
// 	fmt.Printf("size of Time %v\n", unsafe.Sizeof(t))
// 	bs, err := t.Marshal()
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	fmt.Printf("bytes[] -> t %v\n", bs)

// 	direct := Time2Bytes(unsafe.Pointer(&t), uint(unsafe.Sizeof(t)))
// 	fmt.Printf("direct bytes[] -> t %v\n", direct)
// 	t1 := Time{}
// 	UnMarshal(bs, &t1)
// 	fmt.Printf("[]byte to Time : %v\n", t1)

// 	fd, err := os.OpenFile("tmp.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
// 	if err != nil {
// 		panic(err)
// 	}

// 	_, err = fd.Write(direct)
// 	if err != nil {
// 		panic(err)
// 	}

// 	after_file := Time{}
// 	after := Time2Bytes(unsafe.Pointer(&after_file), uint(unsafe.Sizeof(after_file)))

// 	_, err = fd.ReadAt(after, 0)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("after file coversion ", after_file)

// }
