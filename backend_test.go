package enki

import (
	"encoding/binary"
	"fmt"
)

func ExampleIntToBytes() {
	timestamps := []int64{1432808440, 1432808442, 1432808454}

	for _, ts := range timestamps {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(ts))
		fmt.Println(key)
	}
	// Output:
	// [0 0 0 0 85 102 235 248]
	// [0 0 0 0 85 102 235 250]
	// [0 0 0 0 85 102 236 6]
}
