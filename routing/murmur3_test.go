package routing

import "testing"

// same as https://github.com/elastic/elasticsearch/blob/6.8/server/src/test/java/org/elasticsearch/cluster/routing/operation/hash/murmur3/Murmur3HashFunctionTests.java
func TestMurmur3HashFunction(t *testing.T) {
	tests := []struct {
		input  string
		result int
	}{
		{"hell", 0x5a0cb7c3},
		{"hello", 0xd7c31989},
		{"hello w", 0x22ab2984},
		{"hello wo", 0xdf0ca123},
		{"hello wor", 0xe7744d61},
		{"The quick brown fox jumps over the lazy dog", 0xe07db09c},
		{"The quick brown fox jumps over the lazy cog", 0x4e63d2ad},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			r, err := Murmur3HashFunction(test.input)
			if err != nil {
				t.Errorf("error occured %v", err)
			}
			if r != test.result {
				t.Errorf("failed exp=%v got=%v", test.result, r)
			}
		})
	}
}
