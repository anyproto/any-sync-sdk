package store

import "math/bits"

// bitmap tracks which sections of a partial CAR have arrived. Bit i is
// section i in file order.
type bitmap []byte

func newBitmap(n int) bitmap { return make(bitmap, (n+7)/8) }

func (b bitmap) set(i int)      { b[i/8] |= 1 << (i % 8) }
func (b bitmap) has(i int) bool { return i/8 < len(b) && b[i/8]&(1<<(i%8)) != 0 }

// count returns the number of set bits among the first n — padding
// bits past n (garbage in a corrupt persisted bitmap) never count.
func (b bitmap) count(n int) int {
	full := n / 8
	if full > len(b) {
		full = len(b)
	}
	c := 0
	for _, x := range b[:full] {
		c += bits.OnesCount8(x)
	}
	if rem := n % 8; rem > 0 && full < len(b) {
		c += bits.OnesCount8(b[full] & (1<<rem - 1))
	}
	return c
}

func (b bitmap) full(n int) bool { return b.count(n) >= n }
