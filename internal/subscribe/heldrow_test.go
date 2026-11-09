package subscribe

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var heldScope = Scope{ObjectId: "obj1", Dataset: "chat"}

// objectRows builds n object-like rows of about size encoded bytes
// each: an id, the sort key and string properties of 40-80 bytes.
func objectRows(n, size int) ([]row, int) {
	arena := newArena()
	rows := make([]row, n)
	var encoded int
	for i := range rows {
		obj := arena.NewObject()
		obj.Set("id", arena.NewString("bafyrei"+itoa(i)+strings.Repeat("x", 52)))
		obj.Set("n", arena.NewNumberInt(i))
		for f := 0; len(obj.MarshalTo(nil)) < size; f++ {
			obj.Set("prop"+itoa(f), arena.NewString(strings.Repeat("v", 40+f%40)))
		}
		encoded += len(obj.MarshalTo(nil))
		rows[i] = row{id: "r" + itoa(i), n: i, d: obj}
	}
	return rows, encoded / n
}

// heapPerRow returns the heap a windowed subscription holds per row
// once its snapshot of n rows is registered.
func heapPerRow(t *testing.T, rows []row) int {
	t.Helper()
	eng := New("test")
	defer eng.Close()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	sub := subscribeSorted(t, eng, heldScope, nil, len(rows)-1, rows)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(sub)
	runtime.KeepAlive(rows)
	return int((int64(after.HeapAlloc) - int64(before.HeapAlloc)) / int64(len(rows)))
}

// A windowed subscription holds each row at about its encoded size,
// not as a parsed copy.
func TestWindow_HeldRowCostsItsEncodedSize(t *testing.T) {
	for _, size := range []int{1024, 2700} {
		rows, encoded := objectRows(1001, size)
		perRow := heapPerRow(t, rows)
		t.Logf("encoded %d B/row, held %d B/row", encoded, perRow)
		require.Positive(t, perRow, "held row heap")
		require.Less(t, perRow, 2*encoded, "held row heap")
	}
}

// A promoted sentinel ships the row the window holds.
func TestWindow_PromotionShipsHeldRow(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	rows, _ := objectRows(3, 1024)
	sub := subscribeSorted(t, eng, heldScope, nil, 2, rows)

	fireEvent(eng, "obj1", "chat", rows[0].id, nil, true, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	require.Len(t, ev.Added, 1)
	require.Equal(t, rows[2].id, ev.Added[0].Id)
	require.Equal(t, rows[2].d.MarshalTo(nil), ev.Added[0].Doc.MarshalTo(nil))
}
