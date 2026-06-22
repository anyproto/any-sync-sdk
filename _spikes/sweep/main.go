// Command sweep finds the exact block-size crossover where an any-store v2
// document stops packing inline in a btree leaf and starts spilling to a 4 KiB
// overflow chain — the boundary that separates "small, churn-immune, dense" from
// "large, fragments-under-churn, IOPS-bound".
//
// For each payload size it reports:
//   - disk/doc : on-disk allocation per document (jumps to ~4 KiB at overflow)
//   - syscr/doc: read syscalls per doc on a cold read (jumps <1 -> >=1 at overflow)
//   - fresh ms / churn ms: cold read of all docs, fresh db vs one fragmented by churn
//
// Run: go run . -n 8000
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"golang.org/x/sys/unix"
)

func docID(i int) string { return fmt.Sprintf("f%08d/%06d", i/64, i%64) } // clustered

func genBlock(size int) []byte {
	b := make([]byte, size)
	rand.Read(b) // incompressible, like ciphertext
	return b
}

type ioStat struct{ readBytes, syscr int64 }

func readIO() ioStat {
	var s ioStat
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range splitLines(b) {
		var name string
		var v int64
		if _, err := fmt.Sscanf(line, "%s %d", &name, &v); err != nil {
			continue
		}
		switch name {
		case "read_bytes:":
			s.readBytes = v
		case "syscr:":
			s.syscr = v
		}
	}
	return s
}

func splitLines(b []byte) []string {
	var out []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, string(b[start:i]))
			start = i + 1
		}
	}
	return out
}

func evict(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	f.Close()
}

func allocSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if s, ok := st.Sys().(*syscall.Stat_t); ok {
		return s.Blocks * 512
	}
	return 0
}

// build an any-store with n docs of `size` bytes. If churn>0, first import
// `churn` filler docs, delete every other one (punch freelist holes), THEN
// import the n test docs so they draw from the fragmented freelist.
func build(ctx context.Context, dbPath string, size, n, churn int) error {
	db, err := anystore.Open(ctx, dbPath, &anystore.Config{})
	if err != nil {
		return err
	}
	coll, err := db.CreateCollection(ctx, "blobs", anystore.CollectionOptions{
		Compression: anystore.NoCompression, PrimaryKey: "id",
	})
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	ins := func(id string, data []byte) error {
		tx, _ := coll.WriteTx(ctx)
		arena.Reset()
		d := arena.NewObject()
		d.Set("id", arena.NewString(id))
		d.Set("data", arena.NewBinary(data))
		if err := coll.Insert(tx.Context(), d); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	if churn > 0 {
		for i := 0; i < churn; i++ {
			if err := ins(fmt.Sprintf("z%08d", i), genBlock(size)); err != nil {
				return err
			}
		}
		for i := 0; i < churn; i += 2 {
			coll.DeleteId(ctx, fmt.Sprintf("z%08d", i))
		}
	}
	for i := 0; i < n; i++ {
		if err := ins(docID(i), genBlock(size)); err != nil {
			return err
		}
	}
	return db.Close()
}

func coldRead(ctx context.Context, dbPath string, n int) (ioStat, time.Duration) {
	unix.Sync()
	evict(dbPath)
	evict(dbPath + "-wal")
	db, err := anystore.Open(ctx, dbPath, &anystore.Config{})
	if err != nil {
		panic(err)
	}
	coll, _ := db.OpenCollection(ctx, "blobs")
	p := &anyenc.Parser{}
	io0, t := readIO(), time.Now()
	for i := 0; i < n; i++ {
		if doc, err := coll.FindIdWithParser(ctx, p, docID(i)); err == nil {
			_ = doc.Value().GetBytes("data")
		}
	}
	w := time.Since(t)
	r := ioStat{readBytes: readIO().readBytes - io0.readBytes, syscr: readIO().syscr - io0.syscr}
	db.Close()
	return r, w
}

func main() {
	n := flag.Int("n", 8000, "docs per size")
	flag.Parse()
	ctx := context.Background()

	sizes := []int{800, 940, 1000, 2000, 3000, 3500, 4000, 4300, 4500, 4600, 4800, 5000, 6000, 8000, 8200, 8600, 12000}

	stage, _ := os.MkdirTemp(filepath.Join(os.Getenv("HOME"), ".cache"), "sweep-")
	defer os.RemoveAll(stage)

	fmt.Printf("any-store v2 overflow crossover sweep, %d docs/size, clustered ids, NoCompression\n", *n)
	fmt.Printf("%6s %10s %10s %10s %10s %12s\n", "bytes", "disk/doc", "syscr/doc", "fresh ms", "churn ms", "churn us/doc")
	fmt.Println("---------------------------------------------------------------------")
	for _, s := range sizes {
		fresh := filepath.Join(stage, fmt.Sprintf("f%d.db", s))
		if err := build(ctx, fresh, s, *n, 0); err != nil {
			panic(err)
		}
		diskPerDoc := float64(allocSize(fresh)) / float64(*n)
		frIO, frW := coldRead(ctx, fresh, *n)
		os.Remove(fresh)
		os.Remove(fresh + "-wal")

		churn := filepath.Join(stage, fmt.Sprintf("c%d.db", s))
		if err := build(ctx, churn, s, *n, *n); err != nil {
			panic(err)
		}
		_, chW := coldRead(ctx, churn, *n)
		os.Remove(churn)
		os.Remove(churn + "-wal")

		fmt.Printf("%6d %10.0f %10.2f %10s %10s %12.1f\n",
			s, diskPerDoc, float64(frIO.syscr)/float64(*n),
			frW.Round(time.Millisecond), chW.Round(time.Millisecond),
			float64(chW.Microseconds())/float64(*n))
	}
	fmt.Println("\ndisk/doc jumps to ~4096 and syscr/doc crosses 1.0 at the inline->overflow boundary;")
	fmt.Println("churn ms diverges from fresh ms once docs use overflow chains.")
}
