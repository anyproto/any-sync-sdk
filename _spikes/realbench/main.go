// Command realbench measures disk-I/O for reading blocks out of a real anytype
// flatfs, comparing storage layouts. It reconstructs files from dag-pb links,
// then focuses on the HOT path: many SMALL files (the big files are write-once
// archives that are essentially read-never, so only their storage size counts).
//
// Layouts compared, all reading with a COLD page cache (fsync + fadvise
// DONTNEED), accounted via /proc/self/io:
//
//   - flatfs      : real anytype store, one file per CID, sharded by hash.
//   - filedir     : one contiguous file per logical file (the SDK2 blocks/<hash> model).
//   - anystore    : any-store v2, one doc per block, fileId-prefix clustered.
//   - anystore-ch : same, but imported into a freelist fragmented by prior churn.
//
// Run: go run . -flatfs ~/.config/anytype/data/<acct>/flatfs
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/ipfs/go-cid"
	"golang.org/x/sys/unix"
)

// ---------- dag-pb link parsing (manual; no ipld deps) ----------
// PBNode { repeated PBLink Links = 2; optional bytes Data = 1; }
// PBLink { optional bytes Hash = 1; optional string Name = 2; optional uint64 Tsize = 3; }

type pbLink struct{ c cid.Cid }

func parseLinks(b []byte) []pbLink {
	var links []pbLink
	for len(b) > 0 {
		field, wire, n := readKey(b)
		if n == 0 {
			break
		}
		b = b[n:]
		if wire != 2 {
			break
		}
		l, m := binary.Uvarint(b)
		if m <= 0 || uint64(len(b)-m) < l {
			break
		}
		b = b[m:]
		payload := b[:l]
		b = b[l:]
		if field == 1 { // Data — canonically last; no more links
			break
		}
		if field == 2 {
			if lk, ok := parseLink(payload); ok {
				links = append(links, lk)
			}
		}
	}
	return links
}

func parseLink(b []byte) (pbLink, bool) {
	var lk pbLink
	var ok bool
	for len(b) > 0 {
		field, wire, n := readKey(b)
		if n == 0 {
			break
		}
		b = b[n:]
		switch {
		case field == 1 && wire == 2: // Hash = binary CID
			l, m := binary.Uvarint(b)
			if m <= 0 || uint64(len(b)-m) < l {
				return lk, false
			}
			b = b[m:]
			if c, err := cid.Cast(b[:l]); err == nil {
				lk.c, ok = c, true
			}
			b = b[l:]
		case wire == 2:
			l, m := binary.Uvarint(b)
			if m <= 0 || uint64(len(b)-m) < l {
				return lk, false
			}
			b = b[m+int(l):]
		case wire == 0:
			_, m := binary.Uvarint(b)
			if m <= 0 {
				return lk, false
			}
			b = b[m:]
		default:
			return lk, false
		}
	}
	return lk, ok
}

func readKey(b []byte) (field int, wire byte, n int) {
	k, m := binary.Uvarint(b)
	if m <= 0 {
		return 0, 0, 0
	}
	return int(k >> 3), byte(k & 7), m
}

// ---------- flatfs layout ----------

func flatPath(root string, c cid.Cid) string {
	key := strings.ToUpper(c.String())
	shard := "_"
	if len(key) >= 3 {
		shard = key[len(key)-3 : len(key)-1]
	}
	return filepath.Join(root, shard, key+".data")
}

// ---------- index + reconstruct ----------

type node struct {
	c     cid.Cid
	size  int64
	links []pbLink
}

func buildIndex(root string) (map[string]*node, error) {
	idx := map[string]*node{}
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".data") {
			return nil
		}
		base := strings.TrimSuffix(filepath.Base(p), ".data")
		c, err := cid.Decode(strings.ToLower(base))
		if err != nil {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		buf := make([]byte, 64*1024)
		m, _ := f.Read(buf)
		f.Close()
		idx[c.String()] = &node{c: c, size: info.Size(), links: parseLinks(buf[:m])}
		return nil
	})
	return idx, err
}

type file struct {
	root   cid.Cid
	blocks []cid.Cid // ordered: root + children (DAG order)
	bytes  int64
}

func reconstruct(idx map[string]*node) []file {
	linked := map[string]bool{}
	for _, n := range idx {
		for _, lk := range n.links {
			linked[lk.c.String()] = true
		}
	}
	var files []file
	for k, n := range idx {
		if linked[k] || len(n.links) == 0 {
			continue
		}
		seq := []cid.Cid{n.c}
		total := n.size
		for _, lk := range n.links {
			child := idx[lk.c.String()]
			if child == nil {
				continue
			}
			seq = append(seq, child.c)
			total += child.size
			if len(child.links) > 0 { // one more layer
				for _, gl := range child.links {
					if g := idx[gl.c.String()]; g != nil {
						seq = append(seq, g.c)
						total += g.size
					}
				}
			}
		}
		files = append(files, file{root: n.c, blocks: seq, bytes: total})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].bytes > files[j].bytes })
	return files
}

// ---------- I/O accounting ----------

type ioStat struct{ readBytes, syscr int64 }

func readIO() ioStat {
	var s ioStat
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		var v int64
		fmt.Sscanf(f[1], "%d", &v)
		switch strings.TrimSuffix(f[0], ":") {
		case "read_bytes":
			s.readBytes = v
		case "syscr":
			s.syscr = v
		}
	}
	return s
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
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
		}
		return nil
	})
	return total
}

// ---------- read sources ----------

type readResult struct {
	bytes int64
	io    ioStat
	wall  time.Duration
}

// build a contiguous file-dir copy: one file per logical file
func buildFileDir(dir string, flatfs string, files []file) {
	os.MkdirAll(dir, 0o755)
	for i, f := range files {
		out, _ := os.Create(filepath.Join(dir, fmt.Sprintf("f%08d.bin", i)))
		for _, c := range f.blocks {
			if d, err := os.ReadFile(flatPath(flatfs, c)); err == nil {
				out.Write(d)
			}
		}
		out.Close()
	}
}

func buildAnyStore(ctx context.Context, dbPath, flatfs string, files []file, churnFiles []file) error {
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
	insert := func(id string, data []byte) error {
		tx, _ := coll.WriteTx(ctx)
		arena.Reset()
		doc := arena.NewObject()
		doc.Set("id", arena.NewString(id))
		doc.Set("data", arena.NewBinary(data))
		if err := coll.Insert(tx.Context(), doc); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	// CHURN: import filler files, delete a scattered half -> freelist holes
	if churnFiles != nil {
		for ci, f := range churnFiles {
			for seq, c := range f.blocks {
				if d, err := os.ReadFile(flatPath(flatfs, c)); err == nil {
					insert(fmt.Sprintf("z%08d/%06d", ci, seq), d)
				}
			}
		}
		for ci := 0; ci < len(churnFiles); ci += 2 { // delete every other filler
			for seq := range churnFiles[ci].blocks {
				coll.DeleteId(ctx, fmt.Sprintf("z%08d/%06d", ci, seq))
			}
		}
	}
	// the test files: imported AFTER the holes exist (churn) -> fragmented
	for i, f := range files {
		for seq, c := range f.blocks {
			if d, err := os.ReadFile(flatPath(flatfs, c)); err == nil {
				insert(fmt.Sprintf("f%08d/%06d", i, seq), d)
			}
		}
	}
	return db.Close()
}

func coldReadFlatfs(flatfs string, files []file) readResult {
	for _, f := range files {
		for _, c := range f.blocks {
			evict(flatPath(flatfs, c))
		}
	}
	io0, t := readIO(), time.Now()
	var n int64
	for _, f := range files {
		for _, c := range f.blocks {
			if d, err := os.ReadFile(flatPath(flatfs, c)); err == nil {
				n += int64(len(d))
			}
		}
	}
	w := time.Since(t)
	return readResult{n, diff(readIO(), io0), w}
}

func coldReadFileDir(dir string, files []file) readResult {
	for i := range files {
		evict(filepath.Join(dir, fmt.Sprintf("f%08d.bin", i)))
	}
	io0, t := readIO(), time.Now()
	var n int64
	for i := range files {
		if d, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("f%08d.bin", i))); err == nil {
			n += int64(len(d))
		}
	}
	w := time.Since(t)
	return readResult{n, diff(readIO(), io0), w}
}

func coldReadAnyStore(ctx context.Context, dbPath string, files []file) readResult {
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
	var n int64
	for i, f := range files {
		for seq := range f.blocks {
			if doc, err := coll.FindIdWithParser(ctx, p, fmt.Sprintf("f%08d/%06d", i, seq)); err == nil {
				n += int64(len(doc.Value().GetBytes("data")))
			}
		}
	}
	w := time.Since(t)
	r := readResult{n, diff(readIO(), io0), w}
	db.Close()
	return r
}

func diff(b, a ioStat) ioStat { return ioStat{b.readBytes - a.readBytes, b.syscr - a.syscr} }
func mib(b int64) float64     { return float64(b) / (1024 * 1024) }

func report(name string, blocks int, r readResult) {
	fmt.Printf("%-14s %8.1f %10d %12.1f %10d %10s\n",
		name, mib(r.bytes), blocks, mib(r.io.readBytes), r.io.syscr, r.wall.Round(time.Millisecond))
}

func main() {
	flatfs := flag.String("flatfs", "", "path to a real flatfs directory")
	smallMin := flag.Int64("small-min", 0, "min logical bytes for a 'small' file")
	smallMax := flag.Int64("small-max", 1<<20, "max logical bytes for a 'small' file")
	smallN := flag.Int("small-n", 1500, "number of small files in the hot-read set")
	churn := flag.Bool("churn", true, "also build a churn-fragmented any-store and compare")
	flag.Parse()
	if *flatfs == "" {
		fmt.Println("need -flatfs <dir>")
		os.Exit(1)
	}
	ctx := context.Background()

	fmt.Println("indexing flatfs (parsing dag-pb links)…")
	idx, err := buildIndex(*flatfs)
	if err != nil {
		panic(err)
	}
	files := reconstruct(idx)

	// split: archives (big, read-never) vs small (hot path)
	var small, archives []file
	for _, f := range files {
		if f.bytes >= *smallMin && f.bytes <= *smallMax {
			small = append(small, f)
		} else if f.bytes > *smallMax {
			archives = append(archives, f)
		}
	}
	if len(small) > *smallN {
		small = small[len(small)-*smallN:] // smallest ones
	}
	var smallBlocks int
	var smallBytes int64
	for _, f := range small {
		smallBlocks += len(f.blocks)
		smallBytes += f.bytes
	}
	fmt.Printf("reconstructed %d files; %d archives (>%d B, read-never); hot set = %d small files, %d blocks, %.1f MiB\n",
		len(files), len(archives), *smallMax, len(small), smallBlocks, mib(smallBytes))

	stage, _ := os.MkdirTemp(filepath.Join(os.Getenv("HOME"), ".cache"), "realbench-")
	defer os.RemoveAll(stage)

	fmt.Println("building layouts (filedir, anystore, churned)…")
	fileDir := filepath.Join(stage, "filedir")
	buildFileDir(fileDir, *flatfs, small)
	anyDB := filepath.Join(stage, "any.db")
	if err := buildAnyStore(ctx, anyDB, *flatfs, small, nil); err != nil {
		panic(err)
	}
	churnDB := filepath.Join(stage, "churn.db")
	if *churn {
		// fillers = a slab of other small files to churn through the freelist
		fillers := small
		if len(archives) > 0 {
			fillers = append([]file{}, small...) // reuse small set as fillers too
		}
		if err := buildAnyStore(ctx, churnDB, *flatfs, small, fillers); err != nil {
			panic(err)
		}
	}

	// storage sizes (real allocation) for the small-file set
	var flatSmallAlloc int64
	for _, f := range small {
		for _, c := range f.blocks {
			if st, err := os.Stat(flatPath(*flatfs, c)); err == nil {
				if s, ok := st.Sys().(*syscall.Stat_t); ok {
					flatSmallAlloc += s.Blocks * 512
				}
			}
		}
	}
	fmt.Printf("\nstorage for the %d-file hot set: flatfs=%.1f MiB  filedir=%.1f MiB  anystore=%.1f MiB  (logical %.1f MiB)\n",
		len(small), mib(flatSmallAlloc), mib(allocSize(fileDir)), mib(allocSize(anyDB)), mib(smallBytes))

	fmt.Printf("\ncold read of ALL %d small files (%d blocks), per /proc/self/io:\n", len(small), smallBlocks)
	fmt.Printf("%-14s %8s %10s %12s %10s %10s\n", "layout", "MiB", "blocks", "disk_MiB", "syscr", "wall")
	fmt.Println(strings.Repeat("-", 70))
	report("flatfs", smallBlocks, coldReadFlatfs(*flatfs, small))
	report("filedir", smallBlocks, coldReadFileDir(fileDir, small))
	report("anystore", smallBlocks, coldReadAnyStore(ctx, anyDB, small))
	if *churn {
		report("anystore-churn", smallBlocks, coldReadAnyStore(ctx, churnDB, small))
	}
	fmt.Printf("\ndisk_MiB = bytes pulled from the block device (cold). syscr = read syscalls.\n")
}
