package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

// lanFilePair is two devices of one account on the virtual LAN with a
// dead nodeconf: A attached two non-durable files, B sees both rows and
// holds none of their bytes. A can be closed and reopened on its data dir.
type lanFilePair struct {
	nodeConf  []byte
	dirA      string
	providerA *fixedSeedProvider

	sdkA, sdkB    *anysyncsdk.SDK
	spaceId       string
	spB           space.Space
	first, second space.FileInfo
	// firstContent / secondContent are the plaintext of first / second.
	firstContent, secondContent []byte
}

func newLANFilePair(t *testing.T, ctx context.Context, firstSize, secondSize int) *lanFilePair {
	t.Helper()
	yaml, err := loadLocalNetwork() // dead nodeconf: no coordinator, no file nodes
	require.NoError(t, err)

	lan := newVirtualLAN()
	sdkp2p.SetDriverFactory(func() sdkp2p.Driver { return lanDriver{lan} })
	t.Cleanup(func() { sdkp2p.SetDriverFactory(nil) })

	p := &lanFilePair{nodeConf: yaml, dirA: t.TempDir(), providerA: newFixedSeedProvider(t)}
	t.Cleanup(func() {
		if p.sdkA != nil {
			_ = p.sdkA.Close()
		}
	})
	p.openA(t, ctx)

	spA, err := p.sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "P2PFileHolder"})
	require.NoError(t, err)
	p.spaceId = spA.Id()
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)

	rnd := mrand.New(mrand.NewSource(7))
	attach := func(size int, name string) (space.FileInfo, []byte) {
		content := make([]byte, size)
		rnd.Read(content)
		info, err := spA.Files().Attach(ctx, objId, bytes.NewReader(content),
			space.AttachOpts{Name: name, Mime: "application/octet-stream"})
		require.NoError(t, err, "device A: Attach %s", name)
		require.False(t, info.Inline, "%s must be a full-tier file", name)
		require.False(t, info.Durable, "%s: no file node is reachable", name)
		return info, content
	}
	p.first, p.firstContent = attach(firstSize, "first.bin")
	p.second, p.secondContent = attach(secondSize, "second.bin")

	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p.sdkB, err = anysyncsdk.Open(ctx, p.config(t.TempDir()), &fixedSeedProvider{account: p.providerA.account, device: devB})
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = p.sdkB.Close() })

	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		return len(p.sdkA.P2PStatus().Peers) >= 1 && len(p.sdkB.P2PStatus().Peers) >= 1
	}), "devices never handshaked")
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = p.sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := p.sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), p.spaceId)
	}), "device B never learned the space over p2p")
	p.spB, err = p.sdkB.Spaces().Get(ctx, p.spaceId)
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		_ = p.spB.SyncHeads(ctx)
		_, err1 := p.spB.Files().Get(ctx, p.first.FileId)
		_, err2 := p.spB.Files().Get(ctx, p.second.FileId)
		return err1 == nil && err2 == nil
	}), "device B never saw the payloads rows")
	return p
}

func (p *lanFilePair) config(dir string) config.Config {
	return config.Config{
		Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: p.nodeConf},
	}
}

func (p *lanFilePair) openA(t *testing.T, ctx context.Context) {
	t.Helper()
	sdk, err := anysyncsdk.Open(ctx, p.config(p.dirA), p.providerA)
	require.NoError(t, err, "device A: Open")
	p.sdkA = sdk
}

func (p *lanFilePair) closeA(t *testing.T) {
	t.Helper()
	require.NoError(t, p.sdkA.Close())
	p.sdkA = nil
}

// readRange reads n bytes at off.
func readRange(r io.ReadSeeker, off int64, n int) ([]byte, error) {
	if _, err := r.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// readFile opens a file and reads all of it.
func readFile(ctx context.Context, sp space.Space, fileId string) ([]byte, error) {
	r, err := sp.Files().Open(ctx, fileId, space.VariantOriginal)
	if err != nil {
		return nil, err
	}
	got, err := io.ReadAll(r)
	if cErr := r.Close(); err == nil {
		err = cErr
	}
	return got, err
}

// lanPeerHasSpace reports whether sdk lists peerId as a LAN peer of spaceId.
func lanPeerHasSpace(sdk *anysyncsdk.SDK, peerId, spaceId string) bool {
	for _, p := range sdk.P2PStatus().Peers {
		if p.PeerId == peerId {
			return containsString(p.SpaceIds, spaceId)
		}
	}
	return false
}

// TestE2E_P2PFileHolderReturns proves a file's only holder is asked again
// seconds after it comes back. While it is gone B's reads of bytes it lacks
// fail as not available; once it returns B reads the whole file.
func TestE2E_P2PFileHolderReturns(t *testing.T) {
	if testing.Short() {
		t.Skip("p2p file e2e takes tens of seconds; rerun without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	p := newLANFilePair(t, ctx, 3_000_000, 400_000)
	content := p.firstContent

	// B holds only the tail of the first file.
	const span = 64 << 10
	tailOff := int64(len(content) - span)
	stale, err := p.spB.Files().Open(ctx, p.first.FileId, space.VariantOriginal)
	require.NoError(t, err)
	got, err := readRange(stale, tailOff, span)
	require.NoError(t, err, "tail read over p2p")
	require.Equal(t, content[tailOff:], got)

	peerA := p.sdkA.P2PStatus().PeerId
	p.closeA(t)

	_, err = p.spB.Files().Open(ctx, p.second.FileId, space.VariantOriginal)
	require.ErrorIs(t, err, space.ErrFileNotAvailable, "a file B never read, its only holder gone")
	require.True(t, lanPeerHasSpace(p.sdkB, peerA, p.spaceId), "B must still list A: the open above asked it")

	// A reader opened while A was there fails the missing range the same way.
	_, err = readRange(stale, 0, span)
	assert.ErrorIs(t, err, space.ErrFileNotAvailable, "reader opened before A left, missing range")
	require.NoError(t, stale.Close())

	// A fresh reader still serves the range B holds.
	fr, err := p.spB.Files().Open(ctx, p.first.FileId, space.VariantOriginal)
	require.NoError(t, err, "a partial local copy opens without its holder")
	got, err = readRange(fr, tailOff, span)
	require.NoError(t, err, "the range B holds reads without A")
	require.Equal(t, content[tailOff:], got)
	_, err = readRange(fr, 0, span)
	require.ErrorIs(t, err, space.ErrFileNotAvailable, "the range B lacks")
	require.NoError(t, fr.Close())

	p.openA(t, ctx)
	back := time.Now()
	var lastErr error
	require.True(t, waitFor(ctx, 2*time.Minute, 500*time.Millisecond, func() bool {
		got, lastErr = readFile(ctx, p.spB, p.first.FileId)
		return lastErr == nil && bytes.Equal(content, got)
	}), "B never read the file within 2m of A coming back; last error: %v", lastErr)
	t.Logf("B read the whole file %s after A came back", time.Since(back).Round(time.Millisecond))
}

// TestE2E_P2PFileTakeoverAfterRead proves a read that completes the local
// copy of a non-durable file queues its backup on the reading device, after
// the takeover stagger; a partial read queues nothing.
func TestE2E_P2PFileTakeoverAfterRead(t *testing.T) {
	if testing.Short() {
		t.Skip("p2p file e2e takes minutes; rerun without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	p := newLANFilePair(t, ctx, 400_000, 3_000_000)

	got, err := readFile(ctx, p.spB, p.first.FileId)
	require.NoError(t, err)
	require.Equal(t, p.firstContent, got)
	closed := time.Now()

	const span = 64 << 10
	fr, err := p.spB.Files().Open(ctx, p.second.FileId, space.VariantOriginal)
	require.NoError(t, err)
	_, err = readRange(fr, int64(len(p.secondContent)-span), span)
	require.NoError(t, err)
	require.NoError(t, fr.Close())

	status := func(fileId string) space.FileStatus {
		st, err := p.spB.Files().Status(ctx, fileId)
		require.NoError(t, err)
		return st
	}
	ran := func(st space.FileStatus) bool { return st.Attempts > 0 || st.LastErr != "" }

	st := status(p.first.FileId)
	require.True(t, st.Cached, "the full read completed B's copy")
	require.Equal(t, space.FileStateInFlight, st.State)
	require.False(t, ran(st), "the takeover waits out its stagger: %+v", st)
	require.False(t, status(p.second.FileId).Cached)

	// Stagger 45-90 s plus the queue's 15 s scan; the dead file node fails
	// the upload, which is what makes the job visible.
	require.True(t, waitFor(ctx, 150*time.Second, time.Second, func() bool {
		st = status(p.first.FileId)
		return ran(st)
	}), "no backup job ran on B for the file it read in full: %+v", st)
	elapsed := time.Since(closed)
	t.Logf("takeover job ran %s after the read closed: %+v", elapsed.Round(time.Second), st)
	require.Greater(t, elapsed, 40*time.Second, "the takeover is staggered")

	st = status(p.second.FileId)
	require.False(t, ran(st), "a partial read schedules no backup: %+v", st)
}
