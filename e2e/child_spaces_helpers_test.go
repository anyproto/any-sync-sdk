package e2e

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"storj.io/drpc"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// childSpacesNetworkEnv names the network file of a network whose
// coordinator signs child spaces. Unset, the child-spaces suite is
// skipped. Set, every test of the suite runs against that file, and a
// network without the feature fails it.
const childSpacesNetworkEnv = "ANYSYNC_E2E_CHILD_SPACES_NETWORK"

// TestE2E_ChildSpaces runs the child-spaces tests as parallel subtests.
// It shortens the deletion-reconcile and inbox-poll cadences, process-wide
// seams a device reads when it opens, and restores them after the
// subtests. It is not parallel itself, so no other test of the package
// runs while the cadences are changed.
func TestE2E_ChildSpaces(t *testing.T) {
	net := childSpacesNetwork(t)
	t.Cleanup(spaceimpl.SetDeletionReconcileIntervalForTest(2 * time.Second))
	t.Cleanup(spaceimpl.SetOneToOneInboxIntervalsForTest(time.Second, time.Second))
	for _, tc := range []struct {
		name string
		run  func(*testing.T, *childNet)
	}{
		{"CreateAndLimit", childSpacesCreateAndLimit},
		{"CreateOffline", childSpacesCreateOffline},
		{"SharedSpacesLimitUntouched", childSpacesSharedSpacesLimitUntouched},
		{"Membership", childSpacesMembership},
		{"RemovalFromTheParent", childSpacesRemovalFromTheParent},
		{"OwnerAndDeletionFromTheParent", childSpacesOwnerAndDeletionFromTheParent},
		{"ParentTransfer", childSpacesParentTransfer},
		{"Scopes", childSpacesScopes},
		{"DeleteParent", childSpacesDeleteParent},
		{"DeleteParentOffline", childSpacesDeleteParentOffline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, net)
		})
	}
}

// childNet is the network the child-spaces suite runs against.
type childNet struct {
	yaml []byte
	// coordinator and tree are peer ids of the network file.
	coordinator string
	tree        []string
}

func childSpacesNetwork(t *testing.T) *childNet {
	t.Helper()
	path := os.Getenv(childSpacesNetworkEnv)
	if path == "" {
		t.Skipf("set %s to the network file of a network whose coordinator signs child spaces", childSpacesNetworkEnv)
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err, "network file named by %s", childSpacesNetworkEnv)
	n := &childNet{yaml: data}
	var conf struct {
		Nodes []struct {
			PeerId string   `yaml:"peerId"`
			Types  []string `yaml:"types"`
		} `yaml:"nodes"`
	}
	require.NoError(t, yaml.Unmarshal(data, &conf))
	for _, node := range conf.Nodes {
		for _, typ := range node.Types {
			switch typ {
			case "coordinator":
				n.coordinator = node.PeerId
			case "tree":
				n.tree = append(n.tree, node.PeerId)
			}
		}
	}
	require.NotEmpty(t, n.coordinator, "the network file names no coordinator")
	require.NotEmpty(t, n.tree, "the network file names no tree node")
	return n
}

// device is one SDK of an account: its own data dir and device key. It
// can be stopped and started again on the same data.
type device struct {
	name    string
	prov    *fixedSeedProvider
	dir     string
	conf    []byte
	sdk     *anysyncsdk.SDK
	account string
}

// account opens the first device of a new account.
func (n *childNet) account(t *testing.T, ctx context.Context, name string) *device {
	t.Helper()
	return n.open(t, ctx, name, newFixedSeedProvider(t), n.yaml)
}

// device opens another device of the account of d.
func (n *childNet) device(t *testing.T, ctx context.Context, d *device, name string) *device {
	t.Helper()
	return n.open(t, ctx, name, sameAccountFreshDevice(t, d.prov), n.yaml)
}

// open opens a device on the network file conf. Direct peers are off:
// everything a device gets comes through the nodes.
func (n *childNet) open(t *testing.T, ctx context.Context, name string, prov *fixedSeedProvider, conf []byte) *device {
	t.Helper()
	d := &device{name: name, prov: prov, dir: t.TempDir(), conf: conf}
	d.start(t, ctx)
	t.Cleanup(func() { d.stop() })
	return d
}

func (d *device) start(t *testing.T, ctx context.Context) {
	t.Helper()
	p2pOff := false
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: d.dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: d.conf},
		P2P:     config.P2P{Enabled: &p2pOff},
	}, d.prov)
	require.NoError(t, err, "%s: Open", d.name)
	d.sdk, d.account = sdk, sdk.Account().Id()
}

func (d *device) stop() {
	if d.sdk != nil {
		_ = d.sdk.Close()
		d.sdk = nil
	}
}

func (d *device) id() string { return d.account }

func (d *device) spaces() space.Service { return d.sdk.Spaces() }

// row returns the index row of spaceId on d.
func (d *device) row(ctx context.Context, spaceId string) (space.SpaceInfo, bool) {
	infos, err := d.spaces().List(ctx)
	if err != nil {
		return space.SpaceInfo{}, false
	}
	if info := findSpace(infos, spaceId); info != nil {
		return *info, true
	}
	return space.SpaceInfo{}, false
}

// waitRow waits until d holds a row of spaceId that ok accepts, and
// returns it. The space list is synced every few polls.
func (d *device) waitRow(t *testing.T, ctx context.Context, spaceId, what string, ok func(space.SpaceInfo) bool) space.SpaceInfo {
	t.Helper()
	var (
		last  space.SpaceInfo
		found bool
		polls int
	)
	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		if polls%8 == 0 {
			_ = d.spaces().SyncSpaceList(ctx)
		}
		polls++
		last, found = d.row(ctx, spaceId)
		return found && ok(last)
	}) {
		t.Fatalf("%s: %s: space %s; row found=%v %+v", d.name, what, spaceId, found, last)
	}
	return last
}

// get returns d's handle of spaceId, loading it when needed.
func (d *device) get(t *testing.T, ctx context.Context, spaceId string) space.Space {
	t.Helper()
	var (
		sp   space.Space
		last error
	)
	if !waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		sp, last = d.spaces().Get(ctx, spaceId)
		return last == nil
	}) {
		t.Fatalf("%s: space %s never loaded: %v", d.name, spaceId, last)
	}
	return sp
}

// create creates a top-level space on d.
func (d *device) create(t *testing.T, ctx context.Context, name string) space.Space {
	t.Helper()
	sp, err := d.spaces().Create(ctx, space.CreateRequest{Name: name})
	require.NoError(t, err, "%s: Create %s", d.name, name)
	return sp
}

// createChild creates a child of parentId on d. Local: the network
// signs it later, or refuses it.
func (d *device) createChild(t *testing.T, ctx context.Context, parentId, name string) space.Space {
	t.Helper()
	sp, err := d.spaces().CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parentId, Name: name})
	require.NoError(t, err, "%s: CreateChild %s", d.name, name)
	require.Equal(t, parentId, sp.Info().ParentSpaceId, "%s: the new child names its parent", d.name)
	return sp
}

// grant is an account to add to a space and its role there.
type grant struct {
	to   *device
	perm space.Permission
}

// addMembers adds the accounts of grants to sp in one direct-add record
// and has each accept the invite on its device. Returns each one's
// handle of sp, in the order of grants.
func addMembers(t *testing.T, ctx context.Context, sp space.Space, grants ...grant) []space.Space {
	t.Helper()
	adds := make([]space.MemberAdd, 0, len(grants))
	for _, g := range grants {
		adds = append(adds, space.MemberAdd{Identity: g.to.id(), Permission: g.perm})
	}
	require.NoError(t, sp.ACL().AddAccounts(ctx, adds), "AddAccounts to %s", sp.Info().Name)
	out := make([]space.Space, 0, len(grants))
	for _, g := range grants {
		out = append(out, acceptInvite(t, ctx, g.to, sp.Id()))
	}
	return out
}

// acceptInvite accepts on d the direct-add invite to spaceId once it
// arrives, and returns d's handle of the space.
func acceptInvite(t *testing.T, ctx context.Context, d *device, spaceId string) space.Space {
	t.Helper()
	d.waitRow(t, ctx, spaceId, "invite pending", func(si space.SpaceInfo) bool {
		return si.Status == space.StatusInvitePending
	})
	sp, err := d.spaces().AcceptInvite(ctx, spaceId)
	if err != nil {
		require.ErrorIs(t, err, space.ErrInviteAcceptPending, "%s: AcceptInvite", d.name)
	}
	d.waitRow(t, ctx, spaceId, "accepted", func(si space.SpaceInfo) bool {
		return si.Status == space.StatusActive
	})
	if sp == nil {
		sp = d.get(t, ctx, spaceId)
	}
	return sp
}

// children lists the children of parentId as the coordinator answers d.
func children(t *testing.T, ctx context.Context, d *device, parentId string) map[string]space.ChildStatus {
	t.Helper()
	list, err := d.spaces().Children(ctx, parentId)
	require.NoError(t, err, "%s: Children of %s", d.name, parentId)
	out := make(map[string]space.ChildStatus, len(list))
	for _, c := range list {
		out[c.Id] = c.Status
	}
	return out
}

// waitChildren waits until the coordinator's list of the children of
// parentId, as it answers d, satisfies ok. An error, such as a parent
// the coordinator does not know yet, is waited out too.
func waitChildren(t *testing.T, ctx context.Context, d *device, parentId, what string, ok func(map[string]space.ChildStatus) bool) map[string]space.ChildStatus {
	t.Helper()
	var (
		last    map[string]space.ChildStatus
		lastErr error
	)
	if !waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		list, err := d.spaces().Children(ctx, parentId)
		if lastErr = err; err != nil {
			return false
		}
		last = make(map[string]space.ChildStatus, len(list))
		for _, c := range list {
			last[c.Id] = c.Status
		}
		return ok(last)
	}) {
		t.Fatalf("%s: %s: children of %s are %v, last error %v", d.name, what, parentId, last, lastErr)
	}
	return last
}

// waitSigned waits until the coordinator lists childId as an active
// child of parentId: it signed the child. asker is an Admin or the
// Owner of the parent.
func waitSigned(t *testing.T, ctx context.Context, asker *device, parentId, childId string) {
	t.Helper()
	waitChildren(t, ctx, asker, parentId, "child "+childId+" signed", func(m map[string]space.ChildStatus) bool {
		return m[childId] == space.ChildStatusActive
	})
}

// waitDeletionFinal waits until the coordinator no longer lists childId
// under parentId: its deletion is final and it stops counting as a
// child. asker is an Admin or the Owner of the parent.
func waitDeletionFinal(t *testing.T, ctx context.Context, asker *device, parentId, childId string) {
	t.Helper()
	waitChildren(t, ctx, asker, parentId, "deletion of "+childId+" final", func(m map[string]space.ChildStatus) bool {
		_, listed := m[childId]
		return !listed
	})
}

// childMembers lists the accounts of childId as the nodes serve its ACL
// to d, by identity.
func childMembers(t *testing.T, ctx context.Context, d *device, childId string) map[string]space.ChildMember {
	t.Helper()
	list, err := d.spaces().ChildMembers(ctx, childId)
	require.NoError(t, err, "%s: ChildMembers of %s", d.name, childId)
	out := make(map[string]space.ChildMember, len(list))
	for _, m := range list {
		out[m.Identity] = m
	}
	return out
}

// waitChildMembers waits until the ACL of childId, as the nodes serve it
// to d, satisfies ok.
func waitChildMembers(t *testing.T, ctx context.Context, d *device, childId, what string, ok func(map[string]space.ChildMember) bool) {
	t.Helper()
	var (
		last    = map[string]space.ChildMember{}
		lastErr error
	)
	if !waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		list, err := d.spaces().ChildMembers(ctx, childId)
		if lastErr = err; err != nil {
			return false
		}
		last = make(map[string]space.ChildMember, len(list))
		for _, m := range list {
			last[m.Identity] = m
		}
		return ok(last)
	}) {
		t.Fatalf("%s: %s: members of %s are %+v, last error %v", d.name, what, childId, last, lastErr)
	}
}

// activeAs reports that identity is an active account of a child with
// permission perm.
func activeAs(members map[string]space.ChildMember, identity string, perm space.Permission) bool {
	m, ok := members[identity]
	return ok && m.Status == space.MemberStatusActive && m.Permission == perm
}

// writeNote writes a note into sp and returns its id. The note is a
// type named text: content encrypted with the space's read key.
func writeNote(t *testing.T, ctx context.Context, sp space.Space, text string) string {
	t.Helper()
	id, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: text})
	require.NoError(t, err, "write %q into %s", text, sp.Id())
	return id
}

// readsNote reports whether sp holds the note id decrypted, with its
// text.
func readsNote(ctx context.Context, sp space.Space, id, text string) bool {
	info, err := sp.Types().Get(ctx, id)
	return err == nil && info.Name == text
}

// waitNote waits until sp holds the note id with its text.
func waitNote(t *testing.T, ctx context.Context, who string, sp space.Space, id, text string) {
	t.Helper()
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		return readsNote(ctx, sp, id, text)
	}) {
		t.Fatalf("%s never read %q in %s", who, text, sp.Id())
	}
}

// neverNote checks for a while that sp does not hold the note id
// decrypted.
func neverNote(t *testing.T, ctx context.Context, who string, sp space.Space, id, text string, within time.Duration) {
	t.Helper()
	if waitFor(ctx, within, time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		return readsNote(ctx, sp, id, text)
	}) {
		t.Fatalf("%s read %q in %s", who, text, sp.Id())
	}
}

// waitRotationPending waits until sp's ACL on the device of who reads
// a pending rotation equal to want.
func waitRotationPending(t *testing.T, ctx context.Context, who string, sp space.Space, want bool) {
	t.Helper()
	var (
		last bool
		err  error
	)
	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sp.SyncHeads(ctx)
		last, err = sp.ACL().RotationPending(ctx)
		return err == nil && last == want
	}) {
		t.Fatalf("%s: RotationPending of %s never read %v: last %v, %v", who, sp.Id(), want, last, err)
	}
}

// waitChildRotationPending waits until the pending rotation of childId,
// as the nodes serve its ACL to d from the parent's side, reads want.
func waitChildRotationPending(t *testing.T, ctx context.Context, d *device, childId string, want bool) {
	t.Helper()
	var (
		last bool
		err  error
	)
	if !waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		last, err = d.spaces().ChildRotationPending(ctx, childId)
		return err == nil && last == want
	}) {
		t.Fatalf("%s: ChildRotationPending of %s never read %v: last %v, %v", d.name, childId, want, last, err)
	}
}

// coordinatorCall runs fn with a client of the coordinator, dialed from
// d with its account's identity.
func (n *childNet) coordinatorCall(ctx context.Context, d *device, fn func(coordinatorproto.DRPCCoordinatorClient) error) error {
	p, err := d.sdk.PoolInternal().Get(ctx, n.coordinator)
	if err != nil {
		return err
	}
	return rpcerr.Unwrap(p.DoDrpc(ctx, func(conn drpc.Conn) error {
		return fn(coordinatorproto.NewDRPCCoordinatorClient(conn))
	}))
}

// coordinatorStatus reads the coordinator's status of spaceId as it
// answers d.
func (n *childNet) coordinatorStatus(t *testing.T, ctx context.Context, d *device, spaceId string) coordinatorproto.SpaceStatus {
	t.Helper()
	var status coordinatorproto.SpaceStatus
	require.NoError(t, n.coordinatorCall(ctx, d, func(cl coordinatorproto.DRPCCoordinatorClient) error {
		resp, err := cl.SpaceStatusCheck(ctx, &coordinatorproto.SpaceStatusCheckRequest{SpaceId: spaceId})
		if err != nil {
			return err
		}
		status = resp.GetPayload().GetStatus()
		return nil
	}), "%s: status of %s", d.name, spaceId)
	return status
}

// spaceHeader pulls the raw header of spaceId from a tree node that
// holds it.
func (n *childNet) spaceHeader(t *testing.T, ctx context.Context, d *device, spaceId string) []byte {
	t.Helper()
	var errs []error
	for _, peerId := range n.tree {
		p, err := d.sdk.PoolInternal().Get(ctx, peerId)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var header []byte
		err = p.DoDrpc(ctx, func(conn drpc.Conn) error {
			resp, err := spacesyncproto.NewDRPCSpaceSyncClient(conn).SpacePull(ctx, &spacesyncproto.SpacePullRequest{Id: spaceId})
			if err != nil {
				return err
			}
			header = resp.GetPayload().GetSpaceHeader().GetRawHeader()
			return nil
		})
		if err == nil && len(header) > 0 {
			return header
		}
		errs = append(errs, err)
	}
	t.Fatalf("%s: no tree node served the header of %s: %v", d.name, spaceId, errors.Join(errs...))
	return nil
}

// proxiedNetwork rewrites the address of every node the SDK dials to a
// local proxy port. Returns the rewritten file and the proxy routes for
// startTCPProxies: with the proxies down a device on that file is
// offline, with them up it reaches the same network.
func proxiedNetwork(t *testing.T, netYaml []byte) ([]byte, map[string]string) {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(netYaml, &conf))
	nodes, _ := conf["nodes"].([]any)
	routes := map[string]string{}
	var proxied []map[string]any
	for _, n := range nodes {
		node, _ := n.(map[string]any)
		types, _ := node["types"].([]any)
		dialed := false
		for _, typ := range types {
			switch typ {
			case "coordinator", "consensus", "tree":
				dialed = true
			}
		}
		if dialed {
			proxied = append(proxied, node)
		}
	}
	base := freeBasePort(t, len(proxied))
	for i, node := range proxied {
		addrs, _ := node["addresses"].([]any)
		require.NotEmpty(t, addrs)
		real, _ := addrs[0].(string)
		proxy := fmt.Sprintf("127.0.0.1:%d", base+i)
		routes[proxy] = real
		node["addresses"] = []any{proxy}
	}
	out, err := yaml.Marshal(conf)
	require.NoError(t, err)
	return out, routes
}

// refusalLog records every refusal a device's space list reports for a
// set of spaces.
type refusalLog struct {
	mu   sync.Mutex
	seen []string
}

// watchRefusals records on log every refusal d's space list reports
// from now on for the spaces that ids accepts.
func watchRefusals(t *testing.T, d *device, ids func(string) bool) *refusalLog {
	log := &refusalLog{}
	cancel := d.spaces().Subscribe(func(ev space.SpaceListEvent) {
		for _, infos := range [][]space.SpaceInfo{ev.Added, ev.Updated} {
			for _, si := range infos {
				if si.Refusal != space.RefusalNone && ids(si.Id) {
					log.mu.Lock()
					log.seen = append(log.seen, d.name+": "+si.Id+" "+si.Refusal.String())
					log.mu.Unlock()
				}
			}
		}
	})
	t.Cleanup(cancel)
	return log
}

func (l *refusalLog) entries() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

// unknownSpaceId returns a well-formed space id the network does not
// know, with the replication key of like.
func unknownSpaceId(t *testing.T, like string) string {
	t.Helper()
	seed := make([]byte, 32)
	_, err := rand.Read(seed)
	require.NoError(t, err)
	return rawCidV1(t, seed).String() + "." + suffixAfter(like, '.')
}
