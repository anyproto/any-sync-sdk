// Devices registry — the public types for the tech-space `devices`
// dataset (SYN-165): one row per device of the account, plus the
// reader-side active-device election rule. The dataset is
// system-owned like the space list: reads go through the generic
// dataset surface (Service.Query / Datasets), writes only through the
// typed Service methods (SetDevice / ClaimActive / DeleteDevice).

package space

import "errors"

// Devices-registry sentinels — wrapped by the Service device methods
// so callers can classify with errors.Is.
var (
	// ErrDeviceBadApp rejects an app slug that is empty or contains a
	// '.' (slugs are single-level path segments under apps. /
	// activeClaims.).
	ErrDeviceBadApp = errors.New("invalid device app slug")
	// ErrDeviceBadValue rejects a non-scalar app info value (string /
	// bool / number only, stored as float64 — same vocabulary as
	// settings).
	ErrDeviceBadValue = errors.New("unsupported device app value")
	// ErrDeviceEmptyUpsert rejects a SetDevice call with nothing to
	// write.
	ErrDeviceEmptyUpsert = errors.New("device upsert is empty")
	// ErrDeviceUnknown is returned by DeleteDevice, and by a
	// ClaimActive naming a device (this one included), when peerId has
	// no live row in this replica's registry (a device that registered elsewhere may not have synced
	// here yet).
	ErrDeviceUnknown = errors.New("unknown device")
	// ErrDevicePruned reports that this device was pruned
	// (DeleteDevice) and its peer id can never re-register. SetDevice's
	// write is absorbed by the own row's sticky tombstone, and without
	// this error would be indistinguishable from success. ClaimActive
	// from a device already known to be pruned is refused without
	// writing.
	ErrDevicePruned = errors.New("device row is pruned")
	// ErrDeviceAppNotInstalled rejects a ClaimActive naming a device
	// (another one, or this one by its peer id) whose row doesn't carry
	// apps.<slug>: the election skips a claim whose target lacks the
	// app, and a named claim never marks an app installed.
	ErrDeviceAppNotInstalled = errors.New("app not installed on device")
	// ErrDeviceSelfDelete rejects DeleteDevice on the local device's
	// own row — the tombstone is sticky, so self-pruning would
	// permanently lock this installation out of the registry. Prune a
	// device from one of the account's other devices instead.
	ErrDeviceSelfDelete = errors.New("cannot delete own device row")
)

// Device is one row of the account's devices registry. All fields are
// synced account-wide via the tech space (owner-only ACL — invisible
// outside the account). Online status deliberately does not live here.
type Device struct {
	// PeerId is the device's libp2p peer id — the row id. Stable per
	// device installation. SetDevice and ClaimActive write only the
	// device's own row; DeleteDevice is the one write to another's.
	PeerId string

	// Name is the device's display name (hostname or user-set).
	Name string

	// OS is the device's operating system (runtime.GOOS vocabulary by
	// convention).
	OS string

	// Version is the device's engine build version.
	Version string

	// Apps is the set of installed apps keyed by slug (an open set —
	// nothing app-specific is hardcoded). Presence = installed; the
	// value is a free-form scalar bag (e.g. {"version": "1.2.3"}).
	// Nil when the device never registered an app.
	Apps map[string]map[string]any

	// ActiveClaims holds the claim this device made per app slug,
	// written by Service.ClaimActive — for itself or, via Target, for
	// another device. Resolve the winner with ActiveDevice — never by
	// comparing claims ad hoc.
	ActiveClaims map[string]DeviceClaim
}

// DeviceClaim is one active claim: writer-supplied data, NOT a CRDT
// version id (version ids are peer-locally allocated and not
// comparable across devices — see SYN-165).
type DeviceClaim struct {
	// Seq is max(existing seqs for the slug) + 1 at claim time;
	// highest wins.
	Seq int64
	// At is the claim wall-clock time in unix seconds — tiebreak on
	// equal Seq.
	At int64
	// Target is the peer id of the device the claim hands the app to;
	// "" means the claiming device itself.
	Target string
}

// DeviceUpsert is the input to Service.SetDevice. Only non-empty
// scalar fields are written; each Apps entry lands per-slug (a nil
// map value removes the slug — the uninstall signal), so writes
// touching different fields merge instead of clobbering.
type DeviceUpsert struct {
	Name    string
	OS      string
	Version string
	Apps    map[string]map[string]any
}

// ActiveDevice resolves which device is the active instance of app —
// THE single implementation of the election rule (SYN-165's top risk
// is two consumers deciding differently; UI, runtime, and the `any`
// server must all call this, never re-derive it).
//
// Every live row's claim for app is ranked by highest Seq, then
// highest At, then lexicographically largest claimer peer id; the
// winner is the target of the best claim whose target is a live row
// with the app installed (Apps[app] present). A claim for a device
// that uninstalled the app or was pruned never wins and the next
// claim is tried; a pruned claimer's claims vanish with its row. The
// claimer itself needs no app installed, so pass the whole registry
// (ListDevices): a list filtered to devices with the app drops the
// claims of devices without it. Deterministic on converged data for
// every reader. ok=false when no claim qualifies.
//
// No un-claim exists: the winner changes when a better claim appears,
// or when a claim starts or stops qualifying — its claimer or target
// is pruned, or its target uninstalls or reinstalls the app. A device
// holds one claim per app, so a hand-off replaces the claimer's own
// claim: if the hand-off later stops qualifying, the fallback does not
// return to the claimer.
//
// A claim with Seq <= 0 is treated as absent: ClaimActive mints seqs
// from 1, so a zero can only come from a malformed bag (unknown
// future writer, corrupt data) that decoded to the zero value — it
// must never beat genuinely-unclaimed rows.
//
// Known limit (v1): Seq is minted from the claiming replica's view,
// so a claim made on a not-yet-synced device can mint a lower Seq
// than an unseen earlier claim and lose once heads converge — the
// user's newest intent losing to an older one. Claims are cheap:
// re-claim after sync.
func ActiveDevice(devices []Device, app string) (peerId string, ok bool) {
	installed := make(map[string]bool, len(devices))
	for _, d := range devices {
		if _, has := d.Apps[app]; has {
			installed[d.PeerId] = true
		}
	}
	var win DeviceClaim
	var winClaimer string
	for _, d := range devices {
		c, claimed := d.ActiveClaims[app]
		if !claimed || c.Seq <= 0 {
			continue
		}
		target := c.Target
		if target == "" {
			target = d.PeerId
		}
		if !installed[target] {
			continue
		}
		better := c.Seq > win.Seq ||
			(c.Seq == win.Seq && c.At > win.At) ||
			(c.Seq == win.Seq && c.At == win.At && d.PeerId > winClaimer)
		if !ok || better {
			win, winClaimer, peerId, ok = c, d.PeerId, target, true
		}
	}
	return peerId, ok
}
