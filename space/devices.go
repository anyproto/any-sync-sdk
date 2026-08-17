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
	// ErrDeviceUnknown is returned by DeleteDevice when peerId has no
	// live row.
	ErrDeviceUnknown = errors.New("unknown device")
)

// Device is one row of the account's devices registry. All fields are
// synced account-wide via the tech space (owner-only ACL — invisible
// outside the account). Online status deliberately does not live here.
type Device struct {
	// PeerId is the device's libp2p peer id — the row id. Stable per
	// device installation; every device writes only its own row.
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

	// ActiveClaims holds the device's active claim per app slug,
	// written by Service.ClaimActive. Resolve the winner with
	// ActiveDevice — never by comparing claims ad hoc.
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
// Deterministic on converged data for every reader: among live rows
// that carry the app installed (Apps[app] present — a dangling claim
// on a device that uninstalled the app never wins; a pruned device
// has no row at all), the claim with the highest Seq wins, ties
// broken by highest At, then by lexicographically largest peer id.
// ok=false when no device qualifies.
func ActiveDevice(devices []Device, app string) (peerId string, ok bool) {
	var win DeviceClaim
	for _, d := range devices {
		if _, installed := d.Apps[app]; !installed {
			continue
		}
		c, claimed := d.ActiveClaims[app]
		if !claimed {
			continue
		}
		better := c.Seq > win.Seq ||
			(c.Seq == win.Seq && c.At > win.At) ||
			(c.Seq == win.Seq && c.At == win.At && d.PeerId > peerId)
		if !ok || better {
			win, peerId, ok = c, d.PeerId, true
		}
	}
	return peerId, ok
}
