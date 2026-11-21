package anysyncx

import "github.com/anyproto/any-sync/commonspace/spacesyncproto"

// DecodeSpaceHeader parses a raw space header as storage and the wire
// carry it; nil when it does not parse.
func DecodeSpaceHeader(rawHeader []byte) *spacesyncproto.SpaceHeader {
	var raw spacesyncproto.RawSpaceHeader
	if err := raw.UnmarshalVT(rawHeader); err != nil {
		return nil
	}
	hdr := &spacesyncproto.SpaceHeader{}
	if err := hdr.UnmarshalVT(raw.SpaceHeader); err != nil {
		return nil
	}
	return hdr
}
