package p2p

import "testing"

func TestPowerHintSubscribe(t *testing.T) {
	t.Cleanup(func() { SetPowerHint(PowerNormal) })
	var got []PowerHint
	cancel := SubscribePowerHint(func(h PowerHint) { got = append(got, h) })
	SetPowerHint(PowerLow)
	if CurrentPowerHint() != PowerLow || len(got) != 1 || got[0] != PowerLow {
		t.Fatalf("hint not delivered: cur=%v got=%v", CurrentPowerHint(), got)
	}
	cancel()
	cancel()
	SetPowerHint(PowerNormal)
	if len(got) != 1 {
		t.Fatalf("cancelled subscriber still called: %v", got)
	}
	if PowerLow.String() != "low" || PowerNormal.String() != "normal" {
		t.Fatal("string tokens")
	}
}
