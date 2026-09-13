package snapshot

import (
	"testing"
	"time"
)

func TestOptionsZeroValueUsesSafeSASLifecycle(t *testing.T) {
	for _, options := range []Options{{}, {Duration: time.Minute}} {
		if options.SkipPreRevokeActiveSAS {
			t.Fatal("default Options unexpectedly skips pre-revocation")
		}
		if options.SkipRevokeOnCleanup {
			t.Fatal("default Options unexpectedly skips cleanup revocation")
		}
	}
}

func TestOptionsCanExplicitlySkipSASLifecycleSteps(t *testing.T) {
	options := Options{
		SkipPreRevokeActiveSAS: true,
		SkipRevokeOnCleanup:    true,
	}
	if !options.SkipPreRevokeActiveSAS || !options.SkipRevokeOnCleanup {
		t.Fatal("skip options were not preserved")
	}
}
