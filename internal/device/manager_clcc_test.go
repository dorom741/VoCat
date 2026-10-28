package device

import (
	"context"
	"testing"
	"time"

	"vocat/internal/modem"
)

// newCLCCFamilyTestManager starts a manager over one candidate so the
// capability probe sees the resolved chipset profile.
func newCLCCFamilyTestManager(t *testing.T, vendorID, productID string) (*Manager, string) {
	t.Helper()
	id := "clcc-" + vendorID + "-" + productID
	manager, err := NewManager(Options{
		Discoverer: staticDiscoverer{candidates: []modem.Candidate{{
			ID:        id,
			VendorID:  vendorID,
			ProductID: productID,
			ATPort: modem.Port{
				Path: "/dev/ttyUSB0",
				Name: "ttyUSB0",
				Role: modem.PortRoleAT,
			},
		}}},
		Opener:         &staticOpener{client: &zxicScriptedClient{responses: map[string][]string{}}},
		CommandTimeout: time.Second,
		LongTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = manager.Stop(context.Background()) })
	return manager, id
}

// The call-list capability is a property of the firmware dialect, resolved
// from the chipset family. Pollers ask the manager instead of pattern
// matching USB identities at each call site.
func TestManagerSupportsCLCCFollowsChipsetProfile(t *testing.T) {
	zxicManager, zxicID := newCLCCFamilyTestManager(t, "19d2", "0536")
	if zxicManager.SupportsCLCC(zxicID) {
		t.Fatal("ZXIC SupportsCLCC = true, want false (firmware answers +CME ERROR: 6003)")
	}

	// Every product of the family shares the platform dialect, so an
	// unlisted ZXIC product inherits the capability answer.
	familyManager, familyID := newCLCCFamilyTestManager(t, "19d2", "ffff")
	if familyManager.SupportsCLCC(familyID) {
		t.Fatal("other ZXIC family product SupportsCLCC = true, want false")
	}

	quectelManager, quectelID := newCLCCFamilyTestManager(t, "2c7c", "0125")
	if !quectelManager.SupportsCLCC(quectelID) {
		t.Fatal("Quectel SupportsCLCC = false, want true")
	}
}

// A device that is not (or no longer) present must not make the caller skip
// polling: the caller's own presence checks decide, and the historical
// behavior for every unadapted modem is to poll.
func TestManagerSupportsCLCCUnknownDeviceDefaultsToTrue(t *testing.T) {
	manager, _ := newCLCCFamilyTestManager(t, "2c7c", "0125")
	if !manager.SupportsCLCC("no-such-device") {
		t.Fatal("SupportsCLCC for unknown device = false, want true")
	}
}
