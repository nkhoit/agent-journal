package journalclient

import "testing"

func TestClientReportsWhetherTransportIsConfigured(t *testing.T) {
	if New(nil).TransportConfigured() {
		t.Fatal("nil transport reported configured")
	}
}
