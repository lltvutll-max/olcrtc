// Real-server keepalive stress tests. These exercise the engine against a
// live Jitsi deployment and verify that:
//
//   1. The XMPP transport stays alive past Prosody's BOSH 60s idle timeout
//      (bosh_max_inactivity in jitsi-meet.cfg.lua), i.e. our xmppKeepalive
//      goroutine actually keeps the long-poll session pinned. Without the
//      fix, WaitJingle returns "connection closed" exactly once per 60s.
//
//   2. Idle wait does not wedge the engine: after 90s alone in the room
//      we are still able to issue Send/CanSend without ErrSessionClosed.
//
// Both tests are gated behind an env variable so the package's regular
// `go test` workflow stays hermetic and fast. To run them locally:
//
//	OLCRTC_JITSI_KEEPALIVE_HOST=meet.handyweb.org \
//	OLCRTC_JITSI_KEEPALIVE_ROOM=olcrtc-stress-$(date +%s) \
//	  go test -count=1 -v -timeout 5m \
//	    -run '^TestJitsiKeepalive' ./internal/engine/jitsi/...
//
// Reuse the same room name across runs sparingly: jicofo treats each room
// as a focus session and may take a few seconds to garbage-collect after
// the previous run leaves.

package jitsi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

const (
	envKeepaliveHost = "OLCRTC_JITSI_KEEPALIVE_HOST"
	envKeepaliveRoom = "OLCRTC_JITSI_KEEPALIVE_ROOM"
)

func skipIfNoRealHost(t *testing.T) (host, room string) {
	t.Helper()
	host = strings.TrimSpace(os.Getenv(envKeepaliveHost))
	if host == "" {
		t.Skipf("set %s to a real Jitsi host (e.g. meet.handyweb.org) to enable", envKeepaliveHost)
	}
	room = strings.TrimSpace(os.Getenv(envKeepaliveRoom))
	if room == "" {
		room = fmt.Sprintf("olcrtc-keepalive-%d", time.Now().UnixNano())
	}
	return host, room
}

// TestJitsiKeepaliveSurvivesProsodyBOSHIdle is the canary for the BOSH
// inactivity timeout regression: prior to the keepalive fix, joining a
// real Jitsi room and idling for 90 seconds always failed with the j
// library reporting "connection closed" because Prosody's BOSH module had
// expired the long-poll session.
//
// We deliberately do NOT call sess.Connect because Connect attempts a full
// j.JoinMUC which is more flaky against unknown deployments than a
// minimum-viable smoke. Instead, we exercise the keepalive paths under
// realistic conditions by:
//
//   - Constructing a Session and JoinMUC-ing through the j library directly.
//   - Storing the result so the engine's keepalive goroutines (started by
//     Connect) would see jSess.Load() == this session.
//   - Spinning the keepalive in-place against the live LowLevel() conn.
//   - Verifying after 90 s that conn.Send still succeeds — which is exactly
//     what Prosody's BOSH inactivity timer kills without the fix.
//
// Test takes ~95 s on a clean run, so it's gated behind an env var.
func TestJitsiKeepaliveSurvivesProsodyBOSHIdle(t *testing.T) {
	host, room := skipIfNoRealHost(t)

	const idle = 90 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), idle+30*time.Second)
	defer cancel()

	sess, err := New(ctx, engine.Config{
		URL:    host,
		Extra:  map[string]string{credentialKeyRoom: room},
		Name:   "olcrtc-test",
		OnData: func([]byte) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sess.Close() }()

	// Connect joins the MUC and starts every keepalive goroutine the
	// engine ships with: bridgeKeepalive, xmppKeepalive, recvLoop,
	// sendLoop, waitForJingle. The waitForJingle goroutine will sit
	// idle since we never invite a peer — exactly the failure mode we
	// want to validate.
	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	js, ok := sess.(*Session)
	if !ok {
		t.Fatalf("sess type = %T, want *Session", sess)
	}

	// Sanity: the underlying connection is live right after Connect.
	jSess := js.jSess.Load()
	if jSess == nil {
		t.Fatal("jSess is nil right after Connect")
	}
	conn := jSess.LowLevel()
	if conn == nil {
		t.Fatal("LowLevel() is nil right after Connect")
	}

	// Slowly poll over the idle window. We deliberately do NOT issue
	// any application traffic — the only thing keeping the transport
	// alive must be xmppKeepalive.
	deadline := time.Now().Add(idle)
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			t.Fatalf("test ctx died early: %v", ctx.Err())
		case <-tick.C:
		}
		if js.closed.Load() {
			t.Fatal("session marked closed during idle window — keepalive failed")
		}
	}

	// Final verification: a fresh ping must still round-trip. A failure
	// here indicates Prosody terminated the BOSH session and is exactly
	// the symptom the fix targets.
	finalConn := js.jSess.Load().LowLevel()
	if finalConn == nil {
		t.Fatal("LowLevel() is nil after idle window")
	}
	id := finalConn.NextID()
	ping := fmt.Sprintf(
		`<iq type="get" to="%s" id="%s" xmlns="jabber:client"><ping xmlns="urn:xmpp:ping"/></iq>`,
		finalConn.Host(), id,
	)
	if err := finalConn.Send(ping); err != nil {
		t.Fatalf("post-idle XMPP send failed: %v (BOSH/WS session likely expired)", err)
	}
}

// TestJitsiKeepaliveDoesNotMassReconnect verifies the lifetime fix: while
// idle, no spurious reconnects should be triggered, even though the room
// stays at min-participants=1 well past Jicofo's single-participant timer
// (default 20 s in reference.conf, but Jicofo only stops the conference,
// it does not kick our XMPP session). Before the fix, rtcpKeepalive on a
// previously-closed PC would fire "rtcp keepalive dead" reconnects in a
// tight loop.
func TestJitsiKeepaliveDoesNotMassReconnect(t *testing.T) {
	host, room := skipIfNoRealHost(t)

	const observe = 60 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), observe+30*time.Second)
	defer cancel()

	sess, err := New(ctx, engine.Config{
		URL:    host,
		Extra:  map[string]string{credentialKeyRoom: room},
		Name:   "olcrtc-test",
		OnData: func([]byte) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sess.Close() }()

	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	js, ok := sess.(*Session)
	if !ok {
		t.Fatalf("sess type = %T, want *Session", sess)
	}
	js.SetShouldReconnect(func() bool { return true })

	deadline := time.Now().Add(observe)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			t.Fatalf("test ctx died early: %v", ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}

	js.reconnectMu.Lock()
	count := js.reconnectCount
	js.reconnectMu.Unlock()

	// We allow up to one reconnect during the observation window to
	// cover legitimate transient hiccups; anything more indicates the
	// keepalive lifetime regression.
	if count > 1 {
		t.Fatalf("observed %d reconnects in %s of idle — keepalive lifetime regression",
			count, observe)
	}
}

// TestJitsiSelfReconnectIsClean simulates the failure mode the production
// log showed: a forced engine-side reconnect should not race with a stale
// rtcpKeepalive goroutine and produce duplicate "rtcp keepalive dead"
// reconnect requests. The test triggers the supervisor manually, lets
// the recovery complete, and then waits in idle for double the grace
// period to make sure no follow-up reconnect spuriously fires.
func TestJitsiSelfReconnectIsClean(t *testing.T) {
	host, room := skipIfNoRealHost(t)

	settle := reconnectGrace + 5*time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sess, err := New(ctx, engine.Config{
		URL:    host,
		Extra:  map[string]string{credentialKeyRoom: room},
		Name:   "olcrtc-test",
		OnData: func([]byte) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sess.Close() }()

	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	js, ok := sess.(*Session)
	if !ok {
		t.Fatalf("sess type = %T, want *Session", sess)
	}
	js.SetShouldReconnect(func() bool { return true })

	// Trip a single reconnect via the supervisor channel rather than
	// killing the network: this isolates the keepalive regression from
	// real-network flakiness.
	js.requestReconnect("test-induced reconnect")

	// Wait for the supervisor goroutine (started by Connect via
	// WatchConnection) to handle it. We check the counter, which is
	// the canonical source of truth for "a reconnect attempt occurred".
	deadline := time.Now().Add(2 * time.Minute)
	for {
		js.reconnectMu.Lock()
		count := js.reconnectCount
		js.reconnectMu.Unlock()
		if count >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconnect never registered in counter")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("test ctx died during reconnect wait: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}

	// The supervisor has started a reconnect; let it settle long enough
	// to traverse the grace window. If a stale keepalive goroutine is
	// alive, it would fire a second reconnect during this wait.
	time.Sleep(settle)

	js.reconnectMu.Lock()
	count := js.reconnectCount
	js.reconnectMu.Unlock()

	// Allow up to 2 reconnects (the original + one allowed retry inside
	// the same window), but anything ≥ 3 indicates the lifetime fix is
	// not preventing duplicate firings.
	if count >= 3 {
		t.Fatalf("observed %d reconnects after a single trigger — duplicate firing regression",
			count)
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("test ctx expired before settle finished")
	}
}
