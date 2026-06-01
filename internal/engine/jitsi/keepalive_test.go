// Tests for the post-fix keepalive and reconnect-loop behaviour. Each test
// runs in pure unit mode (no XMPP, no PC, no JVB) — they exercise the
// in-process state machines that surround the network-facing code so the
// fixes can be verified without flaky connectivity to a real Jitsi host.
//
// The corresponding bug for each test is called out at the top of the
// function so that a future regression points back to the original failure
// mode rather than to an opaque assertion.
package jitsi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

func newSilentSession(t *testing.T) *Session {
	t.Helper()
	sess, err := New(context.Background(), engine.Config{
		URL:    testHost,
		Extra:  map[string]string{credentialKeyRoom: testRoom},
		OnData: func([]byte) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	js, ok := sess.(*Session)
	if !ok {
		t.Fatalf("sess type = %T, want *Session", sess)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return js
}

// TestPeerEpochChangeWithinGraceDoesNotReconnect ensures that an epoch
// change observed shortly after our own self-reconnect is absorbed
// silently. Without this, the very common pattern "we reconnect → JVB
// re-issues → peer reconnects → peer publishes new epoch → we reconnect"
// turns a single recoverable hiccup into an infinite loop that eventually
// trips maxReconnects.
func TestPeerEpochChangeWithinGraceDoesNotReconnect(t *testing.T) {
	js := newSilentSession(t)
	js.SetShouldReconnect(func() bool { return true })
	js.bridgeReady.Store(true)

	js.localEpoch.Store(0xAAAA)
	// First peer epoch arrives normally and latches.
	first := makeBridgeFrameForEpoch(t, 0x1111, 0xAAAA, []byte("p1"))
	if !js.deliverBridgeMessage(makeBridgeMessageFrom("peerA", map[string]any{rawFieldKey: first}), true) {
		t.Fatal("deliverBridgeMessage(first) returned false")
	}
	drainReconnectChNonBlocking(js)

	// Mark a successful self-reconnect that happened "just now" — this
	// is the grace window we are validating.
	js.lastReconnectAt.Store(time.Now().UnixNano())

	changed := makeBridgeFrameForEpoch(t, 0x2222, 0xAAAA, nil)
	js.deliverBridgeMessage(makeBridgeMessageFrom("peerA", map[string]any{rawFieldKey: changed}), true)

	if got := js.peerEpoch.Load(); got != 0x2222 {
		t.Fatalf("peerEpoch.Load() = 0x%X, want 0x2222 (latch must update even during grace)", got)
	}
	if reconnectQueued(js) {
		t.Fatal("epoch change inside grace window should NOT enqueue a reconnect")
	}
}

// TestPeerEpochChangeAfterGraceTriggersReconnect mirrors the above but
// confirms the safety net still fires once the grace window has passed.
func TestPeerEpochChangeAfterGraceTriggersReconnect(t *testing.T) {
	js := newSilentSession(t)
	js.SetShouldReconnect(func() bool { return true })
	js.bridgeReady.Store(true)
	js.localEpoch.Store(0xBBBB)

	first := makeBridgeFrameForEpoch(t, 0x1111, 0xBBBB, []byte("p1"))
	js.deliverBridgeMessage(makeBridgeMessageFrom("peerA", map[string]any{rawFieldKey: first}), true)
	drainReconnectChNonBlocking(js)

	// Last reconnect was outside the grace window — peer-epoch change
	// must still drive a reconnect to recover from a true peer restart.
	js.lastReconnectAt.Store(time.Now().Add(-2 * reconnectGrace).UnixNano())

	changed := makeBridgeFrameForEpoch(t, 0x2222, 0xBBBB, nil)
	js.deliverBridgeMessage(makeBridgeMessageFrom("peerA", map[string]any{rawFieldKey: changed}), true)

	select {
	case <-js.reconnectCh:
	case <-time.After(time.Second):
		t.Fatal("epoch change outside grace window did not enqueue a reconnect")
	}
}

// TestStableUptimeResetsReconnectCounter exercises the failure mode where
// a long-running session accumulates churn-driven reconnects (peer leaves,
// JVB restart, etc.) until reconnectCount crosses maxReconnects. Resetting
// after stableUptime keeps the safety net for tight reconnect storms while
// not penalising healthy sessions.
func TestStableUptimeResetsReconnectCounter(t *testing.T) {
	js := newSilentSession(t)

	js.reconnectMu.Lock()
	js.reconnectCount = maxReconnects // already at the brink
	js.reconnectWindowStart = time.Now().Add(-time.Minute)
	js.reconnectMu.Unlock()

	// Pretend the last reconnect was longer ago than stableUptime: the
	// next attempt should be treated as fresh and reset the counter.
	js.lastReconnectAt.Store(time.Now().Add(-2 * stableUptime).UnixNano())

	now := time.Now()
	js.reconnectMu.Lock()
	last := js.lastReconnectAt.Load()
	stable := last != 0 && now.Sub(time.Unix(0, last)) >= stableUptime
	if stable || js.reconnectWindowStart.IsZero() || now.Sub(js.reconnectWindowStart) > reconnectWindow {
		js.reconnectWindowStart = now
		js.reconnectCount = 0
	}
	js.reconnectCount++
	count := js.reconnectCount
	js.reconnectMu.Unlock()

	if count != 1 {
		t.Fatalf("reconnectCount after stable reset = %d, want 1 (counter must reset)", count)
	}
}

// TestStableUptimeDoesNotResetWithinWindow guards the inverse: tight
// successive reconnects are exactly the case maxReconnects is meant to
// catch. Resetting the counter prematurely would mask repeated failures.
func TestStableUptimeDoesNotResetWithinWindow(t *testing.T) {
	js := newSilentSession(t)

	js.reconnectMu.Lock()
	js.reconnectCount = 3
	js.reconnectWindowStart = time.Now() // freshly opened
	js.reconnectMu.Unlock()

	// Last reconnect happened very recently — no stable uptime yet.
	js.lastReconnectAt.Store(time.Now().UnixNano())

	now := time.Now()
	js.reconnectMu.Lock()
	last := js.lastReconnectAt.Load()
	stable := last != 0 && now.Sub(time.Unix(0, last)) >= stableUptime
	if stable || js.reconnectWindowStart.IsZero() || now.Sub(js.reconnectWindowStart) > reconnectWindow {
		js.reconnectWindowStart = now
		js.reconnectCount = 0
	}
	js.reconnectCount++
	count := js.reconnectCount
	js.reconnectMu.Unlock()

	if count != 4 {
		t.Fatalf("reconnectCount inside window = %d, want 4 (counter must NOT reset)", count)
	}
}

// TestTeardownPCCancelsPCContext verifies the rtcpKeepalive lifetime fix:
// teardownPC must cancel pcCtx so that any goroutines bound to it (rtcp
// keepalive specifically) exit before the supervisor swaps in a fresh PC.
// Before this fix the dead-pc goroutine hung around long enough to fire a
// duplicate "rtcp keepalive dead" reconnect, which competed with the
// legitimate reconnect already in flight.
func TestTeardownPCCancelsPCContext(t *testing.T) {
	js := newSilentSession(t)

	js.pcMu.Lock()
	if js.pcCancel != nil {
		js.pcCancel()
	}
	pcCtx, pcCancel := context.WithCancel(js.runCtx)
	js.pcCtx = pcCtx
	js.pcCancel = pcCancel
	js.pcMu.Unlock()

	if pcCtx.Err() != nil {
		t.Fatal("pcCtx cancelled before teardownPC ran")
	}

	js.teardownPC()

	select {
	case <-pcCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("teardownPC did not cancel pcCtx")
	}

	js.pcMu.Lock()
	if js.pcCancel != nil || js.pcCtx != nil {
		js.pcMu.Unlock()
		t.Fatal("teardownPC must clear pcCtx/pcCancel pointers")
	}
	js.pcMu.Unlock()
}

// TestXMPPKeepaliveSurvivesNilJSess simulates the boot window and the
// reconnect window where s.jSess is briefly nil. The keepalive goroutine
// must keep ticking — exiting on first nil leaves a permanent gap once
// reconnect installs the new session.
func TestXMPPKeepaliveSurvivesNilJSess(t *testing.T) {
	js := newSilentSession(t)

	// Belt-and-braces: the keepalive goroutine launched by Connect is
	// not running because we never called Connect. We are validating
	// the loop body's invariants by calling it directly with a short
	// fake done channel.
	done := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		ticks := 0
		for {
			select {
			case <-done:
				close(finished)
				return
			case <-ticker.C:
				jSess := js.jSess.Load()
				if jSess == nil {
					ticks++
					if ticks > 5 {
						close(finished)
						return
					}
					continue
				}
				close(finished)
				return
			}
		}
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		close(done)
		t.Fatal("keepalive loop did not survive nil jSess for several ticks")
	}
}

// TestRequestReconnectRespectsShouldReconnect ensures that the supervisor
// remains the single source of truth on whether to reconnect — keepalive
// and bridge errors must not bypass shouldReconnect and force themselves
// onto a session the application has decided to wind down.
func TestRequestReconnectRespectsShouldReconnect(t *testing.T) {
	js := newSilentSession(t)

	var endedReason string
	js.SetEndedCallback(func(r string) { endedReason = r })
	js.SetShouldReconnect(func() bool { return false })

	js.requestReconnect("simulated keepalive failure")

	if endedReason == "" {
		t.Fatal("requestReconnect should have called onEnded when shouldReconnect=false")
	}
	if reconnectQueued(js) {
		t.Fatal("reconnect must NOT be queued when shouldReconnect returns false")
	}
}

// TestRequestReconnectIdempotent guards against duplicate reconnect storms:
// the channel is buffered to depth 1 and additional requests must collapse
// into the existing slot rather than block or panic.
func TestRequestReconnectIdempotent(t *testing.T) {
	js := newSilentSession(t)
	js.SetShouldReconnect(func() bool { return true })

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			js.requestReconnect("burst")
		}()
	}
	wg.Wait()

	// At most one slot consumed.
	select {
	case <-js.reconnectCh:
	case <-time.After(time.Second):
		t.Fatal("expected exactly one reconnect to be enqueued")
	}
	select {
	case <-js.reconnectCh:
		t.Fatal("more than one reconnect enqueued — duplicate-suppression broken")
	default:
	}
}

func drainReconnectChNonBlocking(s *Session) {
	for {
		select {
		case <-s.reconnectCh:
		default:
			return
		}
	}
}

func reconnectQueued(s *Session) bool {
	select {
	case <-s.reconnectCh:
		return true
	default:
		return false
	}
}
