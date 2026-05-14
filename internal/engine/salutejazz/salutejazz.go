// Package salutejazz implements an engine.Session backed by the SaluteJazz
// signaling protocol (WS + SDP with publisher/subscriber peer connection
// split). The on-wire protocol is Sber-specific; the media plane is
// straightforward WebRTC. Token acquisition lives in the auth package.
package salutejazz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
	"github.com/pion/webrtc/v4"
)

const (
	maxDataChannelMessageSize = 12288
	sendDelay                 = 2 * time.Millisecond

	keyRoomID    = "roomId"
	keyEvent     = "event"
	keyRequestID = "requestId"
	keyPayload   = "payload"
	keyGroupID   = "groupId"

	eventMediaIn = "media-in"

	payloadMethod = "method"
	payloadTrack  = "track"
	payloadType   = "type"
	payloadAnswer = "answer"

	credentialKeyPassword = "password"

	defaultSendQueueSize = 5000
	mediaReadyTimeout    = 30 * time.Second
	dataChannelTimeout   = 30 * time.Second
	wsReadTimeout        = 60 * time.Second
	wsHandshakeTimeout   = 15 * time.Second
	sendQueueTimeout     = 50 * time.Millisecond
	closeWaitTimeout     = 2 * time.Second
	subscriberOfferGap   = 300 * time.Millisecond
)

var (
	// ErrPublisherNotInitialized is returned when the publisher peer connection is not set up.
	ErrPublisherNotInitialized = errors.New("publisher peer connection not initialized")
	// ErrSubscriberMediaTimeout is returned when the subscriber media is not ready in time.
	ErrSubscriberMediaTimeout = errors.New("subscriber media timeout")
	// ErrDataChannelTimeout is returned when the data channel fails to open in time.
	ErrDataChannelTimeout = errors.New("datachannel timeout")
	// ErrDataChannelNotReady is returned when send is called before the data channel is open.
	ErrDataChannelNotReady = errors.New("datachannel not ready")
	// ErrSendQueueClosed is returned when send is called after Close.
	ErrSendQueueClosed = errors.New("send queue closed")
	// ErrSendQueueTimeout is returned when the send queue cannot accept new data in time.
	ErrSendQueueTimeout = errors.New("send queue timeout")
	// ErrURLRequired is returned when no connector URL was supplied.
	ErrURLRequired = errors.New("salutejazz connector URL required")
	// ErrRoomIDRequired is returned when no room ID was supplied.
	ErrRoomIDRequired = errors.New("salutejazz room ID required")
)

// Session is the SaluteJazz engine handle.
type Session struct {
	name            string
	connectorURL    string
	roomID          string
	password        string
	ws              *websocket.Conn
	wsMu            sync.Mutex
	pcSub           *webrtc.PeerConnection
	pcPub           *webrtc.PeerConnection
	dc              *webrtc.DataChannel
	onData          func([]byte)
	onReconnect     func(*webrtc.DataChannel)
	shouldReconnect func() bool
	reconnectCh     chan struct{}
	closeCh         chan struct{}
	closed          atomic.Bool
	reconnecting    atomic.Bool
	sendQueue       chan []byte
	sendQueueClosed atomic.Bool
	onEnded         func(string)
	sessionCloseCh  chan struct{}
	videoTrackMu    sync.RWMutex
	videoTracks     []webrtc.TrackLocal
	onVideoTrack    func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	subscriberReady atomic.Bool
	publisherReady  atomic.Bool
	subscriberConn  chan struct{}
	publisherConn   chan struct{}
	wg              sync.WaitGroup
	groupID         string
}

// New creates a new SaluteJazz engine session.
//
// cfg.URL is the SaluteJazz connector WebSocket URL. cfg.Token carries the
// room ID; cfg.Extra["password"] carries the room password. These are
// produced by the salutejazz auth provider.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	// Token field encodes the room ID for this engine.
	roomID := cfg.Token
	if roomID == "" {
		return nil, ErrRoomIDRequired
	}
	password := ""
	if cfg.Extra != nil {
		password = cfg.Extra[credentialKeyPassword]
	}

	return &Session{
		name:           cfg.Name,
		connectorURL:   cfg.URL,
		roomID:         roomID,
		password:       password,
		onData:         cfg.OnData,
		reconnectCh:    make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		sessionCloseCh: make(chan struct{}),
		sendQueue:      make(chan []byte, defaultSendQueueSize),
		subscriberConn: make(chan struct{}),
		publisherConn:  make(chan struct{}),
	}, nil
}

// Capabilities reports what this engine can do.
func (s *Session) Capabilities() engine.Capabilities {
	return engine.Capabilities{ByteStream: true, VideoTrack: true}
}

func (s *Session) resetMediaState() {
	s.subscriberReady.Store(false)
	s.publisherReady.Store(false)
	s.subscriberConn = make(chan struct{})
	s.publisherConn = make(chan struct{})
}

func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (s *Session) hasLocalVideoTracks() bool {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()
	return len(s.videoTracks) > 0
}

func (s *Session) videoTrackHandler() func(*webrtc.TrackRemote, *webrtc.RTPReceiver) {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()
	return s.onVideoTrack
}

func (s *Session) attachPendingVideoTracks() error {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()

	for _, track := range s.videoTracks {
		if _, err := s.pcPub.AddTrack(track); err != nil {
			return fmt.Errorf("failed to add track: %w", err)
		}
	}
	return nil
}

func defaultWebRTCConfig() webrtc.Configuration {
	return webrtc.Configuration{
		ICEServers:   []webrtc.ICEServer{},
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlan,
		BundlePolicy: webrtc.BundlePolicyMaxBundle,
	}
}

func (s *Session) buildAPI() *webrtc.API {
	se := webrtc.SettingEngine{}
	if protect.Protector != nil {
		se.SetICEProxyDialer(protect.NewProxyDialer())
	}
	se.LoggerFactory = logger.NewPionLoggerFactory()
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

func (s *Session) createPeerConnections(api *webrtc.API, config webrtc.Configuration) error {
	var err error
	s.pcSub, err = api.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("create subscriber pc: %w", err)
	}
	s.pcSub.OnConnectionStateChange(s.onSubscriberConnectionStateChange)
	s.pcSub.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			return
		}
		logger.Infof("[salutejazz] remote video track: codec=%s stream=%s track=%s",
			track.Codec().MimeType, track.StreamID(), track.ID())
		if cb := s.videoTrackHandler(); cb != nil {
			cb(track, receiver)
		}
	})

	s.pcPub, err = api.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("create publisher pc: %w", err)
	}
	s.pcPub.OnConnectionStateChange(s.onPublisherConnectionStateChange)
	return nil
}

func (s *Session) createDataChannel() (chan struct{}, error) {
	var err error
	s.dc, err = s.pcPub.CreateDataChannel("_reliable", &webrtc.DataChannelInit{
		Ordered: func() *bool { v := true; return &v }(),
	})
	if err != nil {
		return nil, fmt.Errorf("create datachannel: %w", err)
	}
	dcReady := make(chan struct{})
	s.setupDataChannelHandlers(dcReady)
	return dcReady, nil
}

func (s *Session) waitForReady(ctx context.Context, dcReady chan struct{}) error {
	if dcReady != nil {
		select {
		case <-dcReady:
			return nil
		case <-time.After(dataChannelTimeout):
			return ErrDataChannelTimeout
		case <-ctx.Done():
			return fmt.Errorf("connect canceled: %w", ctx.Err())
		}
	}
	return s.waitForMediaReady(ctx, mediaReadyTimeout)
}

// Connect starts the WebRTC connection process.
func (s *Session) Connect(ctx context.Context) error {
	s.closed.Store(false)
	s.resetMediaState()

	api := s.buildAPI()
	config := defaultWebRTCConfig()

	if err := s.createPeerConnections(api, config); err != nil {
		return err
	}
	if err := s.attachPendingVideoTracks(); err != nil {
		return err
	}

	var dcReady chan struct{}
	if s.onData != nil {
		var err error
		dcReady, err = s.createDataChannel()
		if err != nil {
			return err
		}
	}

	if err := s.dialWebSocket(); err != nil {
		return err
	}
	if err := s.sendJoin(); err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handleSignaling(ctx)
	}()

	return s.waitForReady(ctx, dcReady)
}

func (s *Session) waitForMediaReady(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-s.subscriberConn:
	case <-timer.C:
		return ErrSubscriberMediaTimeout
	case <-ctx.Done():
		return fmt.Errorf("connect cancelled: %w", ctx.Err())
	}
	return nil
}

func (s *Session) dialWebSocket() error {
	wsDialer := websocket.Dialer{
		NetDialContext:   protect.DialContext,
		HandshakeTimeout: wsHandshakeTimeout,
	}

	ws, resp, err := wsDialer.Dial(s.connectorURL, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	s.ws = ws
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})
	_ = ws.SetReadDeadline(time.Now().Add(wsReadTimeout))
	return nil
}

func (s *Session) sendJoin() error {
	joinMsg := map[string]any{
		keyRoomID:    s.roomID,
		keyEvent:     "join",
		keyRequestID: uuid.New().String(),
		keyPayload: map[string]any{
			"password":        s.password,
			"participantName": s.name,
			"supportedFeatures": map[string]any{
				"attachedRooms": true,
				"sessionGroups": true,
				"transcription": true,
			},
			"isSilent": false,
		},
	}

	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if err := s.ws.WriteJSON(joinMsg); err != nil {
		return fmt.Errorf("write join json: %w", err)
	}
	return nil
}

func (s *Session) setupDataChannelHandlers(dcReady chan struct{}) {
	s.dc.OnOpen(func() {
		logger.Verbosef("[salutejazz] Publisher DC opened: %s", s.dc.Label())
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.processSendQueue()
		}()
		close(dcReady)
	})

	s.dc.OnClose(func() {
		logger.Verbosef("[salutejazz] Publisher DC closed")
		if !s.closed.Load() {
			s.queueReconnect()
		}
	})

	s.dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		s.handleIncomingMessage(msg.Data, "publisher")
	})

	s.pcSub.OnDataChannel(func(dc *webrtc.DataChannel) {
		logger.Verbosef("[salutejazz] Received subscriber DataChannel: %s", dc.Label())
		if dc.Label() != "_reliable" {
			return
		}
		if s.onData != nil {
			dc.OnMessage(func(msg webrtc.DataChannelMessage) {
				s.handleIncomingMessage(msg.Data, "subscriber")
			})
		}
	})
}

func (s *Session) onSubscriberConnectionStateChange(state webrtc.PeerConnectionState) {
	switch state {
	case webrtc.PeerConnectionStateConnected:
		s.subscriberReady.Store(true)
		closeSignal(s.subscriberConn)
	case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed:
		s.subscriberReady.Store(false)
		if !s.closed.Load() {
			s.queueReconnect()
		}
	case webrtc.PeerConnectionStateClosed:
		s.subscriberReady.Store(false)
	case webrtc.PeerConnectionStateUnknown,
		webrtc.PeerConnectionStateNew,
		webrtc.PeerConnectionStateConnecting:
	}
}

func (s *Session) onPublisherConnectionStateChange(state webrtc.PeerConnectionState) {
	switch state {
	case webrtc.PeerConnectionStateConnected:
		s.publisherReady.Store(true)
		closeSignal(s.publisherConn)
	case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed:
		s.publisherReady.Store(false)
		if !s.closed.Load() {
			s.queueReconnect()
		}
	case webrtc.PeerConnectionStateClosed:
		s.publisherReady.Store(false)
	case webrtc.PeerConnectionStateUnknown,
		webrtc.PeerConnectionStateNew,
		webrtc.PeerConnectionStateConnecting:
	}
}

func (s *Session) handleIncomingMessage(data []byte, source string) {
	logger.Verbosef("[salutejazz] Received %d bytes on %s DC (raw)", len(data), source)

	payload, ok := DecodeDataPacket(data)
	if !ok {
		logger.Debugf("[salutejazz] Failed to decode DataPacket, trying raw")
		if s.onData != nil && len(data) > 0 {
			s.onData(data)
		}
		return
	}

	logger.Verbosef("[salutejazz] Decoded DataPacket: %d bytes payload", len(payload))
	if s.onData != nil && len(payload) > 0 {
		s.onData(payload)
	}
}

func (s *Session) handleSignaling(_ context.Context) {
	for {
		var msg map[string]any
		if err := s.ws.ReadJSON(&msg); err != nil {
			if !s.closed.Load() {
				logger.Debugf("ws read error: %v", err)
				s.queueReconnect()
			}
			return
		}

		s.updateWSDeadline()

		event, _ := msg[keyEvent].(string)
		payload, _ := msg[keyPayload].(map[string]any)

		switch event {
		case "join-response":
			s.handleJoinResponse(payload)
		case "media-out":
			s.handleMediaOut(payload)
		}
	}
}

func (s *Session) handleJoinResponse(payload map[string]any) {
	group, _ := payload["participantGroup"].(map[string]any)
	s.groupID, _ = group["groupId"].(string)
	logger.Verbosef("[salutejazz] peer joined: groupId=%s", s.groupID)
}

func (s *Session) handleMediaOut(payload map[string]any) {
	method, _ := payload["method"].(string)

	switch method {
	case "rtc:config":
		s.handleRTCConfig(payload)
	case "rtc:join":
		logger.Verbosef("[salutejazz] rtc:join received")
	case "rtc:offer":
		s.handleSubscriberOffer(payload)
	case "rtc:answer":
		s.handlePublisherAnswer(payload)
	case "rtc:ice":
		s.handleICE(payload)
	}
}

func (s *Session) handleRTCConfig(payload map[string]any) {
	config, _ := payload["configuration"].(map[string]any)
	servers, _ := config["iceServers"].([]any)

	var iceServers []webrtc.ICEServer
	for _, srv := range servers {
		server, _ := srv.(map[string]any)
		urls, _ := server["urls"].([]any)
		username, _ := server["username"].(string)
		credential, _ := server["credential"].(string)

		var urlStrs []string
		for _, u := range urls {
			if urlStr, ok := u.(string); ok && urlStr != "" {
				urlStrs = append(urlStrs, urlStr)
			}
		}

		if len(urlStrs) > 0 {
			iceServers = append(iceServers, webrtc.ICEServer{
				URLs:       urlStrs,
				Username:   username,
				Credential: credential,
			})
		}
	}

	if len(iceServers) > 0 {
		newConfig := webrtc.Configuration{
			ICEServers:   iceServers,
			SDPSemantics: webrtc.SDPSemanticsUnifiedPlan,
			BundlePolicy: webrtc.BundlePolicyMaxBundle,
		}
		_ = s.pcSub.SetConfiguration(newConfig)
		_ = s.pcPub.SetConfiguration(newConfig)
	}
}

func (s *Session) handleSubscriberOffer(payload map[string]any) {
	desc, _ := payload["description"].(map[string]any)
	sdp, _ := desc["sdp"].(string)

	if err := s.pcSub.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}); err != nil {
		logger.Debugf("set remote desc error: %v", err)
		return
	}

	answer, err := s.pcSub.CreateAnswer(nil)
	if err != nil {
		logger.Debugf("create answer error: %v", err)
		return
	}

	if err := s.pcSub.SetLocalDescription(answer); err != nil {
		logger.Debugf("set local desc error: %v", err)
		return
	}

	s.wsMu.Lock()
	_ = s.ws.WriteJSON(map[string]any{
		keyRoomID:    s.roomID,
		keyEvent:     eventMediaIn,
		keyGroupID:   s.groupID,
		keyRequestID: uuid.New().String(),
		keyPayload: map[string]any{
			payloadMethod: "rtc:answer",
			"description": map[string]any{
				payloadType: payloadAnswer,
				"sdp":       answer.SDP,
			},
		},
	})
	s.wsMu.Unlock()

	time.Sleep(subscriberOfferGap)
	s.sendPublisherOffer()
}

func (s *Session) sendPublisherOffer() {
	if err := s.sendPublisherTrackAdds(); err != nil {
		logger.Debugf("send publisher track add error: %v", err)
		return
	}

	offer, err := s.pcPub.CreateOffer(nil)
	if err != nil {
		logger.Debugf("create pub offer error: %v", err)
		return
	}

	if err := s.pcPub.SetLocalDescription(offer); err != nil {
		logger.Debugf("set local pub desc error: %v", err)
		return
	}

	s.wsMu.Lock()
	_ = s.ws.WriteJSON(map[string]any{
		keyRoomID:    s.roomID,
		keyEvent:     "media-in",
		"groupId":    s.groupID,
		keyRequestID: uuid.New().String(),
		keyPayload: map[string]any{
			"method": "rtc:offer",
			"description": map[string]any{
				"type": "offer",
				"sdp":  offer.SDP,
			},
		},
	})
	s.wsMu.Unlock()
}

func (s *Session) sendPublisherTrackAdds() error {
	s.videoTrackMu.RLock()
	tracks := append([]webrtc.TrackLocal(nil), s.videoTracks...)
	s.videoTrackMu.RUnlock()

	for _, track := range tracks {
		if track == nil || track.Kind() != webrtc.RTPCodecTypeVideo {
			continue
		}
		if err := s.sendPublisherTrackAdd("VIDEO", "CAMERA", false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) sendPublisherTrackAdd(trackType, source string, muted bool) error {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()

	if err := s.ws.WriteJSON(map[string]any{
		keyRoomID:    s.roomID,
		keyEvent:     eventMediaIn,
		keyGroupID:   s.groupID,
		keyRequestID: uuid.New().String(),
		keyPayload: map[string]any{
			payloadMethod: "rtc:track:add",
			"cid":         uuid.New().String(),
			payloadTrack: map[string]any{
				payloadType: trackType,
				"source":    source,
				"muted":     muted,
			},
		},
	}); err != nil {
		return fmt.Errorf("write track add json: %w", err)
	}
	return nil
}

func (s *Session) handlePublisherAnswer(payload map[string]any) {
	desc, _ := payload["description"].(map[string]any)
	sdp, _ := desc["sdp"].(string)

	if err := s.pcPub.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}); err != nil {
		logger.Debugf("set remote pub desc error: %v", err)
	}
}

func (s *Session) handleICE(payload map[string]any) {
	candidates, _ := payload["rtcIceCandidates"].([]any)

	for _, c := range candidates {
		cand, _ := c.(map[string]any)
		candStr, _ := cand["candidate"].(string)
		target, _ := cand["target"].(string)
		sdpMid, _ := cand["sdpMid"].(string)
		sdpMLineIndex, _ := cand["sdpMLineIndex"].(float64)

		init := webrtc.ICECandidateInit{
			Candidate:     candStr,
			SDPMid:        &sdpMid,
			SDPMLineIndex: func() *uint16 { v := uint16(sdpMLineIndex); return &v }(),
		}

		switch target {
		case "SUBSCRIBER":
			_ = s.pcSub.AddICECandidate(init)
		case "PUBLISHER":
			_ = s.pcPub.AddICECandidate(init)
		}
	}
}

func (s *Session) updateWSDeadline() {
	s.wsMu.Lock()
	if s.ws != nil {
		_ = s.ws.SetReadDeadline(time.Now().Add(wsReadTimeout))
	}
	s.wsMu.Unlock()
}

// Send queues data for transmission.
func (s *Session) Send(data []byte) error {
	if s.dc == nil || s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrDataChannelNotReady
	}
	if s.sendQueueClosed.Load() {
		return ErrSendQueueClosed
	}

	select {
	case s.sendQueue <- data:
		return nil
	case <-time.After(sendQueueTimeout):
		return ErrSendQueueTimeout
	}
}

func (s *Session) processSendQueue() {
	for {
		select {
		case <-s.sessionCloseCh:
			return
		case <-s.closeCh:
			return
		case data := <-s.sendQueue:
			if len(data) > maxDataChannelMessageSize {
				logger.Debugf("[salutejazz] Message too large: %d bytes (max %d)", len(data), maxDataChannelMessageSize)
				continue
			}

			encoded := EncodeDataPacket(data)
			logger.Verbosef("[salutejazz] Sending %d bytes (encoded to %d bytes)", len(data), len(encoded))

			if err := s.dc.Send(encoded); err != nil {
				logger.Debugf("send error: %v", err)
				s.queueReconnect()
				return
			}
			time.Sleep(sendDelay)
		}
	}
}

// Close terminates the connection.
func (s *Session) Close() error {
	s.closed.Store(true)
	s.sendQueueClosed.Store(true)

	close(s.closeCh)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(closeWaitTimeout):
	}

	if s.dc != nil {
		_ = s.dc.Close()
	}
	if s.pcPub != nil {
		_ = s.pcPub.Close()
	}
	if s.pcSub != nil {
		_ = s.pcSub.Close()
	}
	if s.ws != nil {
		s.wsMu.Lock()
		_ = s.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
		_ = s.ws.Close()
		s.wsMu.Unlock()
	}
	return nil
}

// AddVideoTrack adds a video track to the publisher peer connection.
func (s *Session) AddVideoTrack(track webrtc.TrackLocal) error {
	s.videoTrackMu.Lock()
	s.videoTracks = append(s.videoTracks, track)
	s.videoTrackMu.Unlock()

	if s.pcPub == nil {
		return nil
	}
	if _, err := s.pcPub.AddTrack(track); err != nil {
		return fmt.Errorf("failed to add track: %w", err)
	}
	return nil
}

// SetVideoTrackHandler registers a callback for remote video tracks.
func (s *Session) SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.videoTrackMu.Lock()
	defer s.videoTrackMu.Unlock()
	s.onVideoTrack = cb
}

// SetReconnectCallback sets the callback for reconnection events.
func (s *Session) SetReconnectCallback(cb func(*webrtc.DataChannel)) { s.onReconnect = cb }

// SetShouldReconnect sets the policy for reconnection.
func (s *Session) SetShouldReconnect(fn func() bool) { s.shouldReconnect = fn }

// SetEndedCallback sets the callback for connection termination.
func (s *Session) SetEndedCallback(cb func(string)) { s.onEnded = cb }

// WatchConnection monitors the connection lifecycle.
func (s *Session) WatchConnection(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			return
		case <-s.reconnectCh:
		}
	}
}

// CanSend checks if data can be sent.
func (s *Session) CanSend() bool {
	if s.onData == nil {
		if s.hasLocalVideoTracks() {
			return !s.closed.Load() && s.subscriberReady.Load() && s.publisherReady.Load()
		}
		return !s.closed.Load() && s.subscriberReady.Load()
	}
	if s.dc == nil || s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return false
	}
	return len(s.sendQueue) < 4000
}

// GetSendQueue returns the transmission queue.
func (s *Session) GetSendQueue() chan []byte { return s.sendQueue }

// GetBufferedAmount returns the WebRTC buffered amount.
func (s *Session) GetBufferedAmount() uint64 {
	if s.dc != nil {
		return s.dc.BufferedAmount()
	}
	return 0
}

func (s *Session) queueReconnect() {
	if s.closed.Load() || s.reconnecting.Load() {
		return
	}
	if s.shouldReconnect != nil && !s.shouldReconnect() {
		return
	}
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

func init() { //nolint:gochecknoinits // engine registration is the canonical Go pattern for plugins
	engine.Register("salutejazz", New)
}
