package main

import "github.com/prajwalmahajan101/toymq/pkg/client"

// pubResultMsg is delivered when an async PUB completes.
type pubResultMsg struct {
	topic string
	msgID uint64
	dup   bool
	err   error
}

// subStartedMsg is delivered when an async SUB completes. On success
// ch carries deliveries; on failure err is set.
type subStartedMsg struct {
	topic      string
	consumerID string
	ch         <-chan client.Delivery
	err        error
}

// msgArrivedMsg is one MSG frame from the active subscription.
type msgArrivedMsg struct {
	d client.Delivery
}

// ackResultMsg is delivered when an async ACK or NACK completes.
type ackResultMsg struct {
	verb  string // "ack" or "nack"
	msgID uint64
	err   error
}

// transportLostMsg is sent when the subscription channel closes.
// Carries client.Err() so the UI can distinguish caller-close from
// transport blow-up.
type transportLostMsg struct {
	err error
}

// clusterInfoMsg carries one INFO replication poll result (v3 M5). gen
// tags the poll chain that issued it; a mismatch means the view was
// left and re-entered, so the result is stale and dropped.
type clusterInfoMsg struct {
	info client.ReplicationInfo
	err  error
	gen  int
}

// clusterPollTickMsg fires clusterPollInterval after the previous poll
// while the cluster view is open. gen gates re-arming.
type clusterPollTickMsg struct {
	gen int
}
