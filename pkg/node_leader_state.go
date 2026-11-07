package pkg

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.
	return nil
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (n *Node) sendHeartbeats() (fallback bool) {
	// TODO: Students should implement this method
	return true, true
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}
