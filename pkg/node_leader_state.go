package pkg
 
import (
	"sort"
	"strconv"
	"strings"
	"time"
)
 
// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)
	n.setLeader(n.Self)
	currTerm := n.GetCurrentTerm()
 
	// reset leader bookkeeping
	for _, peer := range n.getPeers() {
		n.nextIndex[peer.Id] = n.LastLogIndex() + 1
		n.matchIndex[peer.Id] = 0
	}
 
	// append a NOOP entry for our term so earlier entries can be committed.
	n.StoreLog(&LogEntry{
		Index:  n.LastLogIndex() + 1,
		TermId: currTerm,
		Type:   CommandType_NOOP,
	})
 
	// send heartbeats to followers and revert back to follower if true
	if n.sendHeartbeats() {
		return n.doFollower
	}
 
	ticker := time.NewTicker(n.Config.HeartbeatTimeout)
	defer ticker.Stop()
 
	// leader channels listener
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
 
		case <-ticker.C:
			if n.sendHeartbeats() {
				return n.doFollower
			}
 
		case msg := <-n.appendEntries:
			if _, fallback := n.handleAppendEntries(msg); fallback {
				return n.doFollower
			}
 
		case msg := <-n.requestVote:
			if n.handleRequestVote(msg) {
				return n.doFollower
			}
 
		// defer to helper function
		case msg := <-n.clientRequest:
			n.handleClientRequest(msg)
		}
	}
}
 
// handleClientRequest appends a client's request to our log
func (n *Node) handleClientRequest(msg ClientRequestMsg) {
	req := msg.request
	index := n.LastLogIndex() + 1
 
	entry := &LogEntry{
		Index:  index,
		TermId: n.GetCurrentTerm(),
	}
 
	// if ClientId is 0 its a client registration call
	if req.ClientId == 0 {
		entry.Type = CommandType_CLIENT_REGISTRATION
		entry.CacheId = CreateCacheID(0, index)
	} else {
		entry.Type = CommandType_STATE_MACHINE_COMMAND
		entry.Command = req.StateMachineCmd
		entry.Data = req.Data
		entry.CacheId = CreateCacheID(req.ClientId, req.SequenceNum)
	}
 
	// remember which channel to reply on when this entry is applied
	_, alreadyInFlight := n.requestsByCacheID[entry.CacheId]
	n.requestsByCacheID[entry.CacheId] = append(n.requestsByCacheID[entry.CacheId], msg.reply)
 
	// duplicate check
	if alreadyInFlight {
		return
	}
 
	n.StoreLog(entry)
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
	currTerm := n.GetCurrentTerm()

	// make channel and send heartbeat to each peer
	results := make(chan bool, len(n.getPeers()))
	others := 0
	for _, peer := range n.getPeers() {
		if peer.Id == n.Self.Id {
			continue
		}
		others++
		//sync follower log with own
		go func(p *RemoteNode) {
			results <- n.replicateTo(p, currTerm)
		}(peer)
	}

	// wait for peers during timeout period
	deadline := time.After(n.Config.HeartbeatTimeout)
wait:
// determine if anything interferes with heartbeats
	for i := 0; i < others; i++ {
		select {
		case sawHigherTerm := <-results:
			if sawHigherTerm {
				return true
			}
		case <-deadline:
			break wait
		}
	}

	n.updateCommitIndex(currTerm)
	return false
}
 
// replicateTo helps sync logs between leader and peer
func (n *Node) replicateTo(peer *RemoteNode, currTerm uint64) (fallback bool) {
	n.LeaderMutex.Lock()
	next := n.nextIndex[peer.Id]
	n.LeaderMutex.Unlock()
 
	prevIdx := next - 1
	prevTerm := n.GetLog(prevIdx).TermId
 
	// every entry the peer is missing
	var entries []*LogEntry
	for i := next; i <= n.LastLogIndex(); i++ {
		entries = append(entries, n.GetLog(i))
	}
 
	request := &AppendEntriesRequest{
		Term:         currTerm,
		Leader:       n.Self,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex.Load(),
	}
 
	reply, err := peer.AppendEntriesRPC(n, request) 
	if err != nil {
		return false // peer unreachable try again next heartbeat
	}
 
	if reply.Term > currTerm {
		n.SetCurrentTerm(reply.Term)
		n.setVotedFor("")
		return true
	}
 
	n.LeaderMutex.Lock()
	defer n.LeaderMutex.Unlock()
	if reply.Success {
		n.matchIndex[peer.Id] = prevIdx + uint64(len(entries))
		n.nextIndex[peer.Id] = n.matchIndex[peer.Id] + 1
	} else if n.nextIndex[peer.Id] > 1 {
		// logs don't match at prevIdx: back up one and retry next heartbeat.
		n.nextIndex[peer.Id]--
	}
	return false
}
 
// updateCommitIndex finds the highest index stored on a majority of nodes and,
// if it is from our current term, commits and applies everything up to it.
func (n *Node) updateCommitIndex(currTerm uint64) {
	peers := n.getPeers()
 
	// our own log counts as one copy.
	indices := []uint64{n.LastLogIndex()}
	n.LeaderMutex.Lock()
	for _, peer := range peers {
		if peer.Id != n.Self.Id {
			indices = append(indices, n.matchIndex[peer.Id])
		}
	}
	n.LeaderMutex.Unlock()
 
	// sorted high to low, the entry at len/2 is on a majority of nodes.
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	majorityIdx := indices[len(indices)/2]
 
	// only commit entries from our own term (Raft paper, section 5.4.2).
	if majorityIdx > n.CommitIndex.Load() && n.GetLog(majorityIdx).TermId == currTerm {
		n.Out("Updating commitIndex from %v -> %v", n.CommitIndex.Load(), majorityIdx)
		for majorityIdx > n.LastApplied.Load() {
			n.processLogEntry(n.LastApplied.Inc())
		}
		n.CommitIndex.Store(majorityIdx)
	}
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
