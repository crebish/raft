package pkg

import (
	"fmt"
	"math"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (n *Node) doFollower() stateFunction {
	n.Out("Transitioning to FollowerState")
	n.setState(FollowerState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (n *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	request := msg.request
	reply := msg.reply

	// If leader term is lower than ours, reject
	if request.Term < n.GetCurrentTerm() {
		n.Out("Received AppendEntries request with lower term, rejecting")
		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: false,
		}

		return false, false
	}

	// Fallback since request term is equal or higher
	fallback = true
	// Reset timeout since we've accepted the leader as valid
	resetTimeout = true

	n.lastHeardFromLeader.Store(time.Now().UnixNano())

	// Set leader to be whomever is sending us append entries requests
	if leader := n.getLeader(); leader == nil || (leader.Id != request.Leader.Id) {
		oldLeaderID := "nil"
		if leader != nil {
			oldLeaderID = n.Leader.Id
		}
		n.setLeader(request.Leader)
		n.Out("Updated leader from %v -> %v", oldLeaderID, request.Leader.Id)
	}

	// Update term and clear votedFor if request is from higher term
	if request.Term > n.GetCurrentTerm() {
		n.Out("Received AppendEntries with higher term than ours, updating from %v -> %v",
			n.GetCurrentTerm(), request.Term)
		n.SetCurrentTerm(request.Term)
		n.setVotedFor("")
	}

	if request.PrevLogIndex > n.LastLogIndex() {
		// (Partial check of rule 2 of "AppendEntriesRPC" from figure 2 of Raft paper)
		//
		// If request PrevLogIndex is higher than our LastLogIndex, then the leader's
		// conception of our logs (specifically log length) is false, reject
		n.Out("Received message with previous log index greater than ours (req.prevLogIdx=%v, lastLogIdx=%v)", request.PrevLogIndex, n.LastLogIndex())

		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: false,
		}
	} else if entry := n.GetLog(request.PrevLogIndex); entry != nil && (entry.TermId != request.PrevLogTerm) {
		// (Remaining check of rule 2 of "AppendEntriesRPC" from figure 2 of Raft paper)
		//
		// If our log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm,
		// then leader's conception of our log is false, reject
		n.Out("Log entry already exists for PrevLogIndex")

		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: false,
		}
	} else {
		// Accept request; truncate if necessary and append any new entries
		if len(request.Entries) > 0 {
			n.Out("Successfully received %d entries", len(request.Entries))

			newFirstEntry := request.Entries[0]
			ourLastEntry := n.GetLog(n.LastLogIndex())

			// Truncate log if necessary...
			if ourLastEntry.Index >= newFirstEntry.Index {
				// truncate our log and accept leaders' log
				n.Out("Truncating log to remove index %v and beyond", newFirstEntry.Index)
				n.TruncateLog(newFirstEntry.Index)
			}

			// At this point, our LastLogIndex + 1 == newFirstEntry.Index; safe to append new entries
			if n.LastLogIndex()+1 != newFirstEntry.GetIndex() {
				fmt.Printf("Request: %v\n", request.Entries)
				panic(fmt.Sprintf("after truncation, local LastLogIndex + 1: %v is not equal to newFirstEntry.Index: %v", n.LastLogIndex()+1, newFirstEntry.GetIndex()))
			}

			// Append new entries...
			for _, entry := range request.Entries {
				n.StoreLog(entry)
			}
		}

		// Update commit index, process any newly committed log entries, update lastApplied
		if request.LeaderCommit > n.CommitIndex.Load() {
			newCommitIdx := uint64(math.Min(float64(request.LeaderCommit), float64(n.LastLogIndex())))
			n.Out("Updating commitIndex from %v -> %v", n.CommitIndex.Load(), newCommitIdx)

			for newCommitIdx > n.LastApplied.Load() {
				n.processLogEntry(n.LastApplied.Inc())
			}

			n.CommitIndex.Store(newCommitIdx)
		}

		// Reply with success!
		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: true,
		}
	}

	return
}
