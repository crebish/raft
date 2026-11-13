package pkg

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState) 

	// start a new election
	n.SetCurrentTerm(n.GetCurrentTerm() + 1) // increment term
	n.setVotedFor(n.Self.Id) // vote for self
	n.setLeader(nil) // removes leader until new consensus
	currTerm := n.GetCurrentTerm()

	timeout := randomTimeout(n.Config.ElectionTimeout) // randomize timeout based on [timeout, 2 * timeout]

	wonElection := make(chan bool, 1) // make a channel for winning election
	stepDown := make(chan bool, 1) // vs losing election
	
	// determine if the node won the election
	go func() {
		fallback, won := n.requestVotes(currTerm) // gather votes from followers
		if fallback {
			stepDown <- true
		} else if won {
			wonElection <- true
		}
	}()

	// start a listener for the channels
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}

		// process appendEntries from leader, i.e. a leader exists, so go back to follower
		case msg := <-n.appendEntries:
			if _, fallback := n.handleAppendEntries(msg); fallback {
				return n.doFollower
			}

		// read in a vote request, then handle it
		case msg := <-n.requestVote:
			if n.handleRequestVote(msg) {
				return n.doFollower
			}

		// client tries to make a request while we are currently in an election, respond nothing
		case msg := <-n.clientRequest:
			msg.reply <- ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				ClientId:   msg.request.ClientId,
				Response:   nil,
				LeaderHint: nil,
			}

		// election lost -> become a follower
		case <-stepDown:
			return n.doFollower

		// election won -> become a leader
		case <-wonElection:
			return n.doLeader

		// ain't nobody won so try again with a new term
		case <-timeout:
			return n.doCandidate
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	peers := n.getPeers()

	lastIdx := n.LastLogIndex()
	request := &RequestVoteRequest{
		Term:         currTerm,
		Candidate:    n.Self,
		LastLogIndex: lastIdx,
		LastLogTerm:  n.GetLog(lastIdx).TermId,
	}

	votes := 1 // our own vote
	majority := len(peers)/2 + 1
	if votes >= majority {
		return false, true // single-node cluster
	}

	// Ask every other peer at the same time.
	replies := make(chan *RequestVoteReply, len(peers))
	others := 0
	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue
		}
		others++
		go func(p *RemoteNode) {
			reply, err := p.RequestVoteRPC(n, request)
			if err != nil {
				replies <- nil
				return
			}
			replies <- reply
		}(peer)
	}

	for i := 0; i < others; i++ {
		reply := <-replies
		if reply == nil {
			continue
		}
		if reply.Term > currTerm {
			n.SetCurrentTerm(reply.Term)
			n.setVotedFor("")
			return true, false
		}
		if reply.VoteGranted {
			votes++
			if votes >= majority {
				return false, true
			}
		}
	}
	return false, false
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	request := msg.request
	currTerm := n.GetCurrentTerm()

	// reject candidates with an old term
	if request.Term < currTerm {
		msg.reply <- RequestVoteReply{Term: currTerm, VoteGranted: false}
		return false
	}

	// our term is out of sync so set term to latest and remove vote
	if request.Term > currTerm {
		n.SetCurrentTerm(request.Term)
		n.setVotedFor("")
		currTerm = request.Term
		fallback = true
	}

	// compare node's log with candidate's log
	ourLastIdx := n.LastLogIndex()
	ourLastTerm := n.GetLog(ourLastIdx).TermId
	logOk := request.LastLogTerm > ourLastTerm ||
		(request.LastLogTerm == ourLastTerm && request.LastLogIndex >= ourLastIdx)

	// grant the vote if we haven't voted for someone else and the log is ok
	votedFor := n.GetVotedFor()
	if logOk && (votedFor == "" || votedFor == request.Candidate.Id) {
		n.setVotedFor(request.Candidate.Id)
		msg.reply <- RequestVoteReply{Term: currTerm, VoteGranted: true}
		return true
	}

	// otherwise don't give vote and fallback
	msg.reply <- RequestVoteReply{Term: currTerm, VoteGranted: false}
	return fallback
}