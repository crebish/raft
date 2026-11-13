package test

import (
	raft "raft/pkg"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestRequestVotes(t *testing.T) {
	suppressLoggers()
	t.Run("Follower", RequestVotesFollower)
	t.Run("Candidate", RequestVotesCandidate) // election timeout is 60s
	t.Run("Leader", RequestVotesLeader)
}

func RequestVotesFollower(t *testing.T) {
	// Run common test suite...
	testVoteRequest(t, createFollowerMockClusterWith3Entries)

	// Run additional tests that are unique to followers...

	// 1. Check that follower votes for the same candidate again on retry
	t.Run("votes on retry", func(t *testing.T) {
		student, mocks, err := createFollowerMockClusterWith3Entries(t)
		if err != nil {
			t.Fatal("unable to create cluster", err)
		}
		defer cleanupMockCluster(student, mocks)

		studentTerm := student.GetCurrentTerm()
		studentLLI := student.LastLogIndex()
		studentLLT := student.GetLog(studentLLI).GetTermId()

		// Send vote request to student node...
		request := raft.RequestVoteRequest{
			Candidate:    mocks[0].RemoteSelf,
			Term:         studentTerm + 1,
			LastLogIndex: studentLLI,
			LastLogTerm:  studentLLT,
		}

		reply, err := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &request)
		if err != nil || !reply.GetVoteGranted() {
			t.Errorf("student raft didn't vote: err: %v, reply: %v", err, reply)
		}

		// Should vote for the same node twice
		reply2, err := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &request)
		if err != nil {
			t.Error("error in student raft voting", err)
		}

		if !reply2.GetVoteGranted() {
			t.Error("student raft didn't vote for same candidate")
		}
	})

	// 2. Check that follower does not vote again if they have already voted in the current term
	t.Run("wont vote twice", func(t *testing.T) {
		student, mocks, err := createFollowerMockClusterWith3Entries(t)
		if err != nil {
			t.Fatal("unable to create cluster", err)
		}
		defer cleanupMockCluster(student, mocks)

		studentTerm := student.GetCurrentTerm()
		studentLLI := student.LastLogIndex()
		studentLLT := student.GetLog(studentLLI).GetTermId()

		// Send vote request to student node...
		request := raft.RequestVoteRequest{
			Candidate:    mocks[0].RemoteSelf,
			Term:         studentTerm + 1,
			LastLogIndex: studentLLI,
			LastLogTerm:  studentLLT,
		}

		reply, err := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &request)
		if err != nil || !reply.GetVoteGranted() {
			t.Errorf("student raft didn't vote: err: %v, reply: %v", err, reply)
		}

		// Shouldn't vote for this candidate even though it is more up to date than the other
		request2 := raft.RequestVoteRequest{
			Candidate:    mocks[1].RemoteSelf,
			Term:         studentTerm + 1,
			LastLogIndex: studentLLI + 1,
			LastLogTerm:  studentLLT,
		}

		reply2, err := mocks[1].studentRaftClient.RequestVoteCaller(context.Background(), &request2)
		if err != nil {
			t.Error("error in student raft voting", err)
		}

		if reply2.GetVoteGranted() {
			t.Error("student raft voted for other candidate when it had already voted")
		}
	})

	// 3. Check that follower votes for a candidate if they have rejected a previous candidate in the same term
	t.Run("will vote if first fails", func(t *testing.T) {
		student, mocks, err := createFollowerMockClusterWith3Entries(t)
		if err != nil {
			t.Fatal("unable to create cluster", err)
		}
		defer cleanupMockCluster(student, mocks)

		studentTerm := student.GetCurrentTerm()
		studentLLI := student.LastLogIndex()
		studentLLT := student.GetLog(studentLLI).GetTermId()

		// Send vote request to student node...
		request := raft.RequestVoteRequest{
			Candidate:    mocks[0].RemoteSelf,
			Term:         studentTerm + 1,
			LastLogIndex: studentLLI - 1,
			LastLogTerm:  studentLLT,
		}

		// Expect first vote request to fail...
		reply, err := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &request)
		if err != nil || reply.GetVoteGranted() {
			t.Errorf("student raft voted or errored: err: %v, reply: %v", err, reply)
		}

		// Should vote for this candidate since it hasn't yet voted
		request2 := raft.RequestVoteRequest{
			Candidate:    mocks[1].RemoteSelf,
			Term:         studentTerm + 2,
			LastLogIndex: studentLLI,
			LastLogTerm:  studentLLT,
		}

		reply2, err := mocks[1].studentRaftClient.RequestVoteCaller(context.Background(), &request2)
		if err != nil {
			t.Error("error in student raft voting", err)
		}

		if !reply2.GetVoteGranted() {
			t.Error("student raft didn't vote")
		}
	})
}

func RequestVotesCandidate(t *testing.T) {
	testVoteRequest(t, createCandidateMockCluster)
}

func RequestVotesLeader(t *testing.T) {
	testVoteRequest(t, createLeaderMockCluster)
}

func testVoteRequest(t *testing.T, createCluster func(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error)) {
	// Setup subtests
	testCases := []struct {
		title string

		expectedFollowerVote        bool
		expectedCandidateLeaderVote bool
		expectedFallback            bool

		candidateTermOffset int64

		lastLogIndexOffset int64
		lastLogTermOffset  int64
	}{
		{"lower term/lower last log index/lower last log term", false, false, false, -1, -1, -1},
		{"lower term/lower last log index/equal last log term", false, false, false, -1, -1, 0},
		{"lower term/lower last log index/higher last log term", false, false, false, -1, -1, 1},
		{"lower term/equal last log index/lower last log term", false, false, false, -1, 0, -1},
		{"lower term/equal last log index/equal last log term", false, false, false, -1, 0, 0},
		{"lower term/equal last log index/higher last log term", false, false, false, -1, 0, 1},
		{"lower term/higher last log index/lower last log term", false, false, false, -1, 1, -1},
		{"lower term/higher last log index/equal last log term", false, false, false, -1, 1, 0},
		{"lower term/higher last log index/higher last log term", false, false, false, -1, 1, 1},

		{"equal term/lower last log index/lower last log term", false, false, false, 0, -1, -1},
		{"equal term/lower last log index/equal last log term", false, false, false, 0, -1, 0},
		{"equal term/lower last log index/higher last log term", true, false, false, 0, -1, 1},
		{"equal term/equal last log index/lower last log term", false, false, false, 0, 0, -1},
		{"equal term/equal last log index/equal last log term", true, false, false, 0, 0, 0},
		{"equal term/equal last log index/higher last log term", true, false, false, 0, 0, 1},
		{"equal term/higher last log index/lower last log term", false, false, false, 0, 1, -1},
		{"equal term/higher last log index/equal last log term", true, false, false, 0, 1, 0},
		{"equal term/higher last log index/higher last log term", true, false, false, 0, 1, 1},

		{"higher term/lower last log index/lower last log term", false, false, true, 1, -1, -1},
		{"higher term/lower last log index/equal last log term", false, false, true, 1, -1, 0},
		{"higher term/lower last log index/higher last log term", true, true, true, 1, -1, 1},
		{"higher term/equal last log index/lower last log term", false, false, true, 1, 0, -1},
		{"higher term/equal last log index/equal last log term", true, true, true, 1, 0, 0},
		{"higher term/equal last log index/higher last log term", true, true, true, 1, 0, 1},
		{"higher term/higher last log index/lower last log term", false, false, true, 1, 1, -1},
		{"higher term/higher last log index/equal last log term", true, true, true, 1, 1, 0},
		{"higher term/higher last log index/higher last log term", true, true, true, 1, 1, 1},
	}

	// Run subtests
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			student, mocks, err := createCluster(t)
			if err != nil {
				t.Fatal("unable to create cluster", err)
			}
			defer cleanupMockCluster(student, mocks)

			// Send vote request to student node...
			initialStudentState := student.GetState()
			studentTerm := student.GetCurrentTerm()
			studentLLI := student.LastLogIndex()
			studentLLT := student.GetLog(studentLLI).GetTermId()

			request := raft.RequestVoteRequest{
				Candidate:    mocks[0].RemoteSelf,
				Term:         uint64(int64(studentTerm) + tc.candidateTermOffset),
				LastLogIndex: uint64(int64(studentLLI) + tc.lastLogIndexOffset),
				LastLogTerm:  uint64(int64(studentLLT) + tc.lastLogTermOffset),
			}

			reply, err := mocks[0].studentRaftClient.RequestVoteCaller(context.Background(), &request)
			if err != nil {
				t.Error("error sending vote request to student raft", err)
			}

			// Check term
			expectedStudentTerm := maxTerm(studentTerm, request.Term)
			// might be greater, since studentTerm could have grown if config change is delayed
			// but reply should be >= a term read in the past, and term from request
			if reply.GetTerm() < expectedStudentTerm {
				t.Errorf("term mismatch, expected %v, got %v", expectedStudentTerm, reply.GetTerm())
			}

			// Check vote
			voted, shouldIgnore, votedFor := reply.GetVoteGranted(), request.GetTerm() <= expectedStudentTerm, student.GetVotedFor()

			if initialStudentState == raft.FollowerState && voted != tc.expectedFollowerVote {
				t.Errorf("follower vote mismatch, expected %v, got %v", tc.expectedFollowerVote, reply.GetVoteGranted())
			}

			if initialStudentState != raft.FollowerState && voted != tc.expectedCandidateLeaderVote {
				t.Errorf("candidate/leader vote mismatch, expected %v, got %v", tc.expectedCandidateLeaderVote, reply.GetVoteGranted())
			}

			if (voted && votedFor != request.Candidate.Id) || (!shouldIgnore && !voted && votedFor != "") {
				t.Errorf("student didn't set votedFor correctly (was '%v', voted %v)", votedFor, voted)
				t.Errorf("current term: %v", student.GetCurrentTerm())
			}

			time.Sleep(50 * time.Millisecond)

			// Check state transition
			studentDidFallback := (student.GetState() == raft.FollowerState)
			if initialStudentState != raft.FollowerState && tc.expectedFallback != studentDidFallback {
				t.Errorf("fallback mismatch, expected %v, got %v, student state is %v",
					tc.expectedFallback, studentDidFallback, student.GetState())
			}
		})
	}
}
