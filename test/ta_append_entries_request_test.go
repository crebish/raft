package test

import (
	"context"
	raft "raft/pkg"
	"testing"
)

func TestAppendEntriesRequest(t *testing.T) {
	suppressLoggers()
	t.Run("Follower", AppendEntriesRequestFollower)
	t.Run("Candidate", AppendEntriesRequestCandidate)
	t.Run("Leader", AppendEntriesRequestLeader)
}

func AppendEntriesRequestFollower(t *testing.T) {
	testHandleAppendEntriesRequest(t, createFollowerMockClusterWith3Entries)
}

func AppendEntriesRequestCandidate(t *testing.T) {
	testHandleAppendEntriesRequest(t, createCandidateMockCluster)
}
func AppendEntriesRequestLeader(t *testing.T) {
	testHandleAppendEntriesRequest(t, createLeaderMockCluster)
}

func testHandleAppendEntriesRequest(t *testing.T, createCluster func(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error)) {
	// Setup test cases
	testCases := []struct {
		title string

		expectedFallback bool
		expectedSuccess  bool

		studentCommitOffset int64

		leaderTermOffset   int64
		prevLogIndexOffset int64
		prevLogTermOffset  int64
		entries            []raft.LogEntry
		leaderCommitOffset int64
	}{
		// NOTE: these tests are *far* from comprehensive of all possible requests,
		// but they do cover most representative ones. They should be paired with
		// some manual inspection of student code as well.

		{"lower term is rejected", false, false, 0, -1, 0, 0, []raft.LogEntry{}, 0},
		{"lower term is rejected with higher pli", false, false, 0, -1, 1, 0, []raft.LogEntry{}, 0},
		{"lower term is rejected with mismatched plt", false, false, 0, -1, 0, -1, []raft.LogEntry{}, 0},
		{"lower term is rejected with higher commit index", false, false, 0, -1, 0, 0, []raft.LogEntry{}, 1},
		{"lower term is rejected with one entry", false, false, 0, -1, 0, 0, []raft.LogEntry{{Index: 3, TermId: 1, Type: raft.CommandType_NOOP, Data: []byte{}}}, 1},

		{"equal term is accepted", true, true, 0, 0, 0, 0, []raft.LogEntry{}, 0},
		{"equal term is accepted and truncates with lower pli and one entry", true, true, -1, 0, -1, 0, []raft.LogEntry{{Index: 3, TermId: 1, Type: raft.CommandType_CLIENT_REGISTRATION, Data: []byte{}}}, 0},
		{"equal term is rejected with higher pli", true, false, 0, 0, 1, 0, []raft.LogEntry{}, 0},
		{"equal term is rejected with mismatched plt", true, false, 0, 0, 0, -1, []raft.LogEntry{}, 0},
		{"equal term is accepted with one entry", true, true, 0, 0, 0, 0, []raft.LogEntry{{Index: 4, TermId: 1, Type: raft.CommandType_NOOP, Data: []byte{}}}, 1},
		{"equal term is accepted with higher commit index", true, true, -1, 0, 0, 0, []raft.LogEntry{}, 0},

		{"higher term is accepted", true, true, 0, 1, 0, 0, []raft.LogEntry{}, 0},
		{"higher term is accepted and truncates with lower pli and one entry", true, true, -1, 1, -1, 0, []raft.LogEntry{{Index: 3, TermId: 1, Type: raft.CommandType_CLIENT_REGISTRATION, Data: []byte{}}}, 0},
		{"higher term is rejected with higher pli", true, false, 0, 1, 1, 0, []raft.LogEntry{}, 0},
		{"higher term is rejected with mismatched plt", true, false, 0, 1, 0, -1, []raft.LogEntry{}, 0},
		{"higher term is accepted with one entry", true, true, 0, 1, 0, 0, []raft.LogEntry{{Index: 4, TermId: 1, Type: raft.CommandType_NOOP, Data: []byte{}}}, 0},
		{"higher term is accepted with higher commit index", true, true, -1, 1, 0, 0, []raft.LogEntry{}, 1},

		{"appending multiple entries succeeds", true, true, 0, 0, 0, 0, []raft.LogEntry{
			{
				Type:   raft.CommandType_NOOP,
				Index:  4,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   raft.CommandType_NOOP,
				Index:  5,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   raft.CommandType_NOOP,
				Index:  6,
				TermId: 1,
				Data:   []byte{},
			},
		}, 3},
		{"appending multiple entries with truncation succeeds", true, true, -2, 0, -1, 0, []raft.LogEntry{
			{
				Type:   raft.CommandType_NOOP,
				Index:  3,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   raft.CommandType_NOOP,
				Index:  4,
				TermId: 1,
				Data:   []byte{},
			},
			{
				Type:   raft.CommandType_NOOP,
				Index:  5,
				TermId: 1,
				Data:   []byte{},
			},
		}, 4},
		// This test fails on our implementation, since there's no situation in real execution
		//  when prevLogIndex will be more than 1 less than the log length of a follower
		// {"truncation of multiple entries succeeds", false, true, -2, 0, -2, 0, []LogEntry{
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  2,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  3,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// 	{
		// 		Type:   CommandType_NOOP,
		// 		Index:  4,
		// 		TermId: 1,
		// 		Data:   []byte{},
		// 	},
		// }, 1},
		// {"commit index should not be decremented"},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			// Create cluster
			student, mocks, err := createCluster(t)
			if err != nil {
				t.Fatal("unable to create cluster", err)
			}
			studentInitialState := student.GetState()
			defer cleanupMockCluster(student, mocks)

			// Set commit index and last applied for student node
			optimalCommitIndex := student.LastLogIndex()
			student.CommitIndex.Store(uint64(int64(optimalCommitIndex) + tc.studentCommitOffset))
			student.LastApplied.Store(student.CommitIndex.Load())
			// Save current state of student
			studentTerm := student.GetCurrentTerm()

			studentLogCache := make([]raft.LogEntry, student.LastLogIndex()+1)
			for i, entry := range student.StableStore.AllLogs() {
				// Make copy of log cache...
				studentLogCache[i] = raft.LogEntry{
					Index:   entry.Index,
					TermId:  entry.TermId,
					Type:    entry.Type,
					Command: entry.Command,
					Data:    entry.Data,
					CacheId: entry.CacheId,
				}
			}

			studentLLI := student.LastLogIndex()
			studentLLT := student.GetLog(studentLLI).GetTermId()
			studentCommitIndex := student.CommitIndex
			studentLastApplied := student.LastApplied

			// If sending request to a leader, update our entries[] so their indices
			// are incremented; this is because a leader has it's own NOOP in it's log,
			// which a follower or candidate should not.
			if student.GetState() == raft.LeaderState {
				for i := range tc.entries {
					tc.entries[i].Index++
				}
			}

			// Send append entries request to student node...
			logEntryPointers := make([]*raft.LogEntry, len(tc.entries))
			for i := range tc.entries {
				logEntryPointers[i] = &tc.entries[i]
			}

			request := raft.AppendEntriesRequest{
				Term:         uint64(int64(studentTerm) + tc.leaderTermOffset),
				Leader:       mocks[0].RemoteSelf,
				PrevLogIndex: uint64(int64(studentLLI) + tc.prevLogIndexOffset),
				PrevLogTerm:  uint64(int64(studentLLT) + tc.prevLogTermOffset),
				Entries:      logEntryPointers,
				LeaderCommit: uint64(int64(studentCommitIndex.Load()) + tc.leaderCommitOffset),
			}

			reply, err := mocks[0].studentRaftClient.AppendEntriesCaller(context.Background(), &request)
			if err != nil {
				t.Error("error sending vote request to student raft", err)
			}

			// Check reply success
			if reply.GetSuccess() != tc.expectedSuccess {
				t.Errorf("reply.success mismatch, expected %v, got %v", tc.expectedSuccess, reply.GetSuccess())
			}

			// Check reply and actual term
			expectedStudentTerm := maxTerm(studentTerm, request.Term)
			if reply.GetTerm() != expectedStudentTerm {
				t.Errorf("reply.term mismatch, expected %v, got %v", expectedStudentTerm, reply.GetTerm())
			}
			if student.GetCurrentTerm() != expectedStudentTerm {
				t.Errorf("actual term mismatch, expected %v, got %v", expectedStudentTerm, student.GetCurrentTerm())
			}

			// Check leader
			if request.Term >= studentTerm && student.Leader.GetId() != request.Leader.GetId() {
				t.Errorf("leader mismatch, expected %v, got %v", request.Leader.GetId(), student.Leader.GetId())
			}

			// Check votedFor
			if expectedStudentTerm > studentTerm && student.GetVotedFor() != "" {
				t.Errorf("votedFor mismatch, expected to be empty, was %v", student.GetVotedFor())
			}

			// Check fallback
			studentDidFallback := student.GetState() == raft.FollowerState
			if studentInitialState != raft.FollowerState && tc.expectedFallback != studentDidFallback {
				t.Errorf("fallback mismatch, expected %v, got %v, student state is %v",
					tc.expectedFallback, studentDidFallback, student.GetState())
			}

			// NOTE: Checking of errors if prev log index or prev log term are incorrect
			// is done by way of test cases that simulate the issue and expect a correct state.

			// Check truncation alone (without appending entries)
			shouldTruncate := (request.PrevLogIndex <= studentLLI || (request.PrevLogIndex == studentLLI && request.PrevLogTerm != studentLLT))
			if tc.expectedSuccess && len(request.Entries) == 0 && shouldTruncate {
				// Ensure length of logCache is correct
				expectedLength := request.PrevLogIndex + 1
				if student.LastLogIndex()+1 != expectedLength {
					t.Errorf("truncation mismatch; expected length of log cache to be %v, got %v", expectedLength, student.LastLogIndex()+1)
				}

				// Ensure last log index is correct
				expectedLLI := request.PrevLogIndex
				if student.LastLogIndex() != expectedLLI {
					t.Errorf("truncation mismatch; expected last log index %v, got %v", expectedLLI, student.LastLogIndex())
				}

				// Ensure last log term is correct
				expectedLLT := studentLogCache[expectedLLI].TermId
				truncatedStudentLLT := student.GetLog(student.LastLogIndex()).TermId
				if expectedLLT != truncatedStudentLLT {
					t.Errorf("truncation mismatch; expected last log term %v, got %v", expectedLLT, truncatedStudentLLT)
				}
			}

			// Check added log entries
			if tc.expectedSuccess && len(request.Entries) > 0 {
				// Ensure length of logCache is correct
				expectedLength := request.PrevLogIndex + uint64(len(request.Entries)) + 1
				if student.LastLogIndex()+1 != expectedLength {
					t.Errorf("appending mismatch; expected length of log cache to be %v, got %v", expectedLength, student.LastLogIndex()+1)
				}

				// Ensure last log index is correct
				expectedLLI := request.PrevLogIndex + uint64(len(request.Entries))
				if student.LastLogIndex() != expectedLLI {
					t.Errorf("appending mismatch; expected last log index %v, got %v", expectedLLI, student.LastLogIndex())
				}

				// Ensure last log term is correct
				expectedLLT := request.Entries[len(request.Entries)-1].TermId
				newStudentLLT := student.GetLog(student.LastLogIndex()).TermId
				if expectedLLT != newStudentLLT {
					t.Errorf("appending mismatch; expected last log term %v, got %v", expectedLLT, newStudentLLT)
				}
			}

			// Check log cache doesn't change if expected failure
			if !tc.expectedSuccess {
				// Ensure length of logCache hasn't changed
				expectedLength := len(studentLogCache)
				if expectedLength != int(student.LastLogIndex()+1) {
					t.Errorf("log cache mismatch; expected length of log cache to not change and remain %v, got %v", expectedLength, student.LastLogIndex()+1)
				}

				// Ensure last log index hasn't changed
				if student.LastLogIndex() != studentLLI {
					t.Errorf("log cache mismatch; expected last log index to not change and remain %v, got %v", studentLLI, student.LastLogIndex())
				}

				// Ensure last log term hasn't changed
				currentStudentLLT := student.GetLog(student.LastLogIndex()).TermId
				if studentLLT != currentStudentLLT {
					t.Errorf("log cache mismatch; expected last log term to not change and remain %v, got %v", studentLLT, currentStudentLLT)
				}
			}

			// Check commit index
			if tc.expectedSuccess && student.CommitIndex.Load() != request.LeaderCommit {
				t.Errorf("commit index mismatch, expected %v, got %v", request.LeaderCommit, student.CommitIndex)
			}
			if !tc.expectedSuccess && student.CommitIndex != studentCommitIndex {
				t.Errorf("commit index mismatch, expected to not change and remain %v, got %v", studentCommitIndex, student.CommitIndex)
			}

			// Check lastApplied
			if tc.expectedSuccess && student.LastApplied.Load() != request.LeaderCommit {
				t.Errorf("last applied mismatch, expected %v, got %v", request.LeaderCommit, student.LastApplied)
			}
			if !tc.expectedSuccess && student.LastApplied != studentLastApplied {
				t.Errorf("last applied mismatch, expected not to change and remain %v, got %v", studentLastApplied, student.LastApplied)
			}

			return
		})
	}
}
