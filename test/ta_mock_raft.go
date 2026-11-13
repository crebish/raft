package test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	raft "raft/pkg"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"raft/pkg/hashmachine"
)

// ErrMock is general error for mock RPC calls
var ErrMock error = fmt.Errorf("error by Mock Raft")

// DefaultJoinCaller is Default implementation. Does nothing except return true.
func DefaultJoinCaller(ctx context.Context, r *raft.RemoteNode) (*raft.JoinReply, error) {
	return &raft.JoinReply{Peers: nil}, nil
}

// DefaultAppendEntriesCaller is Default implementation. Does nothing except return true and echo the term.
func DefaultAppendEntriesCaller(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	reply.Term = req.Term
	reply.Success = true
	return reply, nil
}

// DefaultRequestVoteCaller is Default implementation. Echoes the term and always votes positively.
func DefaultRequestVoteCaller(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	reply.Term = req.Term
	reply.VoteGranted = true

	return reply, nil
}

// DefaultClientRequestCaller is Default implementation. Returns empty, since it's not useful for testing student code.
func DefaultClientRequestCaller(ctx context.Context, req *raft.ClientRequest) (*raft.ClientReply, error) {
	return &raft.ClientReply{}, nil
}

// uncomment for capstoners implementing Snapshots only.
/*func DefaultInstallSnapshotCaller(ctx context.Context, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{Term: req.GetTerm()}, nil
}*/

// ErrorJoinCaller is Error implementation. Always returns a ErrMock.
func ErrorJoinCaller(ctx context.Context, r *raft.RemoteNode) (*raft.JoinReply, error) {
	return nil, ErrMock
}

// ErrorAppendEntriesCaller is Error implementation. Always returns a ErrMock.
func ErrorAppendEntriesCaller(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesReply, error) {
	return nil, ErrMock
}

// ErrorRequestVoteCaller is Error implementation. Always returns a ErrMock.
func ErrorRequestVoteCaller(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteReply, error) {
	return nil, ErrMock
}

// ErrorClientRequestCaller is Error implementation. Always returns a ErrMock.
func ErrorClientRequestCaller(ctx context.Context, req *raft.ClientRequest) (*raft.ClientReply, error) {
	return nil, ErrMock
}

// MockRaft implements RaftRPCServer
type MockRaft struct {
	raft.UnimplementedRaftRPCServer
	Join          func(ctx context.Context, r *raft.RemoteNode) (*raft.JoinReply, error)
	AppendEntries func(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesReply, error)
	RequestVote   func(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteReply, error)
	ClientRequest func(ctx context.Context, req *raft.ClientRequest) (*raft.ClientReply, error)
	//InstallSnapshot func(ctx context.Context, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotReply, error)

	RemoteSelf        *raft.RemoteNode
	server            *grpc.Server
	StudentRaft       *raft.Node
	studentRaftClient raft.RaftRPCClient

	receivedReqs  []*raft.AppendEntriesRequest
	receivedMutex sync.Mutex

	t *testing.T
}

// JoinCaller implements RPC
func (m *MockRaft) JoinCaller(ctx context.Context, r *raft.RemoteNode) (*raft.JoinReply, error) {
	return m.Join(ctx, r)
}

// AppendEntriesCaller implements RPC
func (m *MockRaft) AppendEntriesCaller(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesReply, error) {
	return m.AppendEntries(ctx, req)
}

// RequestVoteCaller implements RPC
func (m *MockRaft) RequestVoteCaller(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteReply, error) {
	return m.RequestVote(ctx, req)
}

// ClientRequestCaller implements RPC
func (m *MockRaft) ClientRequestCaller(ctx context.Context, req *raft.ClientRequest) (*raft.ClientReply, error) {
	return m.ClientRequest(ctx, req)
}

/*
func (m *MockRaft) InstallSnapshotCaller(ctx context.Context, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotReply, error) {
	return m.InstallSnapshot(ctx, req)
}*/

// Stop the grpc server at this MockRaft
func (m *MockRaft) Stop() {
	m.server.Stop()
}

// JoinCluster calls JoinRPC on the student node
func (m *MockRaft) JoinCluster() (err error) {
	_, err = m.studentRaftClient.JoinCaller(context.Background(), m.RemoteSelf)
	return
}

// NewDefaultMockRaft creates a new mockraft with the default config
func NewDefaultMockRaft(studentRaft *raft.Node) (m *MockRaft, err error) {
	m = &MockRaft{
		Join:          DefaultJoinCaller,
		AppendEntries: DefaultAppendEntriesCaller,
		RequestVote:   DefaultRequestVoteCaller,
		ClientRequest: DefaultClientRequestCaller,
		//InstallSnapshot: DefaultInstallSnapshotCaller,

		StudentRaft: studentRaft,
	}

	err = m.init(raft.DefaultConfig())

	return
}

func (m *MockRaft) init(config raft.Config) error {
	m.server = grpc.NewServer()
	raft.RegisterRaftRPCServer(m.server, m)
	conn := raft.OpenPort(0)
	addr := fmt.Sprintf("%v", conn.Addr().String())
	id := raft.AddrToID(addr, config.NodeIDSize)
	m.RemoteSelf = &raft.RemoteNode{Addr: addr, Id: id}

	cc, err := m.StudentRaft.Self.ClientConn()
	if err != nil {
		return err
	}

	m.StudentRaft.InitStableStore()
	m.studentRaftClient = cc

	go m.server.Serve(conn)
	return nil
}

// set the testing.T associated with this MockRaft
func (m *MockRaft) setTestingT(t *testing.T) {
	m.t = t
}

// MockCluster creates a cluster from one student raft and ClusterSize-1 mock rafts, connect the mock rafts to the student one.
func MockCluster(joinThem bool, config raft.Config, t *testing.T) (studentRaft *raft.Node, mockRafts []*MockRaft, err error) {
	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	studentRaft, err = raft.CreateNode(raft.OpenPort(0), config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		return
	}
	if err = studentRaft.Connect(nil); err != nil {
		return
	}
	mockRafts = make([]*MockRaft, config.ClusterSize-1)
	errChan := make(chan error)

	for i := range mockRafts {
		var mr *MockRaft
		mr, err = NewDefaultMockRaft(studentRaft)
		if err != nil {
			return
		}
		mockRafts[i] = mr
		mr.setTestingT(t)
		if joinThem {
			go func(mr *MockRaft) {
				errChan <- mr.JoinCluster()
			}(mr)
		}
	}

	if joinThem {
		for range mockRafts {
			if err = <-errChan; err != nil {
				return
			}
		}
	}
	return
}

func createFollowerMockCluster(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error) {
	// Setup config
	config := raft.DefaultConfig()
	config.ElectionTimeout = 60 * time.Second // Set large election timeout
	config.ClusterSize = 3
	// Create cluster
	student, mocks, err = MockCluster(true, config, t)
	if err != nil {
		t.Fatal("couldn't set up mock cluster", err)
	}
	time.Sleep(150 * time.Millisecond)

	// Ensure student node becomes follower
	if student.GetState() != raft.FollowerState {
		t.Fatal("student state was not follower, was: ", student.GetState())
	}

	// Set follower term
	student.SetCurrentTerm(1)

	return
}

func createFollowerMockClusterWith3Entries(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error) {
	student, mocks, err = createFollowerMockCluster(t)
	if err != nil {
		return nil, nil, err
	}

	// Add 3 entries to follower
	studentTerm := student.GetCurrentTerm()
	studentLLI := student.LastLogIndex()
	for i := 0; i < 3; i++ {
		student.StoreLog(&raft.LogEntry{
			TermId: studentTerm,
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   raft.CommandType_NOOP,
			Data:   []byte{},
		})
	}

	return student, mocks, err
}

func createCandidateMockCluster(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error) {
	// Create mock cluster with one student node
	config := raft.DefaultConfig()

	student, mocks, err = MockCluster(false, config, t)
	if err != nil {
		t.Error("couldn't set up mock cluster", err)
	}

	// Setup mocks to always deny votes
	denyVote := func(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteReply, error) {
		return &raft.RequestVoteReply{Term: req.Term, VoteGranted: false}, nil
	}
	mocks[0].RequestVote = denyVote
	mocks[1].RequestVote = denyVote
	mocks[0].JoinCluster()
	mocks[1].JoinCluster()

	time.Sleep(100 * time.Millisecond)

	// Ensure student node becomes candidate
	time.Sleep(raft.DefaultConfig().ElectionTimeout * 4)
	if student.GetState() != raft.CandidateState {
		t.Error("student state was not candidate, was:", student.GetState())
	}

	// Change student node election timeout to ensure term remains the same
	config.ElectionTimeout = time.Duration(60 * time.Second)

	// Add entries to student log cache
	numEntries := 3
	studentLLI := student.LastLogIndex()
	for i := 0; i < numEntries; i++ {
		student.StoreLog(&raft.LogEntry{
			TermId: student.GetCurrentTerm(),
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   raft.CommandType_NOOP,
			Data:   []byte{},
		})
	}

	return
}

func createLeaderMockCluster(t *testing.T) (student *raft.Node, mocks []*MockRaft, err error) {
	// Create mock cluster with one student node, and mocks that always vote yes
	config := raft.DefaultConfig()
	student, mocks, err = MockCluster(true, config, t)
	if err != nil {
		t.Error("Couldn't set up mock cluster", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Ensure student node becomes leader
	time.Sleep(raft.DefaultConfig().ElectionTimeout * 4)
	if student.GetState() != raft.LeaderState {
		t.Error("student state was not leader, was:", student.GetState())
	}

	// Check student node appends NOOP
	if student.LastLogIndex() != 1 {
		t.Fatal("student didn't setup log when becoming leader")
	}

	if entry := student.GetLog(student.LastLogIndex()); entry.GetIndex() != 0 &&
		entry.GetTermId() != 1 &&
		entry.GetType() != raft.CommandType_NOOP {
		t.Fatal("student didn't append noop to log")
	}

	// Change student node election timeout to ensure term remains the same
	student.Config.ElectionTimeout = time.Duration(60 * time.Second)

	// Add entries to student log cache
	numEntries := 3
	studentLLI := student.LastLogIndex()
	for i := 0; i < numEntries; i++ {
		student.StoreLog(&raft.LogEntry{
			TermId: student.GetCurrentTerm(),
			Index:  uint64(int(studentLLI) + i + 1),
			Type:   raft.CommandType_NOOP,
			Data:   []byte{},
		})
	}

	return
}

// maxTerm computes argmax over a list of terms
func maxTerm(terms ...uint64) (max uint64) {
	for _, t := range terms {
		if t > max {
			max = t
		}
	}
	return
}

// cleanupMockCluster stops all the grpc servers, removes raftlogs, and closes client connections
func cleanupMockCluster(student *raft.Node, mockRafts []*MockRaft) {
	var wg sync.WaitGroup
	wg.Add(1)
	student.Server.Stop()
	go func(student *raft.Node) {
		student.GracefulExit()
		student.RemoveLogs()
		wg.Done()
	}(student)

	wg.Wait()
	for _, n := range mockRafts {
		n.Stop()
		n.RemoteSelf.RemoveClientConn()
	}

	student.Self.RemoveClientConn()
}
