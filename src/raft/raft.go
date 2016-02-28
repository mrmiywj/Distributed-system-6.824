package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
//the log struct for the raft server
//

type Log struct {
	term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader           int
	currentTerm      int
	votedFor         int
	log              []Log
	commitIndex      int
	lastApplied      int
	matchIndex       []int
	nextIndex        []int
	isLeader         bool
	isCandidate      bool
	isFollower       bool
	timerForElection *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.me == rf.leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply = new(RequestVoteReply)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.isCandidate || rf.isLeader {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
		if rf.lastApplied == 0 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
		if args.LastLogTerm > rf.log[rf.lastApplied].term {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		} else {
			if args.LastLogTerm == rf.log[rf.lastApplied].term && args.LastLogIndex > rf.lastApplied {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//Struct for appendEntries RPC
//
type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Log
	leaderCommit int
}

//
//Struct for appendEnries reply
//
type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) appendEntries(args AppendEntriesArgs, rep *AppendEntriesReply) {
	if len(args.entries) == 0 {
		if rf.isFollower {
			rf.currentTerm = args.term
			rf.votedFor = args.leaderId
			rep.success = true
			return
		}
		if args.term < rf.currentTerm {
			rep.term = rf.currentTerm
			rep.success = false
			return
		}
		rf.isCandidate = false
		rf.isLeader = false
		rf.isFollower = true
		rf.currentTerm = args.term
		rf.leader = args.leaderId
		rep.term = rf.currentTerm
		rep.success = true
		return
	}
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	//go func() {
	//	t := rand.Intn(150) + 150
	//	selectCh <- time.After(time.Duration(t) * time.Millisecond)
	//}()
	rf.votedFor = -1
	rf.isLeader = false
	rf.isCandidate = false
	rf.isFollower = true
	rf.lastApplied = 0
	rf.commitIndex = 0
	t := rand.Intn(150) + 150
	rf.timerForElection = time.NewTimer(time.Duration(t))
	go func(t *time.Timer, d time.Duration) {
		for {
			select {
			case <-t.C:
				tmpTime := rand.Intn(150) + 150
				t.Reset(time.Duration(tmpTime))
				if rf.isCandidate || rf.isFollower {
					//be Candidate, run election
					rf.isCandidate = true
					rf.isFollower = false
					rf.isLeader = false
					rf.currentTerm++
					count := 0
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							args := RequestVoteArgs{rf.currentTerm, i, rf.commitIndex, rf.lastApplied}
							rep := &RequestVoteReply{}
							rf.sendRequestVote(me, args, rep)
							if rep.VoteGranted {
								count++
							}
							if rep.Term > rf.currentTerm {
								rf.currentTerm = rep.Term
							}
						}
					}
					if count >= len(rf.peers)/2 {
						rf.leader = rf.me
						rf.isLeader = true
						rf.isFollower = false
						rf.isCandidate = false
					} else {
						rf.isFollower = true
						rf.isCandidate = false
					}
				}

			}
		}
	}(rf.timerForElection, time.Duration(t))
	go func() {

	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
