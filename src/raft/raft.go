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

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"


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

type LogEntry struct {
	Index int
	Command interface{}
	Term int
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success		 bool
	LastMatchIndex int
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
	//Persistent state on all servers:
	currentTerm int  //当前term
	votedFor int     //当前任期内收到选票的候选者id
	log[] LogEntry   //日志条目

	//Volatile state on all servers:
	commitIndex int   //已知已提交的最高的日志条目的index
	lastApplied int   //已经被应用到状态机的最高的日志条目的index

	//Volatile state on leaders:
	nextIndex []int
	matchIndex []int

	state int
    ElectionTimeout time.Duration
	timer *time.Timer
	CountVote int
	ApplyCh chan ApplyMsg
}
const (
	Follower =0
	Candidate =1
	Leader =2
	MinTimeout =300
	MaxTimeout =500
)
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader=false
	if rf.state==Leader {
		isleader=true
	}
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm{       //若currentTerm比投票请求Term大，那么投票请求失败
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
   if args.Term >= rf.currentTerm{
   		reply.Term = args.Term
		reply.VoteGranted = false
		if args.Term > rf.currentTerm{   //若currentTerm比投票请求Term小，那么当前的节点成为follower
			rf.ToBeFollower()
			rf.currentTerm = args.Term
		}

		canVote := true
		if len(rf.log) > 0{
			term := rf.log[len(rf.log)-1].Term
			index := rf.log[len(rf.log)-1].Index
			lastindex  :=args.LastLogIndex
			lastterm := args.LastLogTerm
			if term > lastterm || term == lastterm && index > lastindex {
				canVote = false
			}

		}
		if rf.votedFor == -1 && canVote{
			rf.ToBeFollower()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			//重设计时器
			rf.resetTimeout()
	    }
   }
}
//a server become a follower
func (rf *Raft) ToBeFollower(){
	rf.state = Follower
	rf.votedFor = -1
	rf.CountVote = 0
}
//a server become a candidate
func (rf *Raft) ToBeCandidate(){
	rf.state = Candidate
	rf.CountVote = 1
	rf.votedFor = rf.me
}
//a server become a leader
func (rf *Raft) ToBeLeader(){
	rf.state = Leader
	for k := 0; k < len(rf.peers); k++ {
		if k != rf.me {
			rf.nextIndex[k] = len(rf.log)
			rf.matchIndex[k] = -1
		}
	}
}

//Reset timeout
func (rf *Raft)resetTimeout(){
	if rf.state != Leader {
		rf.ElectionTimeout = time.Duration(MinTimeout+rand.Int63n(MaxTimeout-MinTimeout)) * time.Millisecond
	} else {
		rf.ElectionTimeout = time.Millisecond * 50
	}
	rf.timer.Reset(rf.ElectionTimeout)
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft)tackleVoteReply(reply RequestVoteReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term >= rf.currentTerm{  //若回复的任期比当前大，当前节点重置为follower，并将自己的任期更新
	   if reply.Term > rf.currentTerm {
		   rf.ToBeFollower()
		   rf.currentTerm = reply.Term
		   rf.resetTimeout()
		   return
	   }

	   if rf.state == Candidate && reply.VoteGranted {  //若回复的任期相等并且当前节点是candidate并且同意投票，得到票数
		  rf.CountVote++
		  if rf.CountVote >= (len(rf.peers)/2 + 1) { //超过半数的投票，成为leader
		  	rf.ToBeLeader()
			rf.resetTimeout() //重置计时器
		  }
	   }
	}
}

func (rf *Raft)tackleTimeout()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.resetTimeout()

	if rf.state == Leader{  //节点是leader的时候超时，向所有follower重新发送心跳/appendentry
		rf.broadAppendEntries()

	}else{                  //节点不是leader的时候超时，当前节点成为candidate并发起投票
		rf.ToBeCandidate()
		rf.currentTerm++
		rf.persist()

		var reqArgs RequestVoteArgs
		reqArgs.CandidateId = rf.me
		reqArgs.Term = rf.currentTerm
		reqArgs.LastLogIndex = len(rf.log)-1
		if len(rf.log)  >0 {
			reqArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
		}
		for k := 0; k < len(rf.peers); k++ {
			// 开启请求投票线程
			if k != rf.me {
				go func(server int, args RequestVoteArgs) {
					var reply RequestVoteReply
					if rf.sendRequestVote(server, args, &reply) {
						rf.tackleVoteReply(reply)
					}
				}(k, reqArgs)
			}
		}
	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isleader :=false
	if rf.state == Leader{
		isleader = true
	}
	if isleader {
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Index: len(rf.log), Term: term, Command: command})
		index = len(rf.log)
		rf.persist()
	}
	return index, term, isleader
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

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.resetTimeout()
	defer rf.persist()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.ToBeFollower()
	rf.currentTerm = args.Term
	reply.Term = args.Term
	preindex :=args.PreLogIndex
	//进行一致性检查
	if preindex >= 0 && (len(rf.log)-1 < preindex || rf.log[preindex].Term != args.PreLogTerm) {
		if reply.LastMatchIndex > args.PreLogIndex {
			reply.LastMatchIndex = args.PreLogIndex
		}else{
			reply.LastMatchIndex = len(rf.log) - 1
		}
		for ; reply.LastMatchIndex >= 0; reply.LastMatchIndex-- {
			if rf.log[reply.LastMatchIndex].Term == args.PreLogTerm {  // 如果当前term值相等，则跳出循环
				break
			}
		}
		reply.Success = false
	} else {
		if args.Entries == nil { //heartbeats
			if len(rf.log)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				}
				for k := rf.lastApplied + 1; k <= rf.commitIndex; k++ {
					applymsg := ApplyMsg{}
					applymsg.Index = k+1
					applymsg.Command = rf.log[k].Command
					rf.ApplyCh <- applymsg

				}
				rf.lastApplied = rf.commitIndex
			}
			reply.LastMatchIndex = args.PreLogIndex

		} else {  //AppendEntries
			rf.log = rf.log[:args.PreLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
			if len(rf.log)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				}
				for k := rf.lastApplied + 1; k <= rf.commitIndex; k++ {
					applymsg := ApplyMsg{}
					applymsg.Index = k+1
					applymsg.Command = rf.log[k].Command
					rf.ApplyCh <- applymsg
				}
				rf.lastApplied = rf.commitIndex
			}
			reply.LastMatchIndex = len(rf.log) - 1
		}
		reply.Success = true
	}
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft)tackleAppendEntries(server int, reply AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		if reply.Term > rf.currentTerm { //如果回复的term大于当前的term，当前节点变成follower
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.CountVote = 0
			rf.resetTimeout()
			return
		}
		rf.nextIndex[server] = reply.LastMatchIndex + 1

		if reply.Success == false {
			rf.broadAppendEntries() //不成功就重新广播
			return
		}
		rf.matchIndex[server] = reply.LastMatchIndex
		var cnt int
		cnt = 0
		for k := 0; k < len(rf.peers); k++ {
			if k == rf.me || rf.matchIndex[k] >= rf.matchIndex[server] {
				cnt++
			}
		}
		if cnt >= (len(rf.peers)/2+1) && rf.commitIndex < rf.matchIndex[server] && rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			if rf.commitIndex > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			}
			for k := rf.lastApplied + 1; k <= rf.commitIndex; k++ {
				applymsg := ApplyMsg{}
				applymsg.Index = k+1
				applymsg.Command = rf.log[k].Command
				rf.ApplyCh <- applymsg
			}
			rf.lastApplied = rf.commitIndex
		}
	}

}
func (rf *Raft)broadAppendEntries()  {

	for k := 0; k < len(rf.peers); k++ {
		//向其他所有节点发送append entries
		if k != rf.me {
			//初始化AppendEntriesArgs
			var entryArg AppendEntriesArgs
			entryArg.LeaderId = rf.me
			entryArg.Term = rf.currentTerm
			entryArg.PreLogIndex = rf.nextIndex[k] - 1
			entryArg.LeaderCommit = rf.commitIndex
			if entryArg.PreLogIndex >= 0 {
				entryArg.PreLogTerm = rf.log[entryArg.PreLogIndex].Term
			}
			//成为Leader后，先发送heartbeats，收到回复后再更新nextIndex
			if rf.nextIndex[k] < len(rf.log){
				entryArg.Entries = rf.log[rf.nextIndex[k]:]
			}
			//开启线程
			go func (s int, args AppendEntriesArgs){
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(s, args, &reply)
				if ok{
					rf.tackleAppendEntries(s, reply)
				}
			}(k, entryArg)
		}
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.ApplyCh = applyCh
	rf.ElectionTimeout = 1000 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.timer = time.NewTimer(rf.ElectionTimeout)
	go func() {
		for {
			<-rf.timer.C
			rf.tackleTimeout()
		}
	}()
	rf.resetTimeout()
	return rf
}
