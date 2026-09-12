package raft

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRoleString(t *testing.T) {
	cases := map[Role]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader", Role(9): "Role(9)"}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", int(r), got, want)
		}
	}
}

func TestMessageJSONRoundTrip(t *testing.T) {
	cases := []Message{
		{From: "a", To: "b", Type: MsgRequestVote, RequestVote: &RequestVote{Term: 3, CandidateID: "a", LastLogIndex: 7, LastLogTerm: 2}},
		{From: "b", To: "a", Type: MsgRequestVoteResponse, RequestVoteResponse: &RequestVoteResponse{Term: 3, VoteGranted: true}},
		{From: "a", To: "b", Type: MsgAppendEntries, AppendEntries: &AppendEntries{
			Term: 3, LeaderID: "a", PrevLogIndex: 7, PrevLogTerm: 2,
			Entries:      []LogEntry{{Index: 8, Term: 3, Command: []byte("SET x 1")}},
			LeaderCommit: 6,
		}},
		{From: "b", To: "a", Type: MsgAppendEntriesResponse, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 8}},
	}
	for _, in := range cases {
		t.Run(in.Type.String(), func(t *testing.T) {
			if err := in.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			data, err := json.Marshal(in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var out Message
			if err := json.Unmarshal(data, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(in, out) {
				t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v\njson=%s", in, out, data)
			}
			if out.Term() != in.Term() {
				t.Fatalf("Term() = %d, want %d", out.Term(), in.Term())
			}
		})
	}
}

func TestMessageValidate(t *testing.T) {
	bad := []Message{
		{Type: MsgRequestVote}, // no payload
		{Type: MsgRequestVote, RequestVoteResponse: &RequestVoteResponse{}},                    // wrong payload
		{Type: MsgAppendEntries, AppendEntries: &AppendEntries{}, RequestVote: &RequestVote{}}, // two payloads
		{Type: 0, RequestVote: &RequestVote{}},                                                 // unknown type
	}
	for i, m := range bad {
		if err := m.Validate(); err == nil {
			t.Errorf("case %d: Validate() = nil, want error", i)
		}
	}
}

func TestMessageTermMalformed(t *testing.T) {
	// Term must not panic on a malformed envelope; it reports 0 (no term) and
	// leaves the diagnosis to Validate.
	bad := []Message{
		{Type: MsgRequestVote},
		{Type: MsgRequestVoteResponse},
		{Type: MsgAppendEntries},
		{Type: MsgAppendEntriesResponse},
		{Type: 0, RequestVote: &RequestVote{Term: 3}},
	}
	for i, m := range bad {
		if got := m.Term(); got != 0 {
			t.Errorf("case %d: Term() = %d, want 0", i, got)
		}
		if m.Validate() == nil {
			t.Errorf("case %d: Validate() = nil, want error", i)
		}
	}
}
