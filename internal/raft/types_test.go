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

func TestValidateEntries(t *testing.T) {
	cases := []struct {
		name    string
		entries []LogEntry
		first   Index
		ok      bool
	}{
		{"empty", nil, 1, true},
		{"contiguous from 1", entries([2]uint64{1, 1}, [2]uint64{2, 1}), 1, true},
		{"contiguous from 5", entries([2]uint64{5, 2}, [2]uint64{6, 2}), 5, true},
		{"wrong start", entries([2]uint64{2, 1}), 1, false},
		{"gap", entries([2]uint64{1, 1}, [2]uint64{3, 1}), 1, false},
		{"term zero", entries([2]uint64{1, 0}), 1, false},
		{"term zero later", entries([2]uint64{1, 1}, [2]uint64{2, 0}), 1, false},
		{"first is the sentinel index", entries([2]uint64{0, 1}), 0, false},
		{"first is the sentinel index, no entries", nil, 0, false},
		{"last representable index", entries([2]uint64{^uint64(0), 1}), ^Index(0), true},
		{"index wraps past the maximum", entries([2]uint64{^uint64(0), 1}, [2]uint64{0, 1}), ^Index(0), false},
	}
	for _, tc := range cases {
		err := ValidateEntries(tc.entries, tc.first)
		if (err == nil) != tc.ok {
			t.Errorf("%s: ValidateEntries() = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestMessageValidateAppendEntriesPayload(t *testing.T) {
	ae := func(prev Index, es []LogEntry) Message {
		return Message{Type: MsgAppendEntries, AppendEntries: &AppendEntries{Term: 1, LeaderID: "a", PrevLogIndex: prev, Entries: es}}
	}
	if err := ae(7, entries([2]uint64{8, 1}, [2]uint64{9, 1})).Validate(); err != nil {
		t.Fatalf("well-formed AppendEntries rejected: %v", err)
	}
	if err := ae(7, nil).Validate(); err != nil {
		t.Fatalf("heartbeat rejected: %v", err)
	}
	bad := map[string]Message{
		"entries do not follow PrevLogIndex":              ae(7, entries([2]uint64{9, 1})),
		"gap inside entries":                              ae(7, entries([2]uint64{8, 1}, [2]uint64{10, 1})),
		"term-0 entry":                                    ae(7, entries([2]uint64{8, 0})),
		"PrevLogIndex at maximum wraps first to sentinel": ae(^Index(0), entries([2]uint64{0, 1})),
		"PrevLogIndex at maximum heartbeat":               ae(^Index(0), nil),
		"entries wrap past the maximum index":             ae(^Index(0)-1, entries([2]uint64{^uint64(0), 1}, [2]uint64{0, 1})),
	}
	for name, m := range bad {
		if m.Validate() == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}

// TestMessageClone: a clone shares nothing mutable with the original, so a
// transport can hold it while the sender keeps mutating its own copy.
func TestMessageClone(t *testing.T) {
	entries := []LogEntry{{Index: 8, Term: 3, Command: []byte("SET x 1")}}
	cases := []Message{
		{From: "a", To: "b", Type: MsgRequestVote, RequestVote: &RequestVote{Term: 3, CandidateID: "a", LastLogIndex: 7, LastLogTerm: 2}},
		{From: "b", To: "a", Type: MsgRequestVoteResponse, RequestVoteResponse: &RequestVoteResponse{Term: 3, VoteGranted: true}},
		{From: "a", To: "b", Type: MsgAppendEntries, AppendEntries: &AppendEntries{Term: 3, LeaderID: "a", PrevLogIndex: 7, PrevLogTerm: 2, Entries: entries, LeaderCommit: 6}},
		{From: "b", To: "a", Type: MsgAppendEntriesResponse, AppendEntriesResponse: &AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 8}},
		{From: "a", To: "b", Type: MsgAppendEntries, AppendEntries: &AppendEntries{Term: 3, LeaderID: "a"}}, // heartbeat, nil Entries
	}
	for _, in := range cases {
		t.Run(in.Type.String(), func(t *testing.T) {
			out := in.Clone()
			if !reflect.DeepEqual(in, out) {
				t.Fatalf("Clone() = %+v, want equal to %+v", out, in)
			}
			switch in.Type {
			case MsgRequestVote:
				if out.RequestVote == in.RequestVote {
					t.Fatal("RequestVote payload pointer is shared")
				}
				in.RequestVote.Term = 99
			case MsgRequestVoteResponse:
				if out.RequestVoteResponse == in.RequestVoteResponse {
					t.Fatal("RequestVoteResponse payload pointer is shared")
				}
				in.RequestVoteResponse.Term = 99
			case MsgAppendEntries:
				if out.AppendEntries == in.AppendEntries {
					t.Fatal("AppendEntries payload pointer is shared")
				}
				in.AppendEntries.Term = 99
				if len(in.AppendEntries.Entries) > 0 {
					in.AppendEntries.Entries[0].Command[0] = 'X'
					in.AppendEntries.Entries[0].Term = 99
				}
			case MsgAppendEntriesResponse:
				if out.AppendEntriesResponse == in.AppendEntriesResponse {
					t.Fatal("AppendEntriesResponse payload pointer is shared")
				}
				in.AppendEntriesResponse.Term = 99
			}
			if out.Term() == 99 {
				t.Fatal("mutating the original changed the clone's term")
			}
			if ae := out.AppendEntries; ae != nil && len(ae.Entries) > 0 {
				if string(ae.Entries[0].Command) != "SET x 1" || ae.Entries[0].Term != 3 {
					t.Fatalf("mutating the original changed the clone's entries: %+v", ae.Entries[0])
				}
			}
		})
	}
}
