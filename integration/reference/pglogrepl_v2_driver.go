// Test adapter for ParseV2 at the frozen pglogrepl revision. No parser changes.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
)

type vector struct {
	Name     string `json:"name"`
	Hex      string `json:"hex"`
	InStream bool   `json:"in_stream"`
}

type result struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Value  any    `json:"value"`
}

func tuple(t *pglogrepl.TupleData) any {
	if t == nil {
		return nil
	}
	values := make([]any, 0, len(t.Columns))
	for _, c := range t.Columns {
		v := []string{string(c.DataType)}
		if c.DataType == 'b' || c.DataType == 't' {
			v = append(v, hex.EncodeToString(c.Data))
		}
		values = append(values, v)
	}
	return values
}

func decode(v vector) (r result) {
	r = result{Name: v.Name, Status: "panic"}
	defer func() {
		if recover() != nil {
			r.Status = "panic"
			r.Value = nil
		}
	}()
	b, err := hex.DecodeString(v.Hex)
	if err != nil {
		panic(err)
	}
	m, err := pglogrepl.ParseV2(b, v.InStream)
	if err != nil {
		r.Status = "error"
		return
	}
	r.Status = "ok"
	row := func(kind string, xid, id uint32, oldKind uint8, old, next *pglogrepl.TupleData) any {
		var tag any
		if oldKind != 0 {
			tag = string(oldKind)
		}
		return map[string]any{"kind": kind, "xid": xid, "relation": id, "old_kind": tag, "old": tuple(old), "new": tuple(next)}
	}
	switch m := m.(type) {
	case *pglogrepl.StreamStartMessageV2:
		r.Value = map[string]any{"kind": "S", "xid": m.Xid, "first": m.FirstSegment == 1}
	case *pglogrepl.StreamStopMessageV2:
		r.Value = map[string]any{"kind": "E"}
	case *pglogrepl.StreamCommitMessageV2:
		r.Value = map[string]any{"kind": "c", "xid": m.Xid, "commit": m.CommitLSN.String(), "end": m.TransactionEndLSN.String(), "time": fmt.Sprint(m.CommitTime.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds())}
	case *pglogrepl.StreamAbortMessageV2:
		r.Value = map[string]any{"kind": "A", "xid": m.Xid, "subxid": m.SubXid}
	case *pglogrepl.InsertMessageV2:
		r.Value = row("I", m.Xid, m.RelationID, 0, nil, m.Tuple)
	case *pglogrepl.UpdateMessageV2:
		r.Value = row("U", m.Xid, m.RelationID, m.OldTupleType, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessageV2:
		r.Value = row("D", m.Xid, m.RelationID, m.OldTupleType, m.OldTuple, nil)
	case *pglogrepl.LogicalDecodingMessageV2:
		r.Value = map[string]any{"kind": "M", "xid": m.Xid, "lsn": m.LSN.String(), "transactional": m.Transactional, "prefix": m.Prefix, "content": hex.EncodeToString(m.Content)}
	case *pglogrepl.RelationMessageV2:
		columns := make([]any, 0, len(m.Columns))
		for _, c := range m.Columns {
			columns = append(columns, map[string]any{"name": c.Name, "oid": c.DataType, "modifier": int32(c.TypeModifier), "key": c.Flags == 1})
		}
		r.Value = map[string]any{"kind": "R", "xid": m.Xid, "relation": m.RelationID, "namespace": m.Namespace, "name": m.RelationName, "identity": m.ReplicaIdentity, "columns": columns}
	case *pglogrepl.TypeMessageV2:
		r.Value = map[string]any{"kind": "Y", "xid": m.Xid, "oid": m.DataType, "namespace": m.Namespace, "name": m.Name}
	case *pglogrepl.TruncateMessageV2:
		r.Value = map[string]any{"kind": "T", "xid": m.Xid, "relations": m.RelationIDs, "cascade": m.Option&1 != 0, "restart_identity": m.Option&2 != 0}
	default:
		panic(fmt.Sprintf("unexpected message %T", m))
	}
	return
}

func main() {
	if len(os.Args) != 3 {
		panic("input and output paths required")
	}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var input []vector
	if err = json.Unmarshal(b, &input); err != nil {
		panic(err)
	}
	output := make([]result, 0, len(input))
	for _, v := range input {
		output = append(output, decode(v))
	}
	b, err = json.MarshalIndent(output, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(os.Args[2], b, 0600); err != nil {
		panic(err)
	}
}
