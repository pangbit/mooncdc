// Test adapter for the pinned, unmodified jackc/pglogrepl parser.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jackc/pglogrepl"
)

type vector struct {
	Name string `json:"name"`
	Hex  string `json:"hex"`
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
	m, err := pglogrepl.Parse(b)
	if err != nil {
		r.Status = "error"
		return
	}
	r.Status = "ok"
	row := func(kind string, id uint32, oldKind uint8, old, next *pglogrepl.TupleData) any {
		var tag any
		if oldKind != 0 {
			tag = string(oldKind)
		}
		return map[string]any{"kind": kind, "relation": id, "old_kind": tag, "old": tuple(old), "new": tuple(next)}
	}
	switch m := m.(type) {
	case *pglogrepl.InsertMessage:
		r.Value = row("I", m.RelationID, 0, nil, m.Tuple)
	case *pglogrepl.UpdateMessage:
		r.Value = row("U", m.RelationID, m.OldTupleType, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		r.Value = row("D", m.RelationID, m.OldTupleType, m.OldTuple, nil)
	case *pglogrepl.LogicalDecodingMessage:
		r.Value = map[string]any{"kind": "M", "lsn": m.LSN.String(), "transactional": m.Transactional, "prefix": m.Prefix, "content": hex.EncodeToString(m.Content)}
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
	if err := json.Unmarshal(b, &input); err != nil {
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
	if err := os.WriteFile(os.Args[2], b, 0600); err != nil {
		panic(err)
	}
}
