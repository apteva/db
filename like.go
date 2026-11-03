package db

import (
	"bytes"
	"errors"
	"unicode/utf8"
)

// $like evaluates a SQL LIKE pattern over several fields joined by a separator:
//
//	{"$like": {"fields": ["a", "b"], "separator": " ", "pattern": "%23 op%"}}
//
// Semantics follow SQLite's default LIKE: % matches any run and _ exactly one
// UTF-8 character, ASCII letters compare case-insensitively, and there is no
// escape character. Missing and null fields read as the empty string; any
// other non-string value makes the predicate false. It never uses an index,
// so it filters the rows the other conjuncts' access path yields.
type likeSpec struct {
	fields    []string
	separator []byte
	pattern   []byte // ASCII-lowered
	literal   []byte // set when the pattern is %literal% with no wildcards inside
}

const (
	maxLikeFields  = 16
	maxLikePattern = 4096
)

func parseLike(operand any) (pred, error) {
	m, ok := operand.(map[string]any)
	if !ok {
		return pred{}, errors.New("$like needs an object")
	}
	spec := &likeSpec{}
	list, ok := m["fields"].([]any)
	if !ok || len(list) == 0 || len(list) > maxLikeFields {
		return pred{}, errors.New("$like needs 1–16 fields")
	}
	for _, f := range list {
		name, ok := f.(string)
		if !ok || name == "" || name[0] == '$' {
			return pred{}, errors.New("$like fields must be field names")
		}
		spec.fields = append(spec.fields, name)
	}
	if sep, exists := m["separator"]; exists {
		s, ok := sep.(string)
		if !ok {
			return pred{}, errors.New("$like separator must be a string")
		}
		spec.separator = []byte(s)
	}
	pattern, ok := m["pattern"].(string)
	if !ok || len(pattern) > maxLikePattern {
		return pred{}, errors.New("$like needs a string pattern of at most 4096 bytes")
	}
	for k := range m {
		if k != "fields" && k != "separator" && k != "pattern" {
			return pred{}, errors.New("unknown $like option " + k)
		}
	}
	spec.pattern = asciiLower(nil, []byte(pattern))
	if n := len(spec.pattern); n >= 2 && spec.pattern[0] == '%' && spec.pattern[n-1] == '%' {
		inner := spec.pattern[1 : n-1]
		if bytes.IndexAny(inner, "%_") < 0 {
			spec.literal = inner
		}
	}
	return pred{op: opLike, like: spec}, nil
}

func (s *likeSpec) matches(get func(string) value) bool {
	var stack [256]byte
	text := stack[:0]
	for i, f := range s.fields {
		if i > 0 {
			text = append(text, s.separator...)
		}
		v := get(f)
		switch v.k {
		case kMissing, kNull:
		case kString:
			text = asciiLower(text, v.s)
		default:
			return false
		}
	}
	if s.literal != nil {
		return bytes.Contains(text, s.literal)
	}
	return likeMatch(s.pattern, text)
}

func asciiLower(dst, src []byte) []byte {
	for _, c := range src {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst = append(dst, c)
	}
	return dst
}

// likeMatch matches lowered text against a lowered pattern. A % retries from
// the position after its last attempt, so the match is linear per %.
func likeMatch(p, s []byte) bool {
	pi, si := 0, 0
	star, mark := -1, 0
	for si < len(s) {
		if pi < len(p) && p[pi] == '%' {
			star, mark = pi, si
			pi++
			continue
		}
		if pi < len(p) && p[pi] == '_' {
			_, n := utf8.DecodeRune(s[si:])
			pi, si = pi+1, si+n
			continue
		}
		if pi < len(p) && p[pi] == s[si] {
			pi, si = pi+1, si+1
			continue
		}
		if star < 0 {
			return false
		}
		_, n := utf8.DecodeRune(s[mark:])
		mark += n
		pi, si = star+1, mark
	}
	for pi < len(p) && p[pi] == '%' {
		pi++
	}
	return pi == len(p)
}
