package database

import "testing"

// TestTranslatePlaceholders covers issue #116: '?' must only be rewritten when
// it is a placeholder, never when it is data (string literal, quoted
// identifier, dollar-quoted body) or a comment.
func TestTranslatePlaceholders(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "plain placeholders",
			input: "SELECT * FROM t WHERE a = ? AND b = ?",
			want:  "SELECT * FROM t WHERE a = $1 AND b = $2",
		},
		{
			name:  "question mark inside string literal",
			input: "SELECT * FROM t WHERE name = 'a?b' AND id = ?",
			want:  "SELECT * FROM t WHERE name = 'a?b' AND id = $1",
		},
		{
			name:  "escaped quote inside string literal",
			input: "SELECT 'it''s ? still data', ? FROM t",
			want:  "SELECT 'it''s ? still data', $1 FROM t",
		},
		{
			name:  "question mark inside quoted identifier",
			input: `SELECT "col?" FROM t WHERE x = ?`,
			want:  `SELECT "col?" FROM t WHERE x = $1`,
		},
		{
			name:  "line comment",
			input: "SELECT 1 -- is this ? a placeholder\n, ? FROM t",
			want:  "SELECT 1 -- is this ? a placeholder\n, $1 FROM t",
		},
		{
			name:  "block comment",
			input: "SELECT /* ? not a param */ ? FROM t",
			want:  "SELECT /* ? not a param */ $1 FROM t",
		},
		{
			name:  "dollar quoted string",
			input: "SELECT $$a?b$$, ? FROM t",
			want:  "SELECT $$a?b$$, $1 FROM t",
		},
		{
			name:  "tagged dollar quoted string",
			input: "SELECT $tag$a?b$tag$, ? FROM t",
			want:  "SELECT $tag$a?b$tag$, $1 FROM t",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DialectPostgreSQL.Translate(tc.input); got != tc.want {
				t.Errorf("Translate(%q)\n got: %q\nwant: %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestTranslateLeavesSQLiteUntouched(t *testing.T) {
	query := "SELECT * FROM t WHERE a = ? AND b = 'x?y'"
	if got := DialectSQLite.Translate(query); got != query {
		t.Errorf("SQLite translate must be a no-op, got %q", got)
	}
}

func TestTranslateUnterminatedQuoteIsCopiedVerbatim(t *testing.T) {
	// Malformed SQL must not panic or loop: everything after the opening quote
	// is copied as-is.
	query := "SELECT 'unterminated ?"
	if got := DialectPostgreSQL.Translate(query); got != query {
		t.Errorf("unterminated literal: got %q, want %q", got, query)
	}
}
