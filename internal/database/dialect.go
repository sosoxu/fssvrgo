package database

import (
	"fmt"
	"strings"
)

type Dialect int

const (
	DialectSQLite Dialect = iota
	DialectPostgreSQL
)

func DialectFromString(s string) Dialect {
	switch strings.ToLower(s) {
	case "postgresql", "postgres", "pg":
		return DialectPostgreSQL
	default:
		return DialectSQLite
	}
}

func (d Dialect) String() string {
	switch d {
	case DialectPostgreSQL:
		return "postgresql"
	default:
		return "sqlite"
	}
}

func (d Dialect) Placeholder(n int) string {
	switch d {
	case DialectPostgreSQL:
		return fmt.Sprintf("$%d", n)
	default:
		return "?"
	}
}

func (d Dialect) Translate(query string) string {
	if d == DialectSQLite {
		return query
	}

	var result strings.Builder
	paramIndex := 1
	i := 0
	for i < len(query) {
		switch c := query[i]; c {
		case '?':
			// A placeholder. '?' characters inside string literals,
			// identifiers, dollar-quoted strings and comments are copied
			// verbatim — they are data, not placeholders.
			result.WriteString(fmt.Sprintf("$%d", paramIndex))
			paramIndex++
			i++
		case '\'', '"':
			i = copyQuoted(&result, query, i, c)
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = copyLineComment(&result, query, i)
				break
			}
			result.WriteByte(c)
			i++
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = copyBlockComment(&result, query, i)
				break
			}
			result.WriteByte(c)
			i++
		case '$':
			if end, ok := dollarQuoteEnd(query, i); ok {
				result.WriteString(query[i:end])
				i = end
				break
			}
			result.WriteByte(c)
			i++
		default:
			result.WriteByte(c)
			i++
		}
	}
	return result.String()
}

// copyQuoted copies a single-quoted string literal or double-quoted identifier
// starting at start, honouring the doubled-character escape ('' or ""), and
// returns the index just past the closing quote.
func copyQuoted(result *strings.Builder, query string, start int, quote byte) int {
	result.WriteByte(quote)
	i := start + 1
	for i < len(query) {
		result.WriteByte(query[i])
		if query[i] == quote {
			if i+1 < len(query) && query[i+1] == quote {
				result.WriteByte(query[i+1])
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func copyLineComment(result *strings.Builder, query string, start int) int {
	i := start
	for i < len(query) && query[i] != '\n' {
		result.WriteByte(query[i])
		i++
	}
	return i
}

func copyBlockComment(result *strings.Builder, query string, start int) int {
	i := start + 2
	result.WriteString("/*")
	for i < len(query) {
		if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
			result.WriteString("*/")
			return i + 2
		}
		result.WriteByte(query[i])
		i++
	}
	return i
}

// dollarQuoteEnd reports whether a dollar-quoted string starts at start
// ($tag$ ... $tag$) and, if so, returns the index just past its closing tag.
func dollarQuoteEnd(query string, start int) (int, bool) {
	tagEnd := start + 1
	for tagEnd < len(query) && (query[tagEnd] == '_' ||
		(query[tagEnd] >= 'a' && query[tagEnd] <= 'z') ||
		(query[tagEnd] >= 'A' && query[tagEnd] <= 'Z') ||
		(tagEnd > start+1 && query[tagEnd] >= '0' && query[tagEnd] <= '9')) {
		tagEnd++
	}
	if tagEnd >= len(query) || query[tagEnd] != '$' {
		return 0, false
	}
	tag := query[start : tagEnd+1] // e.g. "$$" or "$tag$"
	closeAt := strings.Index(query[tagEnd+1:], tag)
	if closeAt < 0 {
		return 0, false
	}
	return tagEnd + 1 + closeAt + len(tag), true
}

func (d Dialect) BooleanValue(v bool) interface{} {
	if d == DialectPostgreSQL {
		return v
	}
	if v {
		return 1
	}
	return 0
}

func (d Dialect) BooleanCheck(v bool) string {
	if d == DialectPostgreSQL {
		if v {
			return "TRUE"
		}
		return "FALSE"
	}
	if v {
		return "1"
	}
	return "0"
}

func (d Dialect) AutoIncrementType() string {
	if d == DialectPostgreSQL {
		return "SERIAL"
	}
	return "INTEGER"
}

func (d Dialect) BooleanType() string {
	if d == DialectPostgreSQL {
		return "BOOLEAN"
	}
	return "BOOLEAN"
}

func (d Dialect) TextType() string {
	if d == DialectPostgreSQL {
		return "TEXT"
	}
	return "TEXT"
}

func (d Dialect) TimestampType() string {
	if d == DialectPostgreSQL {
		return "TIMESTAMP WITH TIME ZONE"
	}
	return "TIMESTAMP"
}

func (d Dialect) CurrentTimestamp() string {
	if d == DialectPostgreSQL {
		return "NOW()"
	}
	return "CURRENT_TIMESTAMP"
}

func (d Dialect) CreateTableIfNotExistsSuffix() string {
	return ""
}
