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
		if query[i] == '?' {
			result.WriteString(fmt.Sprintf("$%d", paramIndex))
			paramIndex++
			i++
		} else {
			result.WriteByte(query[i])
			i++
		}
	}
	return result.String()
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
