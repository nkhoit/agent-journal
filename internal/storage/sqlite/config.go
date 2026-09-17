package sqlite

// Version is the highest migration included in this scaffold.
const Version = 1

// ConnectionPragmas documents the invariants every future driver-backed
// connection must apply. Keeping these in one package avoids per-caller drift.
func ConnectionPragmas() []string {
	return []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA busy_timeout = 5000",
	}
}
