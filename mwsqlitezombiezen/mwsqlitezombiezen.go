package mwsqlitezombiezen

// return New("file:memory:?mode=memory&journal_mode=WAL&busy_timeout=3000&secure_delete=true&foreign_keys=true&cache=shared", poolSize)
// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643
