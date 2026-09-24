package snapshot

// Stats describes the current state of a store.
type Stats struct {
	// Keys is the number of live keys (present values) in the latest view.
	Keys int
	// Snapshots is the cumulative number of snapshots successfully handed
	// out by this store across all openings of the same directory.
	Snapshots uint64
	// AvgBytes is the mean length of live values, rounded half-up to two
	// decimal places. It is 0 when there are no live keys.
	AvgBytes float64
}

// Stats returns statistics over the latest committed view. It does not count
// as taking a snapshot.
func (s *Store) Stats() Stats {
	v := s.current.Load()
	var keys int
	var totalBytes int
	v.rangeEach(func(_ string, n *node) bool {
		if n.present {
			keys++
			totalBytes += len(n.value)
		}
		return true
	})
	st := Stats{Keys: keys, Snapshots: s.snapshots.Load()}
	if keys > 0 {
		// Exact half-up rounding to hundredths using integer math, avoiding
		// any floating-point tie or negative-zero issues.
		scaled := totalBytes * 100
		hundredths, rem := scaled/keys, scaled%keys
		if 2*rem >= keys {
			hundredths++
		}
		st.AvgBytes = float64(hundredths) / 100
	}
	return st
}
