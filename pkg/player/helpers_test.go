package player

func indices(evs []Event) []int {
	out := make([]int, len(evs))
	for i, ev := range evs {
		out[i] = ev.Index
	}
	return out
}
