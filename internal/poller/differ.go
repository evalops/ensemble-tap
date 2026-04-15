package poller

import "github.com/evalops/siphon/internal/normalize"

func DiffSnapshots(prev, curr map[string]any) map[string]normalize.FieldChange {
	changes := make(map[string]normalize.FieldChange)
	seen := make(map[string]struct{}, len(prev)+len(curr))
	for k := range prev {
		seen[k] = struct{}{}
	}
	for k := range curr {
		seen[k] = struct{}{}
	}
	for key := range seen {
		pv, pok := prev[key]
		cv, cok := curr[key]
		if !pok && cok {
			changes[key] = normalize.FieldChange{From: nil, To: cv}
			continue
		}
		if pok && !cok {
			changes[key] = normalize.FieldChange{From: pv, To: nil}
			continue
		}
		if pv != cv {
			changes[key] = normalize.FieldChange{From: pv, To: cv}
		}
	}
	if len(changes) == 0 {
		return nil
	}
	return changes
}
