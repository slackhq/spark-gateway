package router

import "sort"

func GetOrderedKeys(weightsMap map[string]*metric) []string {
	keys := make([]string, 0, len(weightsMap))
	for k := range weightsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
