package search

import (
	"math"
	"sync"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type NodeResultBack struct {
	pres               map[uint32][2]uint32
	timestampConstrain uint32
	hopLength          uint8
}

type HopResultBack map[uint32]*NodeResultBack

func (hr HopResult) ToBack() HopResultBack {
	ret := make(HopResultBack, len(hr))
	for node, v := range hr {
		ret[node] = &NodeResultBack{
			pres:               make(map[uint32][2]uint32, len(v.pres)),
			timestampConstrain: v.supMinTimestamp,
			hopLength:          v.hopLength,
		}
		for pre, t := range v.pres {
			ret[node].pres[pre] = t
		}
	}
	return ret
}

func (hrb HopResultBack) ToReverse() HopResultBack {
	ret := make(HopResultBack)
	for node, v := range hrb {
		for pre, t := range v.pres {
			if _, ok := ret[pre]; !ok {
				ret[pre] = &NodeResultBack{
					pres: make(map[uint32][2]uint32),
				}
			}
			ret[pre].pres[node] = t
		}
	}
	return ret
}

func appendHopResultBack(des, src HopResultBack, isBackward bool) HopResultBack {
	for node, v := range src {
		if _, ok := des[node]; !ok {
			des[node] = &NodeResultBack{
				pres:               make(map[uint32][2]uint32, len(v.pres)),
				timestampConstrain: v.timestampConstrain,
				hopLength:          v.hopLength,
			}
		}
		for pre, t := range v.pres {
			(des[node].pres)[pre] = t
		}
		if !isBackward && v.timestampConstrain < des[node].timestampConstrain || isBackward && v.timestampConstrain > des[node].timestampConstrain {
			des[node].timestampConstrain = v.timestampConstrain
		}
		if v.hopLength < des[node].hopLength {
			des[node].hopLength = v.hopLength
		}
	}
	return des
}

func appendHopResultsBackParallel(ds, ss []HopResultBack, parallel int, isBackward bool) []HopResultBack {
	retSlice := struct {
		sync.Mutex
		ret []HopResultBack
	}{
		ret: make([]HopResultBack, len(ds)),
	}
	appendFunc := func(i int) {
		ret := appendHopResultBack(ds[i], ss[i], isBackward)
		retSlice.Lock()
		(retSlice.ret)[i] = ret
		retSlice.Unlock()
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for i := range ds {
		s := i
		eg.Go(func() error {
			appendFunc(s)
			return nil
		})
	}
	eg.Wait()
	return retSlice.ret
}

func getNextHopBack(thisHop HopResultBack, clsgraph HopResultBack, preHops HopResultBack, alterVisitedMap HopResultBack, isBackward bool) HopResultBack {
	ret := make(HopResultBack)
	for src, v := range thisHop {
		if t, ok := preHops[src]; ok && t.hopLength <= v.hopLength && (!isBackward && t.timestampConstrain <= v.timestampConstrain || isBackward && t.timestampConstrain >= v.timestampConstrain) {
			continue
		}
		if t, ok := alterVisitedMap[src]; ok && t.hopLength <= v.hopLength && (!isBackward && t.timestampConstrain <= v.timestampConstrain || isBackward && t.timestampConstrain >= v.timestampConstrain) {
			continue
		}
		if v.hopLength >= model.MaxHopLimit {
			continue
		}
		var srcInfo *NodeResultBack
		var ok bool
		//if srcInfo, ok = clsgraph[src]; !ok || ok && len(srcInfo.pres) > model.SuperNodeOutDegreeLimitLevel5 {
		//if srcInfo, ok = clsgraph[src]; !ok || ok && len(srcInfo.pres) > 2000 {
		if srcInfo, ok = clsgraph[src]; !ok {
			continue
		}
		for des, edgeTimestamp := range srcInfo.pres {
			if !isBackward && edgeTimestamp[1] < v.timestampConstrain || isBackward && edgeTimestamp[0] > v.timestampConstrain {
				continue
			}
			if _, ok := ret[des]; !ok {
				ret[des] = &NodeResultBack{
					pres:      make(map[uint32][2]uint32),
					hopLength: v.hopLength + 1,
				}
				if !isBackward {
					if edgeTimestamp[0] > v.timestampConstrain {
						ret[des].timestampConstrain = edgeTimestamp[0]
					} else {
						ret[des].timestampConstrain = v.timestampConstrain
					}
				} else {
					if edgeTimestamp[1] < v.timestampConstrain {
						ret[des].timestampConstrain = edgeTimestamp[1]
					} else {
						ret[des].timestampConstrain = v.timestampConstrain
					}
				}
			} else {
				if !isBackward {
					var thisPathSMT uint32
					if edgeTimestamp[0] > v.timestampConstrain {
						thisPathSMT = edgeTimestamp[0]
					} else {
						thisPathSMT = v.timestampConstrain
					}
					if thisPathSMT < ret[des].timestampConstrain {
						ret[des].timestampConstrain = thisPathSMT
					}
				} else {
					var thisPathIMT uint32
					if edgeTimestamp[1] < v.timestampConstrain {
						thisPathIMT = edgeTimestamp[1]
					} else {
						thisPathIMT = v.timestampConstrain
					}
					if thisPathIMT > ret[des].timestampConstrain {
						ret[des].timestampConstrain = thisPathIMT
					}
				}
				if v.hopLength+1 < ret[des].hopLength {
					ret[des].hopLength = v.hopLength + 1
				}
			}
			ret[des].pres[src] = edgeTimestamp
		}
	}
	return ret
}

func NormalizeHopResultBackToAddresses(hop HopResultBack, subgraph *model.Subgraph, rMap []string) ([]string, []uint32, []uint8) {
	if rMap == nil {
		rMap = model.ReverseAddressMap(subgraph.AddressMap)
	}
	retAddresses := make([]string, 0, len(hop))
	retTimestamps := make([]uint32, 0, len(hop))
	rethopLengths := make([]uint8, 0, len(hop))
	for nodeID, v := range hop {
		retAddresses = append(retAddresses, rMap[nodeID])
		retTimestamps = append(retTimestamps, v.timestampConstrain)
		rethopLengths = append(rethopLengths, v.hopLength)
	}
	return retAddresses, retTimestamps, rethopLengths
}

func NormalizeAddressesToHopResultBack(addresses []string, hopLengths []uint8, clsgraph HopResultBack, subgraph *model.Subgraph, visitedMap HopResultBack, isBackward, bySubgraph bool) HopResultBack {
	ret := make(HopResultBack, len(addresses)/2)
	for i := range addresses {
		if nodeID, ok := subgraph.AddressMap[addresses[i]]; ok {
			if _, ok := clsgraph[nodeID]; !bySubgraph && !ok {
				continue
			}
			if v, ok := visitedMap[nodeID]; ok && (!isBackward && v.timestampConstrain == 0 || isBackward && v.timestampConstrain == math.MaxUint32) {
				continue
			}
			var temp uint32
			if !isBackward {
				temp = 0
			} else {
				temp = math.MaxUint32
			}
			ret[nodeID] = &NodeResultBack{
				pres:               nil,
				timestampConstrain: temp,
				hopLength:          hopLengths[i],
			}
		}
	}
	return ret
}

func closureInClsgraph(clsgraph HopResultBack, subgraph *model.Subgraph, firstHop HopResultBack, visitedMap HopResultBack, isBackward, bySubgraph bool) ([]HopResultBack, HopResultBack) {
	ret := make([]HopResultBack, 1)
	ret[0] = firstHop
	closure := make(HopResultBack)
	for i := 0; len(ret[i]) != 0; i++ {
		if !bySubgraph {
			ret = append(ret, getNextHopBack(ret[i], clsgraph, closure, visitedMap, isBackward))
		} else {
			ret = append(ret, getNextHopBackBySubgraph(ret[i], subgraph, closure, isBackward, visitedMap, nil, nil))
		}
		closure = appendHopResultBack(closure, ret[i], isBackward)
	}
	return ret[:len(ret)-1], closure
}

func hopsInClsgraphsParallel(clsgraphs []HopResultBack, subgraphs []*model.Subgraph, visitedMaps []HopResultBack, firstAddresses []string, firstHopLengths []uint8, parallel int, isBackward, bySubgraph bool) ([][]HopResultBack, []HopResultBack) {
	resultSync := struct {
		sync.Mutex
		hopsSlice    [][]HopResultBack
		closureSlice []HopResultBack
	}{
		hopsSlice:    make([][]HopResultBack, len(clsgraphs)),
		closureSlice: make([]HopResultBack, len(clsgraphs)),
	}
	search := func(i int) {
		firstHop := NormalizeAddressesToHopResultBack(firstAddresses, firstHopLengths, clsgraphs[i], subgraphs[i], visitedMaps[i], isBackward, bySubgraph)
		hops, closure := closureInClsgraph(clsgraphs[i], subgraphs[i], firstHop, visitedMaps[i], isBackward, bySubgraph)
		resultSync.Lock()
		(resultSync.hopsSlice)[i] = hops
		(resultSync.closureSlice)[i] = closure
		resultSync.Unlock()
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for i := range subgraphs {
		s := i
		eg.Go(func() error {
			search(s)
			return nil
		})
	}
	eg.Wait()
	return resultSync.hopsSlice, resultSync.closureSlice
}

func getClosuresBack(clsgraphs []HopResultBack, subgraphs []*model.Subgraph, srcAddresses []common.Address, rMaps [][]string, parallel int, isBackward, bySubgraph bool) []HopResultBack {
	stepClosures := make([][]HopResultBack, 0, 1)
	conClosures := make([]HopResultBack, len(subgraphs))
	for i := range conClosures {
		conClosures[i] = make(HopResultBack)
	}
	src := make([]string, len(srcAddresses))
	for i, srcAddress := range srcAddresses {
		src[i] = string(srcAddress.Bytes())
	}
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	for i := 0; i < len(clsgraphs); i++ {
		var firstAddresses []string
		var firstHopLengths []uint8
		if i == 0 {
			firstAddresses = src
			firstHopLengths = make([]uint8, len(src))
			for i := range firstHopLengths {
				firstHopLengths[i] = 0
			}
		} else {
			firstAddresses, _, firstHopLengths = NormalizeHopResultBackToAddresses(conClosures[i-1], subgraphs[i-1], rMaps[i-1])
		}
		_, stepClosureRPart := hopsInClsgraphsParallel(clsgraphs[i:], subgraphs[i:], conClosures[i:], firstAddresses, firstHopLengths, parallel, isBackward, bySubgraph)
		stepClosures = append(stepClosures, append(make([]HopResultBack, i), stepClosureRPart...))
		for j := i; j < len(subgraphs); j++ {
			conClosures[j] = appendHopResultBack(conClosures[j], stepClosures[i][j], isBackward)
		}
	}
	return conClosures
}

func getNextHopBackBySubgraph(thisHop HopResultBack, subgraph *model.Subgraph, preHops HopResultBack, isBackward bool, alterVisitedMap HopResultBack, allowedMap, forbiddenMap map[uint32]struct{}) HopResultBack {
	ret := make(HopResultBack)
	for src, v := range thisHop {
		if t, ok := preHops[src]; ok && t.hopLength <= v.hopLength && (!isBackward && t.timestampConstrain <= v.timestampConstrain || isBackward && t.timestampConstrain >= v.timestampConstrain) {
			continue
		}
		if t, ok := alterVisitedMap[src]; ok && t.hopLength <= v.hopLength && (!isBackward && t.timestampConstrain <= v.timestampConstrain || isBackward && t.timestampConstrain >= v.timestampConstrain) {
			continue
		}
		if v.hopLength >= model.MaxHopLimit {
			continue
		}

		rowS := subgraph.NodePtrs[src]
		rowE := subgraph.NodePtrs[src+1]
		_, aok := allowedMap[src]
		_, fok := forbiddenMap[src]
		if !aok && rowE-rowS > model.SuperNodeOutDegreeLimitLevel5 || fok {
			continue
		}
		columns := subgraph.Columns[rowS:rowE]
		timestamps := subgraph.Timestamps[rowS:rowE]
		for i, des := range columns {
			if !isBackward && timestamps[i][1] < v.timestampConstrain || isBackward && timestamps[i][0] > v.timestampConstrain {
				continue
			}
			if _, ok := ret[des]; !ok {
				ret[des] = &NodeResultBack{
					pres:      make(map[uint32][2]uint32),
					hopLength: v.hopLength + 1,
				}
				if !isBackward {
					if timestamps[i][0] > v.timestampConstrain {
						ret[des].timestampConstrain = timestamps[i][0]
					} else {
						ret[des].timestampConstrain = v.timestampConstrain
					}
				} else {
					if timestamps[i][1] < v.timestampConstrain {
						ret[des].timestampConstrain = timestamps[i][1]
					} else {
						ret[des].timestampConstrain = v.timestampConstrain
					}
				}
			} else {
				if !isBackward {
					var thisPathSMT uint32
					if timestamps[i][0] > v.timestampConstrain {
						thisPathSMT = timestamps[i][0]
					} else {
						thisPathSMT = v.timestampConstrain
					}
					if thisPathSMT < ret[des].timestampConstrain {
						ret[des].timestampConstrain = thisPathSMT
					}
				} else {
					var thisPathIMT uint32
					if timestamps[i][1] < v.timestampConstrain {
						thisPathIMT = timestamps[i][1]
					} else {
						thisPathIMT = v.timestampConstrain
					}
					if thisPathIMT > ret[des].timestampConstrain {
						ret[des].timestampConstrain = thisPathIMT
					}
				}
				if v.hopLength+1 < ret[des].hopLength {
					ret[des].hopLength = v.hopLength + 1
				}
			}
			ret[des].pres[src] = timestamps[i]
		}
	}
	return ret
}
