package search

import (
	"fmt"
	"math"
	"sync"
	"time"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type NodeResult struct {
	pres            map[uint32][2]uint32
	supMinTimestamp uint32
	hopLength       uint8
}

type HopResult map[uint32]*NodeResult

func (nr *NodeResult) HopLength() uint8 {
	return nr.hopLength
}

func mergeHopResults(hops []HopResult) HopResult {
	ret := make(HopResult)
	for _, hop := range hops {
		for node, v := range hop {
			if _, ok := ret[node]; !ok {
				ret[node] = &NodeResult{
					pres:            make(map[uint32][2]uint32, len(v.pres)),
					supMinTimestamp: v.supMinTimestamp,
					hopLength:       v.hopLength,
				}
			}
			for pre, t := range v.pres {
				(ret[node].pres)[pre] = t
			}
			if v.supMinTimestamp < ret[node].supMinTimestamp {
				ret[node].supMinTimestamp = v.supMinTimestamp
			}
			if v.hopLength < ret[node].hopLength {
				ret[node].hopLength = v.hopLength
			}
		}
	}
	return ret
}

func appendHopResult(des, src HopResult) HopResult {
	for node, v := range src {
		if _, ok := des[node]; !ok {
			des[node] = &NodeResult{
				pres:            make(map[uint32][2]uint32, len(v.pres)),
				supMinTimestamp: v.supMinTimestamp,
				hopLength:       v.hopLength,
			}
		}
		for pre, t := range v.pres {
			(des[node].pres)[pre] = t
		}
		if v.supMinTimestamp < des[node].supMinTimestamp {
			des[node].supMinTimestamp = v.supMinTimestamp
		}
		if v.hopLength < des[node].hopLength {
			des[node].hopLength = v.hopLength
		}
	}
	return des
}

func appendHopResultsParallel(ds, ss []HopResult, parallel int) []HopResult {
	retSlice := struct {
		sync.Mutex
		ret []HopResult
	}{
		ret: make([]HopResult, len(ds)),
	}
	appendFunc := func(i int) {
		ret := appendHopResult(ds[i], ss[i])
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

func getNextHop(thisHop HopResult, subgraph *model.Subgraph, preHops HopResult, alterVisitedMap HopResult, allowedMap, forbiddenMap map[uint32]struct{}) HopResult {
	ret := make(HopResult)
	for src, v := range thisHop {

		if t, ok := preHops[src]; ok && t.supMinTimestamp <= v.supMinTimestamp && t.hopLength <= v.hopLength {
			continue
		}
		if t, ok := alterVisitedMap[src]; ok && t.supMinTimestamp <= v.supMinTimestamp && t.hopLength <= v.hopLength {
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
			if timestamps[i][1] < v.supMinTimestamp {
				continue
			}
			/*
				if _, ok := ret[des]; !ok {
					ret[des] = &NodeResult{
						pres:            make(map[uint32][2]uint32),
						supMinTimestamp: timestamps[i][0],
					}
					(ret[des].pres)[src] = timestamps[i]
					continue
				}
				(ret[des].pres)[src] = timestamps[i]
				if timestamps[i][0] > ret[des].supMinTimestamp {
					ret[des].supMinTimestamp = timestamps[i][0]
				}
			*/
			if _, ok := ret[des]; !ok {
				ret[des] = &NodeResult{
					pres:      make(map[uint32][2]uint32),
					hopLength: v.hopLength + 1,
				}
				if timestamps[i][0] > v.supMinTimestamp {
					ret[des].supMinTimestamp = timestamps[i][0]
				} else {
					ret[des].supMinTimestamp = v.supMinTimestamp
				}
			} else {
				/*
					if timestamps[i][0] > v.supMinTimestamp && timestamps[i][0] < ret[des].supMinTimestamp {
						ret[des].supMinTimestamp = timestamps[i][0]
					//} else if v.supMinTimestamp > timestamps[i][0] && v.supMinTimestamp < ret[des].supMinTimestamp {
					} else if v.supMinTimestamp < ret[des].supMinTimestamp {
						ret[des].supMinTimestamp = v.supMinTimestamp
					}
				*/

				var temp uint32
				if timestamps[i][0] > v.supMinTimestamp {
					temp = timestamps[i][0]
				} else {
					temp = v.supMinTimestamp
				}
				if temp < ret[des].supMinTimestamp {
					ret[des].supMinTimestamp = temp
				}

				if v.hopLength+1 < ret[des].hopLength {
					ret[des].hopLength = v.hopLength + 1
				}
			}
			(ret[des].pres)[src] = timestamps[i]
		}
	}
	return ret
}

func ClosureInSubgraphFromSrc(subgraph *model.Subgraph, srcAddress common.Address) ([]HopResult, HopResult) {
	/*
		srcID, ok := subgraph.AddressMap[string(srcAddress.Bytes())]
		if !ok {
			return nil, nil
		}
		rowS := subgraph.NodePtrs[srcID]
		rowE := subgraph.NodePtrs[srcID+1]
		columns := subgraph.Columns[rowE:rowS]
		timestamps := subgraph.Timestamps[rowE:rowS]
		ret := make([]HopResult, 1)
		ret[0] = make(HopResult, rowE-rowS)
		for i := range columns {
			ret[0][columns[i]] = &NodeResult{
				pres:         make(map[uint32]uint32, 1),
				minTimestamp: timestamps[i],
			}
			(ret[0][columns[i]].pres)[srcID] = timestamps[i]
		}
	*/
	ret := make([]HopResult, 1)
	ret[0] = convertAddressToHopResult(srcAddress, subgraph)
	closure := make(HopResult)
	//start := time.Now()
	for i := 0; len(ret[i]) != 0; i++ {
		//for i := 0; len(ret[i]) != 0 && i <= 20; i++ {
		//start := time.Now()
		ret = append(ret, getNextHop(ret[i], subgraph, closure, nil, nil, nil))
		//get := time.Now()
		closure = appendHopResult(closure, ret[i])
		//fmt.Printf("[Debug] {ClosureInSubgraphFromMap} get hop[%d], nodeNum: %d, time %f, merge time %f\n", i+1, len(ret[i+1]), get.Sub(start).Seconds(), time.Since(get).Seconds())
	}
	//fmt.Printf("[Debug] {ClosureInSubgraphFromSrc} finish time %f\n", time.Since(start).Seconds())
	return ret[:len(ret)-1], closure
}

/*
func NormalizeHopResultForNewSubgraph(oldG, newG *model.Subgraph, closure HopResult, srcAddress common.Address, rMap []string) (HopResult, uint32) {
	if rMap == nil {
		rMap = ReverseAddressMap(oldG.AddressMap)
	}
	srcID, ok := newG.AddressMap[string(srcAddress.Bytes())]
	if !ok {
		srcID = uint32(len(newG.AddressMap))
	}
	ret := make(HopResult, len(closure)/2)
	for oldID := range closure {
		if newID, ok := newG.AddressMap[rMap[oldID]]; ok {
			ret[newID] = &NodeResult{
				pres:            make(map[uint32][2]uint32, 1),
				supMinTimestamp: 0,
			}
			(ret[newID].pres)[srcID] = [2]uint32{0, math.MaxUint32}
		}
	}
	return ret, srcID
}
*/

func NormalizeHopResultToAddresses(hop HopResult, subgraph *model.Subgraph, rMap []string) ([]string, []uint32, []uint8) {
	if rMap == nil {
		rMap = model.ReverseAddressMap(subgraph.AddressMap)
	}
	retAddresses := make([]string, 0, len(hop))
	retTimestamps := make([]uint32, 0, len(hop))
	rethopLengths := make([]uint8, 0, len(hop))
	for nodeID, v := range hop {
		retAddresses = append(retAddresses, rMap[nodeID])
		retTimestamps = append(retTimestamps, v.supMinTimestamp)
		rethopLengths = append(rethopLengths, v.hopLength)
	}
	return retAddresses, retTimestamps, rethopLengths
}

func convertSrcAddressToSrcIDs(srcAddress string, subgraphs []*model.Subgraph) []uint32 {
	srcIDs := make([]uint32, len(subgraphs))
	for i, subgraph := range subgraphs {
		srcID, ok := subgraph.AddressMap[srcAddress]
		if !ok {
			srcID = uint32(len(subgraph.AddressMap))
		}
		srcIDs[i] = srcID
	}
	return srcIDs
}

func NormalizeAddressesToHopResult(addresses []string, hopLengths []uint8, subgraph *model.Subgraph, srcAddress string, visitedMap HopResult, needVirtualEdge bool) (HopResult, uint32) {
	ret := make(HopResult, len(addresses)/2)
	srcID, ok := subgraph.AddressMap[srcAddress]
	if !ok {
		srcID = uint32(len(subgraph.AddressMap))
	}
	for i := range addresses {
		if nodeID, ok := subgraph.AddressMap[addresses[i]]; ok {
			// @@ [Lagency]
			// @ Not all nodes in visitedMap should be cut, because some
			// @ are not search-finished due to timestamp constrains.
			// @ Only those with supMinTimestamp == 0 should be cut.
			// @ 'Normalize' generates the 1st hop with no timestamp constrain.
			// @ Then all nodes in the 1st hop will be search-finished.
			/*
				if _, ok = visitedMap[nodeID]; !ok {
					ret[nodeID] = &NodeResult{
						pres:            make(map[uint32][2]uint32, 1),
						supMinTimestamp: 0,
					}
					(ret[nodeID].pres)[srcID] = [2]uint32{0, math.MaxUint32}
				}
			*/
			if v, ok := visitedMap[nodeID]; ok && v.supMinTimestamp == 0 {
				continue
			}
			if needVirtualEdge {
				ret[nodeID] = &NodeResult{
					pres:            make(map[uint32][2]uint32, 1),
					supMinTimestamp: 0,
					hopLength:       hopLengths[i],
				}
				(ret[nodeID].pres)[srcID] = [2]uint32{0, math.MaxUint32} //add a virtual edge from src
			} else {
				ret[nodeID] = &NodeResult{
					pres:            nil,
					supMinTimestamp: 0,
					hopLength:       hopLengths[i],
				}
			}
		}
	}
	return ret, srcID
}

func isVirtualEdge(edgeTimestamp [2]uint32) bool {
	return edgeTimestamp[0] == 0 && edgeTimestamp[1] == math.MaxUint32
}

func convertAddressToHopResult(address common.Address, subgraph *model.Subgraph) HopResult {
	ret := make(HopResult)
	srcID, ok := subgraph.AddressMap[string(address.Bytes())]
	if !ok {
		return ret
	}
	ret[srcID] = &NodeResult{
		pres:            nil,
		supMinTimestamp: 0,
		hopLength:       0,
	}
	return ret
}

func ClosureInSubgraphFromMap(subgraph *model.Subgraph, firstHop HopResult) ([]HopResult, HopResult) {
	ret := make([]HopResult, 1)
	ret[0] = firstHop
	closure := make(HopResult)
	for i := 0; len(ret[i]) != 0; i++ {
		//start := time.Now()
		ret = append(ret, getNextHop(ret[i], subgraph, closure, nil, nil, nil))
		//fmt.Printf("[Debug] {ClosureInSubgraphFromMap} get hop[%d] time %f\n", i, time.Since(start).Seconds())
		//start = time.Now()
		closure = appendHopResult(closure, ret[i])
		//fmt.Printf("[Debug] {ClosureInSubgraphFromMap} merge closure time %f\n", time.Since(start).Seconds())
	}
	return ret[:len(ret)-1], closure
}

func ConstructPaths(srcID, desID uint32, closure HopResult, amount int) [][]uint32 {
	visitMap := make(map[uint32]bool)
	ret := make([][]uint32, 0)
	currentPath := make([]uint32, 0)
	var dfs func(v, des uint32, infMaxTimestamp uint32)
	dfs = func(v, des uint32, infMaxTimestamp uint32) {
		if visited, ok := visitMap[v]; ok && visited || len(ret) == amount {
			return
		}
		visitMap[v] = true
		currentPath = append(currentPath, v)
		if v == des {
			path := make([]uint32, len(currentPath))
			copy(path, currentPath)
			ret = append(ret, path)
			visitMap[v] = false
			currentPath = currentPath[:len(currentPath)-1]
			return
		}
		for u, t := range closure[v].pres {
			/*
				if t[0] <= infMaxTimestamp {
					if t[1] < infMaxTimestamp {
						infMaxTimestamp = t[1]
					}
					dfs(u, des, infMaxTimestamp)
				}
			*/
			if t[0] <= infMaxTimestamp {
				nextInfMaxTimestamp := infMaxTimestamp
				if t[1] < nextInfMaxTimestamp {
					nextInfMaxTimestamp = t[1]
				}
				dfs(u, des, nextInfMaxTimestamp)
				if len(ret) == amount {
					return
				}
			}
		}
		visitMap[v] = false
		currentPath = currentPath[:len(currentPath)-1]
	}
	start := time.Now()
	dfs(desID, srcID, math.MaxUint32)
	fmt.Printf("[Debug] {ConstructPaths} dfs time %f\n", time.Since(start).Seconds())
	return ret
}

func ConstructPathsAsString(srcID, desID uint32, closure HopResult, amount int, subgraph *model.Subgraph, rMap []string) [][]string {
	if rMap == nil {
		rMap = model.ReverseAddressMap(subgraph.AddressMap)
	}
	//fmt.Printf("[Debug] {ConstructPathsAsString} src %s\n", common.BytesToAddress([]byte(rMap[srcID])).Hex())
	//fmt.Printf("[Debug] {ConstructPathsAsString} des %s\n", common.BytesToAddress([]byte(rMap[desID])).Hex())
	fmt.Printf("[Debug] {ConstructPathsAsString} len(closure) %d\n", len(closure))
	visitMap := make(map[uint32]bool)
	ret := make([][]string, 0)
	currentPath := make([]string, 0)
	var dfs func(v, des uint32, infMaxTimestamp uint32)
	dfs = func(v, des uint32, infMaxTimestamp uint32) {
		if len(ret) == amount /*|| len(currentPath) > 20*/ {
			return
		}
		if visited, ok := visitMap[v]; ok && visited {
			return
		}
		//fmt.Printf("[Debug] {ConstructAllPaths} dfs.v %d, depth %d\n", v, len(currentPath))
		visitMap[v] = true
		currentPath = append(currentPath, rMap[v])
		if v == des {
			path := make([]string, len(currentPath))
			copy(path, currentPath)
			ret = append(ret, path)
			visitMap[v] = false
			currentPath = currentPath[:len(currentPath)-1]
			return
		}
		for u, t := range closure[v].pres {
			if t[0] <= infMaxTimestamp {
				nextInfMaxTimestamp := infMaxTimestamp
				if t[1] < nextInfMaxTimestamp {
					nextInfMaxTimestamp = t[1]
				}
				dfs(u, des, nextInfMaxTimestamp)
				if len(ret) == amount {
					return
				}
			}
		}
		visitMap[v] = false
		currentPath = currentPath[:len(currentPath)-1]
	}
	start := time.Now()
	dfs(desID, srcID, math.MaxUint32)
	fmt.Printf("[Debug] {ConstructPathsAsString} dfs time %f, len(ret) %d\n", time.Since(start).Seconds(), len(ret))
	return ret
}
