package search

import (
	"fmt"
	"sync"
	"transfer-graph/model"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type SearchResult struct {
	found   bool
	srcID   uint32
	desID   uint32
	closure HopResult
}

func searchInSubgraph(subgraph *model.Subgraph, firstHop HopResult, desAddress string, visitedMap HopResult) ([]HopResult, HopResult, bool, uint32) {
	found := false
	ret := make([]HopResult, 1)
	ret[0] = firstHop
	closure := make(HopResult)
	desID, ok := subgraph.AddressMap[desAddress]
	if !ok {
		desID = uint32(len(subgraph.AddressMap))
	}
	for i := 0; len(ret[i]) != 0; i++ {
		//for i := 0; len(ret[i]) != 0 && i <= 20; i++ {
		_, found = ret[i][desID]
		if found {
			closure = appendHopResult(closure, ret[i])
			return ret, closure, found, desID
		}
		next := getNextHop(ret[i], subgraph, closure, visitedMap, nil, nil)
		ret = append(ret, next)
		closure = appendHopResult(closure, ret[i])
		/*
			_, found = next[desID]
			if found {
				closure = appendHopResult(closure, next)
				return ret, closure, found, desID
			}
		*/
	}
	return ret[:len(ret)-1], closure, found, desID
}

func closureInSubgraph(subgraph *model.Subgraph, firstHop HopResult, visitedMap HopResult, allowedMap, forbiddenMap map[uint32]struct{}) ([]HopResult, HopResult) {
	ret := make([]HopResult, 1)
	ret[0] = firstHop
	closure := make(HopResult)
	for i := 0; len(ret[i]) != 0; i++ {
		ret = append(ret, getNextHop(ret[i], subgraph, closure, visitedMap, allowedMap, forbiddenMap))
		closure = appendHopResult(closure, ret[i])
	}
	return ret[:len(ret)-1], closure
}

func searchInSubgraphsParallel(subgraphs []*model.Subgraph, visitedMaps []HopResult, firstAddresses []string, firstHopLengths []uint8, srcAddress, desAddress string, parallel int) []*SearchResult {
	resultSlice := struct {
		sync.Mutex
		results []*SearchResult
	}{
		results: make([]*SearchResult, len(subgraphs)),
	}
	search := func(i int) {
		firstHop, srcID := NormalizeAddressesToHopResult(firstAddresses, firstHopLengths, subgraphs[i], srcAddress, visitedMaps[i], true)
		fmt.Printf("[Debug] {searchInSubgraphsParallel} start\n")
		//_, closure, found, desID := searchInSubgraph(subgraphs[i], firstHop, desAddress, visitedMaps[i])
		_, closure, found, desID := searchInSubgraph(subgraphs[i], firstHop, desAddress, nil)
		fmt.Printf("[Debug] {searchInSubgraphsParallel} end\n")
		/*
			if i == 1 {
				rMap := ReverseAddressMap(subgraphs[i].AddressMap)
				_, ok := firstHop[2403801]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %s\n", common.Bytes2Hex([]byte(rMap[2403801])))
				ok = false
				for _, addr := range firstAddresses {
					if strings.Compare(common.BytesToAddress([]byte(addr)).Hex(), "0x13ECcc013d7f99711e5fC540AAB16f6F109A4B7C") == 0 {
						ok = true
						break
					}
				}
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				_, closure_t := ClosureInSubgraphFromSrc(subgraphs[i], common.BytesToAddress([]byte(rMap[2403801])))
				_, ok = closure_t[2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				hops, closure_test, found_test, _ := searchInSubgraph(subgraphs[i], convertAddressToHopResult(common.BytesToAddress([]byte(rMap[2403801])), subgraphs[i]), desAddress, visitedMaps[i])
				_, ok = closure_test[2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				hops_tt, closure_tt, _, _ := searchInSubgraph(subgraphs[i], convertAddressToHopResult(common.BytesToAddress([]byte(rMap[2403801])), subgraphs[i]), desAddress, nil)
				_, ok = closure_tt[2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				_, closure_src, found_src, _ := searchInSubgraphMaxHop(subgraphs[i], convertAddressToHopResult(common.HexToAddress("0xFdffB4Fd1FD55d40Cb27EDFae02f752fCd50Fd56"), subgraphs[i]), desAddress, nil, 21)
				_, ok = closure_src[2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t %t\n", ok, found_src)
				if false && ok {
					endHop := 0
					for ; endHop < len(hops_tt); endHop++ {
						if _, ok := hops_tt[endHop][2999771]; ok {
							break
						}
					}
					path := make([]uint32, 0, endHop+1)
					pathTimestamp := make([]uint32, 0, endHop+1)
					thisNode := uint32(2999771)
					path = append(path, thisNode)
					for i := endHop; i > 0; i-- {
						pathTimestamp = append(pathTimestamp, hops_tt[i][thisNode].supMinTimestamp)
						for preNode := range hops_tt[i][thisNode].pres {
							fmt.Printf("%d ", preNode)
						}
						fmt.Printf("\n")
						for preNode := range hops_tt[i][thisNode].pres {
							path = append(path, preNode)
							thisNode = preNode
							break
						}
					}
					fmt.Printf("[Debug] {searchInSubgraphsParallel} len(hops) %d, found_test %t\n", len(hops), found_test)
					stopNode := path[len(path)-2]
					_, ok := hops[0][path[len(path)-1]]
					fmt.Printf("[Debug] {searchInSubgraphsParallel} hops[0][path[0]] %t %d\n", ok, hops[0][path[len(path)-1]].supMinTimestamp)
					_, ok = hops[1][path[len(path)-2]]
					fmt.Printf("[Debug] {searchInSubgraphsParallel} hops[1][path[1]] %t %d\n", ok, hops[1][path[len(path)-2]].supMinTimestamp)
					for _, node := range path {
						fmt.Printf("[Debug] {searchInSubgraphsParallel} path %d\n", node)
					}
					for _, timestamp := range pathTimestamp {
						fmt.Printf("[Debug] {searchInSubgraphsParallel} pathTimestamp %d\n", timestamp)
					}
					fmt.Printf("[Debug] {searchInSubgraphsParallel} stopNode %d\n", stopNode)
					sPtr := subgraphs[i].NodePtrs[2403801]
					ePtr := subgraphs[i].NodePtrs[2403802]
					row := subgraphs[i].Columns[sPtr:ePtr]
					timestamps := (subgraphs[i]).Timestamps[sPtr:ePtr]
					for i, node := range row {
						if node == 661 {
							fmt.Println(timestamps[i][0], timestamps[i][1])
							break
						}
					}
				}
				_, ok = closure[2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t, %t\n", ok, found)
				t, ok := visitedMaps[i][2403801]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
				if ok {
					fmt.Printf("[Debug] {searchInSubgraphsParallel} %d\n", t.supMinTimestamp)
				}
				_, ok = visitedMaps[i][2999771]
				fmt.Printf("[Debug] {searchInSubgraphsParallel} %t\n", ok)
			}
		*/

		resultSlice.Lock()
		(resultSlice.results)[i] = &SearchResult{
			found:   found,
			srcID:   srcID,
			desID:   desID,
			closure: closure,
		}
		resultSlice.Unlock()
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	fmt.Printf("[Debug] {searchInSubgraphsParallel} search start\n")
	for i := range subgraphs {
		s := i
		eg.Go(func() error {
			search(s)
			return nil
		})
	}
	eg.Wait()
	fmt.Printf("[Debug] {searchInSubgraphsParallel} search done\n")
	return resultSlice.results
}

func closureInSubgraphsParallel(subgraphs []*model.Subgraph, visitedMaps []HopResult, firstAddresses []string, firstHopLengths []uint8, allowedMap, forbiddenMap map[string]struct{}, parallel int) []HopResult {
	resultSlice := struct {
		sync.Mutex
		results []HopResult
	}{
		results: make([]HopResult, len(subgraphs)),
	}
	search := func(i int) {
		aMap := make(map[uint32]struct{})
		fMap := make(map[uint32]struct{})
		firstHop, _ := NormalizeAddressesToHopResult(firstAddresses, firstHopLengths, subgraphs[i], string(""), visitedMaps[i], false)
		for addr := range allowedMap {
			if id, ok := subgraphs[i].AddressMap[addr]; ok {
				aMap[id] = struct{}{}
			}
		}
		for addr := range forbiddenMap {
			if id, ok := subgraphs[i].AddressMap[addr]; ok {
				fMap[id] = struct{}{}
			}
		}
		_, closure := closureInSubgraph(subgraphs[i], firstHop, visitedMaps[i], aMap, fMap)
		resultSlice.Lock()
		//fmt.Printf("~%d\t\t", len(firstHop))
		(resultSlice.results)[i] = closure
		resultSlice.Unlock()
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
	//fmt.Printf("\n")
	return resultSlice.results
}

type SubPath struct {
	SubgraphIdx int
	Path        []string
}

func traceBackSingle(stepResults [][]*SearchResult, subgraphs []*model.Subgraph, rMaps [][]string, ret []*SubPath) (bool, []*SubPath) {
	step := len(stepResults) - 1
	found := false
	if ret == nil {
		ret = make([]*SubPath, 0)
	}
	for i := step; i < len(subgraphs); i++ {
		searchResult := stepResults[step][i]
		if !searchResult.found {
			continue
		}
		found = true
		path := ConstructPathsAsString(searchResult.srcID, searchResult.desID, searchResult.closure, 1, subgraphs[i], rMaps[i])[0]
		ret = append(ret, &SubPath{
			SubgraphIdx: i,
			Path:        path,
		})
		fmt.Printf("path[len(path)-2] %s\n", common.BytesToAddress([]byte(path[len(path)-2])).Hex())
		for thisStep := step; thisStep > 0; {
			fmt.Println("this step", thisStep)
			preStep := thisStep - 1
			for i := 0; i < thisStep; i++ {
				thisDesID := subgraphs[preStep].AddressMap[path[len(path)-2]]
				if _, ok := (stepResults[i][preStep].closure)[thisDesID]; ok {
					path = ConstructPathsAsString(stepResults[i][preStep].srcID, thisDesID, stepResults[i][preStep].closure, 1, subgraphs[preStep], rMaps[preStep])[0]
					ret = append(ret, &SubPath{
						SubgraphIdx: preStep,
						Path:        path,
					})
					thisStep = i
					break
				}
			}
		}
		break
	}
	return found, ret
}

func FindOnePath(subgraphs []*model.Subgraph, srcAddress, desAddress common.Address, parallel int) (bool, []*SubPath) {
	ret := make([]*SubPath, 0, 1)
	found := false
	stepResults := make([][]*SearchResult, 1)
	conClosures := make([]HopResult, len(subgraphs))
	src := string(srcAddress.Bytes())
	des := string(desAddress.Bytes())
	for i := range conClosures {
		conClosures[i] = make(HopResult)
	}
	fmt.Printf("[Debug] {FindOnePath} search step 0 start\n")
	stepResults[0] = searchInSubgraphsParallel(subgraphs, conClosures, []string{src}, []uint8{0}, src, des, parallel)
	fmt.Printf("[Debug] {FindOnePath} search step 0 done\n")
	for i, searchResult := range stepResults[0] {
		if searchResult.found {
			fmt.Printf("[Debug] {FindOnePath} found in subgraph[%d], srcID: %d, desID: %d\n", i, searchResult.srcID, searchResult.desID)
			ret = append(ret, &SubPath{
				SubgraphIdx: i,
				Path:        ConstructPathsAsString(searchResult.srcID, searchResult.desID, searchResult.closure, 1, subgraphs[i], nil)[0],
			})
			return true, ret
		}
	}
	rMaps := model.ReverseAddressMaps(nil, subgraphs)
	for i := 0; i < len(subgraphs)-1 && !found; i++ {
		fmt.Printf("[Debug] {FindOnePath} iter start %d\n", i)
		for j := i; j < len(subgraphs); j++ {
			conClosures[j] = appendHopResult(conClosures[j], stepResults[i][j].closure)
		}
		firstAddresses, _, firstHopLengths := NormalizeHopResultToAddresses(conClosures[i], subgraphs[i], rMaps[i])
		fmt.Printf("[Debug] {FindOnePath} normalize done %d\n", i)
		stepRetRPart := searchInSubgraphsParallel(subgraphs[i+1:], conClosures[i+1:], firstAddresses, firstHopLengths, src, des, parallel)
		fmt.Printf("[Debug] {FindOnePath} search done %d\n", i)
		stepResults = append(stepResults, append(make([]*SearchResult, i+1), stepRetRPart...))
		// @@ [Debug]
		/*
			if i == 0 {
				//_, ok := (stepResults[1][2].closure)[1144902]
				//fmt.Printf("[Debug] {FindOnePath} %t\n", ok)
				_, ok := conClosures[0][324339]
				fmt.Printf("[Debug] {FindOnePath} %t\n", ok)
				fmt.Printf("[Debug] {FindOnePath} %s\n", common.BytesToAddress([]byte(rMaps[0][324339])).Hex())
				ok = false
				for _, addr := range firstAddresses {
					if strings.Compare(common.BytesToAddress([]byte(addr)).Hex(), "0x5a19917ac702dA1bf771EdfF7C0C22221c3116fc") == 0 {
						ok = true
						break
					}
				}
				fmt.Printf("[Debug] {FindOnePath} %t\n", ok)
			}
		*/
		found, ret = traceBackSingle(stepResults, subgraphs, rMaps, ret)
		fmt.Printf("[Debug] {FindOnePath} trace done %d, %t\n", i, found)
	}
	return found, ret
}

func GetClosures(subgraphs []*model.Subgraph, srcAddresses []common.Address, allowed, forbidden []common.Address, parallel int) []HopResult {
	stepClosures := make([][]HopResult, 0, 1)
	conClosures := make([]HopResult, len(subgraphs))
	src := make([]string, len(srcAddresses))
	for i, srcAddress := range srcAddresses {
		src[i] = string(srcAddress.Bytes())
	}
	for i := range conClosures {
		conClosures[i] = make(HopResult)
	}
	rMaps := model.ReverseAddressMaps(nil, subgraphs)
	aMap := make(map[string]struct{}, len(allowed))
	fMap := make(map[string]struct{}, len(forbidden))
	for _, addr := range allowed {
		aMap[utils.AddrToAddrString(addr)] = struct{}{}
	}
	for _, addr := range forbidden {
		fMap[utils.AddrToAddrString(addr)] = struct{}{}
	}
	/*
		stepClosures[0] = closureInSubgraphsParallel(subgraphs, conClosures, []string{src}, src, parallel)
		for i := 0; i < len(subgraphs)-1; i++ {
			for j := i; j < len(subgraphs); j++ {
				conClosures[j] = appendHopResult(conClosures[j], stepClosures[i][j])
			}
			firstAddresses, _ := NormalizeHopResultToAddresses(conClosures[i], subgraphs[i], rMaps[i])
			stepClosureRPart := closureInSubgraphsParallel(subgraphs[i+1:], conClosures[i+1:], firstAddresses, src, parallel)
			stepClosures = append(stepClosures, append(make([]HopResult, i+1), stepClosureRPart...))
		}
	*/
	for i := 0; i < len(subgraphs); i++ {
		var firstAddresses []string
		var firstHopLengths []uint8
		if i == 0 {
			firstAddresses = src
			firstHopLengths = make([]uint8, len(src))
			for i := range firstHopLengths {
				firstHopLengths[i] = 0
			}
		} else {
			firstAddresses, _, firstHopLengths = NormalizeHopResultToAddresses(conClosures[i-1], subgraphs[i-1], rMaps[i-1])
		}
		stepClosureRPart := closureInSubgraphsParallel(subgraphs[i:], conClosures[i:], firstAddresses, firstHopLengths, aMap, fMap, parallel)
		stepClosures = append(stepClosures, append(make([]HopResult, i), stepClosureRPart...))
		for j := i; j < len(subgraphs); j++ {
			conClosures[j] = appendHopResult(conClosures[j], stepClosures[i][j])
		}
	}
	return conClosures
}

func hopsInSubgraphsParallel(subgraphs []*model.Subgraph, visitedMaps []HopResult, firstAddresses []string, firstHopLengths []uint8, srcAddress string, allowedMap, forbiddenMap map[string]struct{}, parallel int) ([][]HopResult, []HopResult) {
	resultSync := struct {
		sync.Mutex
		hopsSlice    [][]HopResult
		closureSlice []HopResult
	}{
		hopsSlice:    make([][]HopResult, len(subgraphs)),
		closureSlice: make([]HopResult, len(subgraphs)),
	}
	search := func(i int) {
		aMap := make(map[uint32]struct{})
		fMap := make(map[uint32]struct{})
		firstHop, _ := NormalizeAddressesToHopResult(firstAddresses, firstHopLengths, subgraphs[i], srcAddress, visitedMaps[i], true)
		for addr := range allowedMap {
			if id, ok := subgraphs[i].AddressMap[addr]; ok {
				aMap[id] = struct{}{}
			}
		}
		for addr := range forbiddenMap {
			if id, ok := subgraphs[i].AddressMap[addr]; ok {
				fMap[id] = struct{}{}
			}
		}
		hops, closure := closureInSubgraph(subgraphs[i], firstHop, visitedMaps[i], aMap, fMap)
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

func getCompleteSearchResult(subgraphs []*model.Subgraph, srcAddress common.Address, rMaps [][]string, allowed, forbidden []common.Address, parallel int) ([][][]HopResult, [][]HopResult, []HopResult) {
	hops := make([][][]HopResult, 0, 1)
	stepClosures := make([][]HopResult, 0, 1)
	conClosures := make([]HopResult, len(subgraphs))
	for i := range conClosures {
		conClosures[i] = make(HopResult)
	}
	src := string(srcAddress.Bytes())
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	aMap := make(map[string]struct{}, len(allowed))
	fMap := make(map[string]struct{}, len(forbidden))
	for _, addr := range allowed {
		aMap[utils.AddrToAddrString(addr)] = struct{}{}
	}
	for _, addr := range forbidden {
		fMap[utils.AddrToAddrString(addr)] = struct{}{}
	}
	for i := 0; i < len(subgraphs); i++ {
		var firstAddresses []string
		var firstHopLengths []uint8
		if i == 0 {
			firstAddresses = []string{src}
			firstHopLengths = []uint8{0}
		} else {
			firstAddresses, _, firstHopLengths = NormalizeHopResultToAddresses(conClosures[i-1], subgraphs[i-1], rMaps[i-1])
		}
		hopsRPart, stepClosureRPart := hopsInSubgraphsParallel(subgraphs[i:], conClosures[i:], firstAddresses, firstHopLengths, src, aMap, fMap, parallel)
		hops = append(hops, append(make([][]HopResult, i), hopsRPart...))
		stepClosures = append(stepClosures, append(make([]HopResult, i), stepClosureRPart...))
		for j := i; j < len(subgraphs); j++ {
			conClosures[j] = appendHopResult(conClosures[j], stepClosures[i][j])
		}
	}
	return hops, stepClosures, conClosures /*y x z, y x, x*/
}

func serializeHops(hops [][][]HopResult /*y x z*/) [][]HopResult /*x z*/ {
	ret := make([][]HopResult, len(hops))
	for x := 0; x < len(hops); x++ {
		ret[x] = make([]HopResult, 0)
		for z := 0; ; z++ {
			temp := make([]HopResult, 0, x+1)
			for y := 0; y <= x; y++ {
				if z >= len(hops[y][x]) {
					continue
				}
				temp = append(temp, hops[y][x][z])
			}
			if len(temp) > 0 {
				ret[x] = append(ret[x], mergeHopResults(temp))
			} else {
				break
			}
		}
	}
	return ret
}
