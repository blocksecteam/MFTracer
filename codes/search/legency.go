package search

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	cmap "github.com/orcaman/concurrent-map/v2"
	"golang.org/x/sync/errgroup"
)

type addressCounter struct {
	sync.Mutex
	counter uint32
}

func (ac *addressCounter) resetCounter() {
	ac.counter = 0
}

/*
	func (ac *addressCounter) getCounter() int {
		ac.Lock()
		ret := ac.counter
		ac.counter++
		ac.Unlock()
		return ret
	}
*/

func (ac *addressCounter) requestCounter() uint32 {
	ac.Lock()
	return ac.counter
}

func (ac *addressCounter) commitCounter(commit bool) {
	if commit {
		ac.counter++
	}
	ac.Unlock()
}

func MergeSubgraphsPrallel_Legency(subgraphs []*model.Subgraph, parallel int) (*model.Subgraph, error) {
	if len(subgraphs) == 0 {
		return nil, fmt.Errorf("MergeSubgraphsPrallel: para @subgraphs is nil")
	}
	ret := &model.Subgraph{
		BlockID: subgraphs[0].BlockID,
		Token:   model.EmptyAddress,
	}
	cMaps := cmap.New[cmap.ConcurrentMap[string, [2]uint32]]()
	aMap := cmap.New[string]()
	var ac addressCounter
	ac.resetCounter()

	var timestampLock sync.Mutex
	iterSubgraph := func(subgraph *model.Subgraph) {
		rMap := model.ReverseAddressMap(subgraph.AddressMap)
		for src, i := range subgraph.AddressMap {
			/*
				srcID, ok := aMap.Get(src)
				if !ok {
					srcID = strconv.Itoa(ac.getCounter())
					aMap.Set(src, srcID)
					srcMap = cmap.New[uint32]()
					cMaps.Set(srcID, srcMap)
				} else if srcMap, ok = cMaps.Get(srcID); !ok {
					srcMap = cmap.New[uint32]()
					cMaps.Set(srcID, srcMap)
				}
			*/
			ac.commitCounter(aMap.SetIfAbsent(src, strconv.Itoa(int(ac.requestCounter()))))
			srcID, _ := aMap.Get(src)
			cMaps.SetIfAbsent(srcID, cmap.New[[2]uint32]())
			srcMap, _ := cMaps.Get(srcID)
			for j := subgraph.NodePtrs[i]; j < subgraph.NodePtrs[i+1]; j++ {
				des := rMap[int(subgraph.Columns[j])]
				ac.commitCounter(aMap.SetIfAbsent(des, strconv.Itoa(int(ac.requestCounter()))))
				desID, _ := aMap.Get(des)
				/*
					desID, ok := aMap.Get(des)
					if !ok {
						desID = strconv.Itoa(ac.getCounter())
						aMap.Set(des, desID)
					}
				*/
				timestampLock.Lock()
				if timestamp, ok := srcMap.Get(desID); !ok {
					srcMap.Set(desID, subgraph.Timestamps[j])
				} else if subgraph.Timestamps[j][0] < timestamp[0] {
					srcMap.Set(desID, [2]uint32{subgraph.Timestamps[j][0], timestamp[1]})
				} else if subgraph.Timestamps[j][1] > timestamp[1] {
					srcMap.Set(desID, [2]uint32{timestamp[0], subgraph.Timestamps[j][1]})
				}
				timestampLock.Unlock()
			}
		}
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	start := time.Now()
	for _, subgraph := range subgraphs {
		s := subgraph
		eg.Go(func() error {
			ss := s
			iterSubgraph(ss)
			return nil
		})
	}
	eg.Wait()
	fmt.Println("[BENCHMARK] merge core time", time.Since(start).Seconds())

	start = time.Now()
	cMapsItems := cMaps.Items()
	fmt.Println("[BENCHMARK] get cmaps item time", time.Since(start).Seconds())
	start = time.Now()
	itemCount := 0
	//cMapsSorted := make([]map[string]uint32, len(cMapsItems))
	cMapsSorted := make([]cmap.ConcurrentMap[string, [2]uint32], len(cMapsItems))
	for k, v := range cMapsItems {
		nodeID, _ := strconv.Atoi(k)
		//cMapsSorted[nodeID] = v.Items()
		cMapsSorted[nodeID] = v
		itemCount += v.Count()
	}
	fmt.Println("[BENCHMARK] get cmap item time", time.Since(start).Seconds())

	timestamps := make([][2]uint32, 0, itemCount)
	columns := make([]uint32, 0, itemCount)
	nodePtrs := make([]uint32, len(cMapsItems)+1)
	nodePtrs[0] = 0
	start = time.Now()
	for i, cMap := range cMapsSorted {
		/*
			tempC := make([]uint32, 0, len(cMap))
			tempT := make([]uint32, 0, len(cMap))
			for k, v := range cMap {
				column, _ := strconv.Atoi(k)
				tempC = append(tempC, uint32(column))
				tempT = append(tempT, v)
			}
			sort.SliceStable(tempT, func(i, j int) bool {
				return tempC[i] < tempC[j]
			})
			sort.SliceStable(tempC, func(i, j int) bool {
				return tempC[i] < tempC[j]
			})
			columns = append(columns, tempC...)
			timestamps = append(timestamps, tempT...)
		*/
		type tempComp struct {
			column    uint32
			timestamp [2]uint32
		}
		cmapLength := cMap.Count()
		temp := make([]tempComp, 0, cmapLength)
		iter := cMap.IterBuffered()
		for item := range iter {
			column, _ := strconv.Atoi(item.Key)
			temp = append(temp, tempComp{
				column:    uint32(column),
				timestamp: item.Val,
			})
		}
		/*
			temp := make([]tempComp, 0, len(cMap))
			for k, v := range cMap {
				column, _ := strconv.Atoi(k)
				temp = append(temp, tempComp{
					column:    uint32(column),
					timestamp: v,
				})
			}
		*/
		sort.Slice(temp, func(i int, j int) bool {
			return temp[i].column < temp[j].column
		})
		for _, v := range temp {
			columns = append(columns, v.column)
			timestamps = append(timestamps, v.timestamp)
		}
		nodePtrs[i+1] = nodePtrs[i] + uint32(cmapLength)
	}
	fmt.Println("[BENCHMARK] merge copy time", time.Since(start).Seconds())

	aMapItems := aMap.Items()
	addressMap := make(map[string]uint32, len(aMapItems))
	for k, v := range aMapItems {
		nodeID, _ := strconv.Atoi(v)
		addressMap[k] = uint32(nodeID)
	}

	ret.Timestamps = timestamps
	ret.Columns = columns
	ret.NodePtrs = nodePtrs
	ret.AddressMap = addressMap
	return ret, nil
}

func MergeSubgraphsPrallel_Bench(subgraphs []*model.Subgraph, parallel int) (*model.Subgraph, error) {
	if len(subgraphs) == 0 {
		return nil, fmt.Errorf("MergeSubgraphsPrallel: para @subgraphs is nil")
	}
	ret := &model.Subgraph{
		BlockID: subgraphs[0].BlockID,
		Token:   model.EmptyAddress,
	}
	cMaps := cmap.NewWithCustomShardingFunction[uint32, map[uint32][2]uint32](func(key uint32) uint32 { return key })
	aMap := cmap.New[uint32]()
	var ac addressCounter
	ac.resetCounter()

	var columnsMapLock sync.Mutex
	iterSubgraph := func(subgraph *model.Subgraph) {
		rMap := model.ReverseAddressMap(subgraph.AddressMap)
		for src, i := range subgraph.AddressMap {
			ac.commitCounter(aMap.SetIfAbsent(src, ac.requestCounter()))
			srcID, _ := aMap.Get(src)
			cMaps.SetIfAbsent(srcID, make(map[uint32][2]uint32))
			srcMap, _ := cMaps.Get(srcID)
			for j := subgraph.NodePtrs[i]; j < subgraph.NodePtrs[i+1]; j++ {
				des := rMap[int(subgraph.Columns[j])]
				ac.commitCounter(aMap.SetIfAbsent(des, ac.requestCounter()))
				desID, _ := aMap.Get(des)
				columnsMapLock.Lock()
				if timestamp, ok := srcMap[desID]; !ok {
					srcMap[desID] = subgraph.Timestamps[j]
				} else if subgraph.Timestamps[j][0] < timestamp[0] {
					srcMap[desID] = [2]uint32{subgraph.Timestamps[j][0], timestamp[1]}
				} else if subgraph.Timestamps[j][1] > timestamp[1] {
					srcMap[desID] = [2]uint32{timestamp[0], subgraph.Timestamps[j][1]}
				}
				columnsMapLock.Unlock()
			}
		}
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	start := time.Now()
	for _, subgraph := range subgraphs {
		s := subgraph
		eg.Go(func() error {
			ss := s
			iterSubgraph(ss)
			return nil
		})
	}
	eg.Wait()
	fmt.Println("[BENCHMARK] merge core time\t", time.Since(start).Seconds())

	start = time.Now()
	cMapsIter := cMaps.IterBuffered()
	itemCount := 0
	cMapsLength := cMaps.Count()
	cMapsSorted := make([]map[uint32][2]uint32, cMapsLength)
	for item := range cMapsIter {
		cMapsSorted[item.Key] = item.Val
		itemCount += len(item.Val)
	}
	fmt.Println("[BENCHMARK] get cmap item time\t", time.Since(start).Seconds())

	start = time.Now()
	timestamps := make([][2]uint32, 0, itemCount)
	columns := make([]uint32, 0, itemCount)
	nodePtrs := make([]uint32, cMapsLength+1)
	nodePtrs[0] = 0
	for i, cMap := range cMapsSorted {
		type tempComp struct {
			column    uint32
			timestamp [2]uint32
		}
		temp := make([]tempComp, 0, len(cMap))
		for k, v := range cMap {
			temp = append(temp, tempComp{
				column:    k,
				timestamp: v,
			})
		}
		sort.Slice(temp, func(i int, j int) bool {
			return temp[i].column < temp[j].column
		})
		for _, v := range temp {
			columns = append(columns, v.column)
			timestamps = append(timestamps, v.timestamp)
		}
		nodePtrs[i+1] = nodePtrs[i] + uint32(len(cMap))
	}
	fmt.Println("[BENCHMARK] merge copy time\t", time.Since(start).Seconds())

	start = time.Now()
	addressMap := aMap.Items()
	fmt.Println("[BENCHMARK] get amap item time\t", time.Since(start).Seconds())

	ret.Timestamps = timestamps
	ret.Columns = columns
	ret.NodePtrs = nodePtrs
	ret.AddressMap = addressMap
	return ret, nil
}

func MergeSubgraphsPrallel(subgraphs []*model.Subgraph, parallel int) (*model.Subgraph, error) {
	if len(subgraphs) == 0 {
		return nil, fmt.Errorf("MergeSubgraphsPrallel: para @subgraphs is nil")
	}
	ret := &model.Subgraph{
		BlockID: subgraphs[0].BlockID,
		Token:   model.EmptyAddress,
	}
	cMaps := cmap.NewWithCustomShardingFunction[uint32, map[uint32][2]uint32](func(key uint32) uint32 { return key })
	aMap := cmap.New[uint32]()
	var ac addressCounter
	ac.resetCounter()

	var columnsMapLock sync.Mutex
	iterSubgraph := func(subgraph *model.Subgraph) {
		rMap := model.ReverseAddressMap(subgraph.AddressMap)
		for src, i := range subgraph.AddressMap {
			ac.commitCounter(aMap.SetIfAbsent(src, ac.requestCounter()))
			srcID, _ := aMap.Get(src)
			cMaps.SetIfAbsent(srcID, make(map[uint32][2]uint32))
			srcMap, _ := cMaps.Get(srcID)
			for j := subgraph.NodePtrs[i]; j < subgraph.NodePtrs[i+1]; j++ {
				des := rMap[int(subgraph.Columns[j])]
				ac.commitCounter(aMap.SetIfAbsent(des, ac.requestCounter()))
				desID, _ := aMap.Get(des)
				columnsMapLock.Lock()
				if timestamp, ok := srcMap[desID]; !ok {
					srcMap[desID] = subgraph.Timestamps[j]
				} else if subgraph.Timestamps[j][0] < timestamp[0] {
					srcMap[desID] = [2]uint32{subgraph.Timestamps[j][0], timestamp[1]}
				} else if subgraph.Timestamps[j][1] > timestamp[1] {
					srcMap[desID] = [2]uint32{timestamp[0], subgraph.Timestamps[j][1]}
				}
				columnsMapLock.Unlock()
			}
		}
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for _, subgraph := range subgraphs {
		s := subgraph
		eg.Go(func() error {
			ss := s
			iterSubgraph(ss)
			return nil
		})
	}
	eg.Wait()

	cMapsIter := cMaps.IterBuffered()
	itemCount := 0
	cMapsLength := cMaps.Count()
	cMapsSorted := make([]map[uint32][2]uint32, cMapsLength)
	for item := range cMapsIter {
		cMapsSorted[item.Key] = item.Val
		itemCount += len(item.Val)
	}

	timestamps := make([][2]uint32, 0, itemCount)
	columns := make([]uint32, 0, itemCount)
	nodePtrs := make([]uint32, cMapsLength+1)
	nodePtrs[0] = 0
	for i, cMap := range cMapsSorted {
		type tempComp struct {
			column    uint32
			timestamp [2]uint32
		}
		temp := make([]tempComp, 0, len(cMap))
		for k, v := range cMap {
			temp = append(temp, tempComp{
				column:    k,
				timestamp: v,
			})
		}
		sort.Slice(temp, func(i int, j int) bool {
			return temp[i].column < temp[j].column
		})
		for _, v := range temp {
			columns = append(columns, v.column)
			timestamps = append(timestamps, v.timestamp)
		}
		nodePtrs[i+1] = nodePtrs[i] + uint32(len(cMap))
	}

	ret.Timestamps = timestamps
	ret.Columns = columns
	ret.NodePtrs = nodePtrs
	ret.AddressMap = aMap.Items()
	return ret, nil
}

func FloodMainGraph(mg MainGraph, closure HopResult, floodStep int) MainGraph {
	newNodes := make(map[uint32]struct{}, len(mg))
	for node := range mg {
		newNodes[node] = struct{}{}
	}
	for i := 0; i < floodStep; i++ {
		tempSet := make(map[uint32]struct{})
		for thisNode := range newNodes {
			for preNode, edgeTimestamp := range closure[thisNode].pres {
				if isVirtualEdge(edgeTimestamp) {
					continue
				}
				if _, ok := mg[preNode]; !ok {
					mg[preNode] = make(map[uint32]struct{}, 1)
					tempSet[preNode] = struct{}{}
				}
				mg[preNode][thisNode] = struct{}{}
			}
		}
		newNodes = tempSet
	}
	return mg
}

// [TOTHINK] what if des appears twice in a path?
func mainGraphOfSubgraph_Legency(hops []HopResult, closure HopResult, desIDs []uint32, floodStep int) (MainGraph, map[uint32]struct{}) {
	mg := make(MainGraph)
	infMaxTimeMark := make(map[uint32]uint32)
	firstHop := make(map[uint32]struct{})
	for _, desID := range desIDs {
		if _, ok := closure[desID]; !ok {
			continue
		}
		for i := range hops {
			if _, ok := hops[i][desID]; !ok {
				continue
			}
			j := i
			bfsQueue := make([][2]uint32, 1) //[2]uint32{nodeID, infMaxTimestamp}
			bfsQueue[0] = [2]uint32{desID, math.MaxUint32}
			bfsQueueScratch := make(map[uint32]uint32)
			for j > 0 {
				thisNode := bfsQueue[0][0]
				thisInfMaxTimestamp := bfsQueue[0][1]
				if otherPathIMT, ok := infMaxTimeMark[thisNode]; !ok || ok && otherPathIMT < thisInfMaxTimestamp {
					infMaxTimeMark[thisNode] = thisInfMaxTimestamp
				}
				for preNode, v := range hops[j][thisNode].pres {
					if v[0] > thisInfMaxTimestamp {
						continue
					}
					if _, ok := mg[preNode]; !ok {
						mg[preNode] = make(map[uint32]struct{})
					}
					mg[preNode][thisNode] = struct{}{}
					var preInfMaxTimestamp uint32
					if v[1] < thisInfMaxTimestamp {
						preInfMaxTimestamp = v[1]
					} else {
						preInfMaxTimestamp = thisInfMaxTimestamp
					}
					if otherPathIMT, ok := bfsQueueScratch[preNode]; !ok || ok && (otherPathIMT < preInfMaxTimestamp) {
						bfsQueueScratch[preNode] = preInfMaxTimestamp
					}
				}
				bfsQueue = bfsQueue[1:]
				if len(bfsQueue) == 0 {
					bfsQueue = make([][2]uint32, 0, len(bfsQueueScratch))
					for k, v := range bfsQueueScratch {
						bfsQueue = append(bfsQueue, [2]uint32{k, v})
					}
					bfsQueueScratch = make(map[uint32]uint32)
					j--
				}
			}
			for i := range bfsQueue {
				firstHop[bfsQueue[i][0]] = struct{}{}
				if otherPathIMT, ok := infMaxTimeMark[bfsQueue[i][0]]; !ok || ok && otherPathIMT < bfsQueue[i][1] {
					infMaxTimeMark[bfsQueue[i][0]] = bfsQueue[i][1]
				}
			}
		}
	}
	mg, mgDeltaNode := floodMainGraphPrune_Legency(mg, infMaxTimeMark, closure, floodStep)
	for node := range mgDeltaNode {
		if _, ok := hops[0][node]; ok {
			firstHop[node] = struct{}{}
		}
	}
	//mg, _ = floodMainGraphPrune(mg, infMaxTimeMark, closure, floodStep)
	return mg, firstHop
}

func floodMainGraphPrune_Legency(mg MainGraph, infMaxTimestamp map[uint32]uint32, closure HopResult, floodStep int) (MainGraph, map[uint32]struct{}) {
	newNodes := make(map[uint32]struct{}, len(mg))
	mainMark := make(map[uint32]uint8, len(mg))
	mgDeltaNode := make(map[uint32]struct{}, len(mg))
	for src, desMap := range mg {
		newNodes[src] = struct{}{}
		mainMark[src] = 0
		for des := range desMap {
			newNodes[des] = struct{}{}
			mainMark[des] = 0
		}
	}
	var doMarkDFS func(this, supMinTimestamp uint32, pathLength uint8)
	//[TODO] fix prune problem for doMarkDFS()
	doMarkDFS = func(this, supMinTimestamp uint32, pathLength uint8) {
		//if otherPathLength, ok := mainMark[this]; pathLength > model.MaxHopLimit || ok && otherPathLength <= pathLength {
		if _, ok := mainMark[this]; ok {
			return
		}
		mainMark[this] = pathLength
		for next := range mg[this] {
			var nextSMT uint32 = 0
			/*
				edgeTimestamp := closure[next].pres[this]
				if edgeTimestamp[1] < supMinTimestamp {
					continue
				}
				if edgeTimestamp[0] > supMinTimestamp {
					nextSMT = edgeTimestamp[0]
				} else {
					nextSMT = supMinTimestamp
				}
			*/
			doMarkDFS(next, nextSMT, pathLength+1)
		}
	}
	for i := 0; i < floodStep; i++ {
		tempSet := make(map[uint32]struct{})
		IMTSratch := make(map[uint32]uint32)
		for thisNode := range newNodes {
			for preNode, edgeTimestamp := range closure[thisNode].pres {
				thisInfMaxTimestamp := infMaxTimestamp[thisNode]
				if isVirtualEdge(edgeTimestamp) || edgeTimestamp[0] > thisInfMaxTimestamp {
					continue
				}
				if _, ok := mg[preNode]; !ok {
					mg[preNode] = make(map[uint32]struct{}, 1)
					tempSet[preNode] = struct{}{}
				}
				mg[preNode][thisNode] = struct{}{}
				mgDeltaNode[preNode] = struct{}{}
				if _, ok := mainMark[preNode]; ok {
					doMarkDFS(thisNode, closure[thisNode].supMinTimestamp, closure[thisNode].hopLength)
				}
				var preInfMaxTimestamp uint32
				if edgeTimestamp[1] < thisInfMaxTimestamp {
					preInfMaxTimestamp = edgeTimestamp[1]
				} else {
					preInfMaxTimestamp = thisInfMaxTimestamp
				}
				if otherPathIMT, ok := infMaxTimestamp[preNode]; !ok || ok && otherPathIMT < preInfMaxTimestamp {
					if otherPathIMTScratch, ok := IMTSratch[preNode]; !ok || ok && otherPathIMTScratch < preInfMaxTimestamp {
						IMTSratch[preNode] = preInfMaxTimestamp
					}
					tempSet[preNode] = struct{}{}
				}
			}
		}
		for node, imt := range IMTSratch {
			infMaxTimestamp[node] = imt
		}
		newNodes = tempSet
	}
	for node := range mg {
		if _, ok := mainMark[node]; !ok {
			delete(mg, node)
			delete(mgDeltaNode, node)
		} else {
			for oNode := range mg[node] {
				if _, ok := mainMark[oNode]; !ok {
					delete(mg[node], oNode)
				}
			}
		}
	}
	return mg, mgDeltaNode
}

func GetMainGraph_Legency(subgraphs []*model.Subgraph, srcAddress, desAddress common.Address, rMaps [][]string, parallel int) []MainGraph {
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	//start := time.Now()
	sRetYXZ, _, sRetX := getCompleteSearchResult(subgraphs, srcAddress, rMaps, nil, nil, parallel)
	//fmt.Println(time.Since(start))
	for i, r := range sRetX {
		fmt.Printf("search len(ret[%d]) = %d\n", i, len(r))
	}
	sRetXZ := serializeHops(sRetYXZ)
	for i := range sRetXZ {
		fmt.Println(len(sRetXZ[i]))
	}
	desIDs := make([]map[uint32]struct{}, len(subgraphs))
	for i := len(subgraphs) - 1; i >= 0; i-- {
		desIDs[i] = make(map[uint32]struct{})
		desID, ok := subgraphs[i].AddressMap[string(desAddress.Bytes())]
		if ok {
			desIDs[i][desID] = struct{}{}
		}
	}
	mgs := make([]MainGraph, len(subgraphs))
	for i := len(subgraphs) - 1; i >= 0; i-- {
		if len(desIDs[i]) == 0 {
			continue
		}
		desIDsSlice := make([]uint32, 0, len(desIDs[i]))
		for desID := range desIDs[i] {
			desIDsSlice = append(desIDsSlice, desID)
		}
		mg, firstHop := mainGraphOfSubgraph_Legency(sRetXZ[i], sRetX[i], desIDsSlice, 10)
		mgs[i] = mg
		for k := range firstHop {
			kAddress := rMaps[i][k]
			for j := i - 1; j >= 0; j-- {
				if id, ok := subgraphs[j].AddressMap[kAddress]; ok {
					desIDs[j][id] = struct{}{}
				}
			}
		}
	}
	return mgs
}

func (mg MultiGraph) FindPathBFS(src, des string, maxDepth int, minValue float64, count int, timeLimit time.Duration) []MEdges {
	timeS := time.Now()
	if maxDepth <= 0 {
		return nil
	}
	if _, ok := mg[src]; !ok {
		return nil
	}
	//visitied := make(map[string]struct{})
	var ret []MEdges
	type pathCache struct {
		path MEdges
		addr string
	}
	bfsQueue := make([]pathCache, 0, 1)
	bfsQueue = append(bfsQueue, pathCache{
		path: nil,
		addr: src,
	})
depthIter:
	for depth := 0; depth < maxDepth && len(bfsQueue) > 0 && len(ret) < count; depth++ {
		nextQueue := make([]pathCache, 0, len(bfsQueue)*2)
		for _, cache := range bfsQueue {
			if cache.addr == des {
				ret = append(ret, cache.path)
				continue
			}
			for next, edges := range mg[cache.addr] {
				edges.SortByValue()
				for _, edge := range edges {
					if time.Since(timeS) > timeLimit {
						break depthIter
					}
					if edge.Value() < minValue || len(cache.path) > 0 && !edge.After(cache.path[len(cache.path)-1]) {
						continue
					}
					nextQueue = append(nextQueue, pathCache{
						path: append(cache.path, edge),
						addr: next,
					})
				}
			}
		}
		bfsQueue = nextQueue
	}
	return ret
}
