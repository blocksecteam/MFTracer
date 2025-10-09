package model

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type Subgraph struct {
	BlockID    uint16            `json:"blockID"`
	Token      common.Address    `json:"token"`
	Timestamps [][2]uint32       `json:"timestamps"` //[2]uint32{minTimestamp, maxTimestamp}
	Columns    []uint32          `json:"columns"`
	NodePtrs   []uint32          `json:"nodePtrs"`
	AddressMap map[string]uint32 `json:"addressMap"`
}

func (subgraph *Subgraph) IsLinked(src, des uint32) bool {
	rowS := subgraph.NodePtrs[src]
	rowE := subgraph.NodePtrs[src+1]
	column := subgraph.Columns[rowS:rowE]
	i := sort.Search(len(column), func(i int) bool {
		return column[i] >= des
	})
	if i < len(column) && column[i] == des {
		return true
	} else {
		return false
	}
}

func (subgraph *Subgraph) AddressIsLinked(src, des common.Address) bool {
	var srcID, desID uint32
	if id, ok := subgraph.AddressMap[string(src.Bytes())]; ok {
		srcID = id
	} else {
		return false
	}
	if id, ok := subgraph.AddressMap[string(des.Bytes())]; ok {
		desID = id
	} else {
		return false
	}
	return subgraph.IsLinked(srcID, desID)
}

func (subgraph *Subgraph) ODegree(address common.Address) int {
	if id, ok := subgraph.AddressMap[string(address.Bytes())]; ok {
		return int(subgraph.NodePtrs[id+1] - subgraph.NodePtrs[id])
	} else {
		return 0
	}
}

func ReverseAddressMap(addressMap map[string]uint32) []string {
	ret := make([]string, len(addressMap))
	for k, v := range addressMap {
		ret[v] = k
	}
	return ret
}

func ReverseAddressMapsParallel(addressMaps []map[string]uint32, subgraphs []*Subgraph, parallel int) [][]string {
	retSlice := struct {
		sync.Mutex
		rMaps [][]string
	}{}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	if addressMaps != nil {
		retSlice.rMaps = make([][]string, len(addressMaps))
		reverse := func(i int) {
			rMap := ReverseAddressMap(addressMaps[i])
			retSlice.Lock()
			(retSlice.rMaps)[i] = rMap
			retSlice.Unlock()
		}
		for i := range addressMaps {
			s := i
			eg.Go(func() error {
				reverse(s)
				return nil
			})
		}
	} else {
		retSlice.rMaps = make([][]string, len(subgraphs))
		reverse := func(i int) {
			rMap := ReverseAddressMap(subgraphs[i].AddressMap)
			retSlice.Lock()
			(retSlice.rMaps)[i] = rMap
			retSlice.Unlock()
		}
		for i := range subgraphs {
			s := i
			eg.Go(func() error {
				reverse(s)
				return nil
			})
		}
	}
	eg.Wait()
	return retSlice.rMaps
}

func ReverseAddressMaps(addressMaps []map[string]uint32, subgraphs []*Subgraph) [][]string {
	var ret [][]string
	if addressMaps != nil {
		ret = make([][]string, len(addressMaps))
		for i := range addressMaps {
			ret[i] = ReverseAddressMap(addressMaps[i])
		}
	} else {
		ret = make([][]string, len(subgraphs))
		for i := range subgraphs {
			ret[i] = ReverseAddressMap(subgraphs[i].AddressMap)
		}
	}
	return ret
}

func MergeSubgraphs(subgraphs []*Subgraph) (*Subgraph, error) {
	if len(subgraphs) == 0 {
		return nil, fmt.Errorf("MergeSubgraphsPrallel: para @subgraphs is nil")
	}
	ret := &Subgraph{
		BlockID: subgraphs[0].BlockID,
		Token:   CompositeAddress,
	}
	cMaps := make(map[uint32]map[uint32][2]uint32)
	aMap := make(map[string]uint32)
	ac := uint32(0)

	start := time.Now()
	for _, subgraph := range subgraphs {
		//start := time.Now()
		//fmt.Printf("[Debug] {MergeSubgraphs} core iter start\n")
		rMap := ReverseAddressMap(subgraph.AddressMap)
		for src, i := range subgraph.AddressMap {
			var ok bool
			var srcID uint32
			var srcMap map[uint32][2]uint32
			if srcID, ok = aMap[src]; !ok {
				srcID = ac
				aMap[src] = ac
				ac++
			}
			if srcMap, ok = cMaps[srcID]; !ok {
				srcMap = make(map[uint32][2]uint32)
				cMaps[srcID] = srcMap
			}
			for j := subgraph.NodePtrs[i]; j < subgraph.NodePtrs[i+1]; j++ {
				var ok bool
				var desID uint32
				des := rMap[int(subgraph.Columns[j])]
				if desID, ok = aMap[des]; !ok {
					desID = ac
					aMap[des] = ac
					ac++
				}
				if timestamp, ok := srcMap[desID]; !ok {
					srcMap[desID] = subgraph.Timestamps[j]
				} else if subgraph.Timestamps[j][0] < timestamp[0] {
					srcMap[desID] = [2]uint32{subgraph.Timestamps[j][0], timestamp[1]}
				} else if subgraph.Timestamps[j][1] > timestamp[1] {
					srcMap[desID] = [2]uint32{timestamp[0], subgraph.Timestamps[j][1]}
				}
			}
		}
		//fmt.Printf("[Debug] {MergeSubgraphs} core iter finished: %f\n", time.Since(start).Seconds())
	}
	fmt.Printf("[Debug] {MergeSubgraphs} merge core finished: %f\n", time.Since(start).Seconds())

	itemCount := 0
	cMapsSorted := make([]map[uint32][2]uint32, len(cMaps))
	for k, v := range cMaps {
		cMapsSorted[k] = v
		itemCount += len(v)
	}
	//fmt.Printf("[Debug] {MergeSubgraphs} cmaps sort finished: %f\n", time.Since(start).Seconds())

	start = time.Now()
	timestamps := make([][2]uint32, 0, itemCount)
	columns := make([]uint32, 0, itemCount)
	nodePtrs := make([]uint32, len(cMaps)+1)
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
	fmt.Printf("[Debug] {MergeSubgraphs} cmaps copy finished: %f\n", time.Since(start).Seconds())

	ret.Timestamps = timestamps
	ret.Columns = columns
	ret.NodePtrs = nodePtrs
	ret.AddressMap = aMap
	return ret, nil
}

func MergeSubgraphsBatch(subgraphss [][]*Subgraph, parallel int) ([]*Subgraph, error) {
	retSlice := struct {
		sync.Mutex
		ret []*Subgraph
	}{
		ret: make([]*Subgraph, len(subgraphss)),
	}
	mergeFunc := func(i int) error {
		ret, err := MergeSubgraphs(subgraphss[i])
		if err != nil {
			return err
		}
		retSlice.Lock()
		(retSlice.ret)[i] = ret
		retSlice.Unlock()
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for i := range subgraphss {
		s := i
		eg.Go(func() error {
			return mergeFunc(s)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return retSlice.ret, nil
}

func ReverseSubgraph(subgraph *Subgraph) *Subgraph {
	ret := &Subgraph{
		BlockID:    subgraph.BlockID,
		Token:      subgraph.Token,
		Timestamps: make([][2]uint32, len(subgraph.Timestamps)),
		Columns:    make([]uint32, len(subgraph.Columns)),
		NodePtrs:   make([]uint32, len(subgraph.NodePtrs)),
		AddressMap: make(map[string]uint32, len(subgraph.AddressMap)),
	}
	edges := make(map[uint32][][3]uint32) //map[des] = {src, minTimestamp, maxTimestamp}
	for src := uint32(0); src < uint32(len(subgraph.AddressMap)); src++ {
		for i := subgraph.NodePtrs[src]; i < subgraph.NodePtrs[src+1]; i++ {
			des := subgraph.Columns[i]
			minTimestamp, maxTimestamp := subgraph.Timestamps[i][0], subgraph.Timestamps[i][1]
			if _, ok := edges[des]; !ok {
				edges[des] = make([][3]uint32, 0, 1)
			}
			edges[des] = append(edges[des], [3]uint32{src, minTimestamp, maxTimestamp})
		}
	}
	ret.NodePtrs[0] = 0
	for src := uint32(0); src < uint32(len(subgraph.AddressMap)); src++ {
		if es, ok := edges[src]; ok {
			sort.Slice(es, func(i, j int) bool {
				return es[i][0] < es[j][0]
			})
			ret.NodePtrs[src+1] = ret.NodePtrs[src] + uint32(len(es))
			for i, e := range es {
				ret.Columns[i+int(ret.NodePtrs[src])] = e[0]
				ret.Timestamps[i+int(ret.NodePtrs[src])] = [2]uint32{e[1], e[2]}
			}
		} else {
			ret.NodePtrs[src+1] = ret.NodePtrs[src]
		}
	}
	for k, v := range subgraph.AddressMap {
		ret.AddressMap[k] = v
	}
	return ret
}
