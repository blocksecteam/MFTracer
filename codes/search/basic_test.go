package search_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"transfer-graph/graph"
	"transfer-graph/model"
	"transfer-graph/search"

	"github.com/ethereum/go-ethereum/common"
)

const DBPath = "/data/test-db-20"
const DataPath = "/home/icecy/transfer-graph-data/test-data"

func _TestMergeSubgraphsPrallel_Bench(t *testing.T) {
	start := time.Now()
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	subgraphs, err := g.BlockIDScanAllTokensToSubgraphs(context.Background(), uint16(16100000/model.BlockSpan), graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	t.Logf("ScanAllTokens finished, time: %f", time.Since(start).Seconds())
	/*
		targets := make([]*model.Subgraph, 0, 20)
		for _, subgraph := range subgraphs {
			if len(targets) < 5 && len(subgraph.NodePtrs) > 100000 || subgraph.Token.Cmp(model.EtherAddress) == 0 {
				targets = append(targets, subgraph)
			}
		}
		t.Logf("targets constructed: %d", len(targets))
	*/
	mainGraph, err := model.MergeSubgraphs(subgraphs)
	//mainGraph, err := search.MergeSubgraphsPrallel(targets, 8)
	if err != nil {
		t.Error(err)
	}
	t.Logf("MergeSubgraphs finished, time: %f", time.Since(start).Seconds())
	template, err := graph.ConstructSubgraphs_TestTool("16100000_16200000.json.zst", DataPath)
	if err != nil {
		t.Error(err)
	}
	boolMap := make(map[int]struct{}, len(mainGraph.AddressMap))
	for _, v := range mainGraph.AddressMap {
		boolMap[int(v)] = struct{}{}
	}
	for i := 0; i < len(boolMap); i++ {
		if _, ok := boolMap[i]; !ok {
			t.Errorf("addressMap length error, missing %d", i)
		}
	}

	mRMap := model.ReverseAddressMap(mainGraph.AddressMap)
	tRMap := model.ReverseAddressMap(template.AddressMap)
	t.Logf("construction finished, time: %f", time.Since(start).Seconds())
	for k, v := range mainGraph.AddressMap {
		rowS := int(mainGraph.NodePtrs[int(v)])
		rowE := int(mainGraph.NodePtrs[int(v)+1])
		columns := mainGraph.Columns[rowS:rowE]
		timestamps := mainGraph.Timestamps[rowS:rowE]
		v_t, ok := template.AddressMap[k]
		if !ok {
			t.Errorf("missing address -> addressID: %s", common.BytesToAddress([]byte(k)).Hex())
		}
		rowS_t := int(template.NodePtrs[int(v_t)])
		rowE_t := int(template.NodePtrs[int(v_t)+1])
		if rowE_t-rowS_t != rowE-rowS {
			t.Errorf("wrong row length: %s, [%d] [%d]", common.BytesToAddress([]byte(k)).Hex(), rowE_t-rowS_t, rowE-rowS)
		}
		columns_t := template.Columns[rowS_t:rowE_t]
		timestamps_t := template.Timestamps[rowS_t:rowE_t]

		type tempComp struct {
			column    uint32
			timestamp [2]uint32
		}
		temp := make([]tempComp, len(columns))
		for i := range temp {
			temp[i].column = columns[i]
			temp[i].timestamp = timestamps[i]
		}
		temp_t := make([]tempComp, len(columns_t))
		for i := range temp_t {
			temp_t[i].column = columns_t[i]
			temp_t[i].timestamp = timestamps_t[i]
		}
		sort.Slice(temp, func(i int, j int) bool {
			return strings.Compare(mRMap[temp[i].column], mRMap[temp[j].column]) < 0
		})
		sort.Slice(temp_t, func(i int, j int) bool {
			return strings.Compare(tRMap[temp_t[i].column], tRMap[temp_t[j].column]) < 0
		})
		for i := range temp {
			des := common.BytesToAddress([]byte(mRMap[temp[i].column])).Hex()
			des_t := common.BytesToAddress([]byte(tRMap[temp_t[i].column])).Hex()
			//fmt.Println(temp[i].timestamp, temp_t[i].timestamp)
			if strings.Compare(des, des_t) != 0 || temp[i].timestamp != temp_t[i].timestamp {
				t.Errorf("mismatching of element i=%d: des[i]=%s, des_t[i]=%s, timestamps[i]=%d, timestamps_t[i]=%d", i, des, des_t, timestamps[i], timestamps_t[i])
			}
		}
	}
}

func _TestSearchInSubgraphFromSrc(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	subgraph, err := g.BlockIDWithTokenToSubgraph(context.Background(), uint16(16100000/model.BlockSpan), model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	var srcID uint32
	for i := range subgraph.NodePtrs {
		if subgraph.NodePtrs[i+1]-subgraph.NodePtrs[i] < 100 && subgraph.NodePtrs[i+1]-subgraph.NodePtrs[i] > 10 {
			srcID = uint32(i)
			break
		}
	}
	rMap := model.ReverseAddressMap(subgraph.AddressMap)
	src := rMap[srcID]
	hops, closure := search.ClosureInSubgraphFromSrc(subgraph, common.BytesToAddress([]byte(src)))
	t.Logf("length of hops: %d, closure: %d", len(hops), len(closure))
}

func _TestConstructPaths(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	subgraph, err := g.BlockIDWithTokenToSubgraph(context.Background(), uint16(16100000/model.BlockSpan), model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	var srcID uint32
	for i := range subgraph.NodePtrs {
		if subgraph.NodePtrs[i+1]-subgraph.NodePtrs[i] < 10000 && subgraph.NodePtrs[i+1]-subgraph.NodePtrs[i] > 1000 {
			srcID = uint32(i)
			break
		}
	}
	rMap := model.ReverseAddressMap(subgraph.AddressMap)
	src := rMap[srcID]
	hops, closure := search.ClosureInSubgraphFromSrc(subgraph, common.BytesToAddress([]byte(src)))
	var desID uint32
	for k := range hops[50] {
		desID = k
		break
	}
	paths := search.ConstructPathsAsString(srcID, desID, closure, 1, subgraph, rMap)
	t.Logf("hops: %d, closure:%d, total paths: %d\n src: %s\n des: %s\n", len(hops), len(closure), len(paths), common.BytesToAddress([]byte(src)).Hex(), common.BytesToAddress([]byte(rMap[desID])).Hex())
	for _, addr := range paths[0] {
		fmt.Println(common.BytesToAddress([]byte(addr)).Hex())
	}
	sufMintimestamp := uint32(0)
	for i := len(paths[0]) - 1; i > 0; i-- {
		sV := subgraph.AddressMap[paths[0][i]]
		eV := subgraph.AddressMap[paths[0][i-1]]
		find := false
		time := true
		nextTimestamp := sufMintimestamp
		for ptr := subgraph.NodePtrs[sV]; ptr < subgraph.NodePtrs[sV+1]; ptr++ {
			if subgraph.Columns[ptr] == eV {
				find = true
				if subgraph.Timestamps[ptr][1] < sufMintimestamp {
					time = false
				}
				if subgraph.Timestamps[ptr][0] > sufMintimestamp {
					nextTimestamp = subgraph.Timestamps[ptr][0]
				}
				break
			}
		}
		if !find {
			t.Errorf("edge not find: %s -> %s", common.BytesToAddress([]byte(paths[0][i])).Hex(), common.BytesToAddress([]byte(paths[0][i-1])).Hex())
		}
		if !time {
			t.Errorf("timestamp isn't fit: %d < %d", nextTimestamp, sufMintimestamp)
		}
		sufMintimestamp = nextTimestamp
		fmt.Printf("timstamp: %d\n", sufMintimestamp)
	}
}

func _TestFindOnePathSingleStep(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	subgraphs, err := g.BlockIDsWithTokenToSubgraphs(context.Background(), []uint16{161, 162, 163}, model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())
	var srcID uint32
	for i := range subgraphs[0].NodePtrs {
		if subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] < 1000 && subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] > 500 {
			srcID = uint32(i)
			break
		}
	}
	var desID uint32
	start = time.Now()
	fmt.Printf("reverse address map start\n")
	rMaps := model.ReverseAddressMapsParallel(nil, subgraphs, 1)
	fmt.Printf("reverse address map done: %f\n", time.Since(start).Seconds())
	src := rMaps[0][srcID]
	hops, closure := search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(src)))
	for k := range hops[len(hops)-1] {
		desID = k
		break
	}
	des := rMaps[2][desID]
	fmt.Printf("subgraphs len: %d, closure addresses: %d, max hop: %d\n", len(subgraphs), len(closure), len(hops))
	fmt.Printf("srcID: %d, src: %s\n", srcID, common.BytesToAddress([]byte(src)).Hex())
	fmt.Printf("desID: %d, des: %s\n", desID, common.BytesToAddress([]byte(des)).Hex())
	found, path := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des)), 2)
	if !found {
		t.Errorf("search in single step failed")
	}
	fmt.Printf("result subgraphIndex: %d\n", path[0].SubgraphIdx)
	for _, addr := range path[0].Path {
		fmt.Println(common.BytesToAddress([]byte(addr)).Hex())
	}
}

func _TestFindOnePath(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	subgraphs, err := g.BlockIDsWithTokenToSubgraphs(context.Background(), []uint16{161, 162, 163}, model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())
	var srcID uint32
	for i := range subgraphs[0].NodePtrs {
		if subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] < 10000 && subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] > 500 {
			srcID = uint32(i)
			break
		}
	}
	var des0ID uint32
	start = time.Now()
	fmt.Printf("reverse address map start\n")
	rMaps := model.ReverseAddressMapsParallel(nil, subgraphs, 1)
	fmt.Printf("reverse address map done: %f\n", time.Since(start).Seconds())
	src := rMaps[0][srcID]
	hops, closure := search.ClosureInSubgraphFromSrc(subgraphs[0], common.BytesToAddress([]byte(src)))
	for k := range hops[len(hops)/80] {
		des0ID = k
		break
	}
	fmt.Printf("subgraphs len: %d, closure addresses: %d, max hop: %d\n", len(subgraphs), len(closure), len(hops))
	fmt.Printf("srcID: %d, src: %s\n", srcID, common.BytesToAddress([]byte(src)).Hex())
	des0 := rMaps[0][des0ID]
	var des2ID uint32
	hops, closure = search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(des0)))
	for k := range hops[len(hops)-1] {
		des2ID = k
		break
	}
	des2 := rMaps[2][des2ID]
	fmt.Printf("subgraphs len: %d, closure addresses: %d, max hop: %d\n", len(subgraphs), len(closure), len(hops))
	fmt.Printf("des0ID: %d, des0: %s\n", des0ID, common.BytesToAddress([]byte(des0)).Hex())
	fmt.Printf("des2ID: %d, des2: %s\n", des2ID, common.BytesToAddress([]byte(des2)).Hex())
	found, path := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des2)), 2)
	if !found {
		fmt.Printf("search not found\n")
	}
	fmt.Printf("result subgraphIndex num: %d\n", len(path))
	for i := range path {
		fmt.Printf("result subgraphIndex: %d\n", path[i].SubgraphIdx)
		for _, addr := range path[i].Path {
			fmt.Println(common.BytesToAddress([]byte(addr)).Hex())
		}
	}
}

func _TestFindOnePathSpecific(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	subgraphs_r, err := g.BlockIDsWithTokenToSubgraphs(context.Background(), []uint16{161, 162, 163}, model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	subgraphs := make([]*model.Subgraph, 3)
	for _, subgraph := range subgraphs_r {
		subgraphs[subgraph.BlockID-161] = subgraph
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())
	rMaps := model.ReverseAddressMaps(nil, subgraphs)
	var srcID uint32
	for i := range subgraphs[0].NodePtrs {
		if subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] < 10000 && subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] > 500 {
			//_, ok := subgraphs[2].AddressMap[rMaps[0][i]]
			if rand.Intn(20) == 0 {
				srcID = uint32(i)
				break
			}
		}
	}
	//srcID = 13
	//srcID = 20
	srcID = 340
	for i, rMap := range rMaps {
		fmt.Printf("len(rMap[%d]) = %d\n", i, len(rMap))
	}
	src := rMaps[0][srcID]
	var des0ID uint32
	/*
		_, srcClosure0 := search.ClosureInSubgraphFromSrc(subgraphs[0], common.BytesToAddress([]byte(src)))
		_, srcClosure2 := search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(src)))
		for k := range srcClosure0 {
			_, ok := subgraphs[2].AddressMap[rMaps[0][k]]
			if rand.Intn(4) == 0 && ok {
				des0ID = k
				break
			}
		}
	*/
	//des0ID = 2965089
	//des0ID = 4763738
	des0ID = 324339
	des0 := rMaps[0][des0ID]
	var des2ID uint32
	/*
		_, des0Closure2 := search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(des0)))
		for k := range des0Closure2 {
			_, ok0 := srcClosure0[subgraphs[0].AddressMap[rMaps[2][k]]]
			_, ok2 := srcClosure2[k]
			if !ok0 && !ok2 {
				des2ID = k
				break
			}
		}
	*/
	//des2ID = 2782311
	//des2ID = 1144902
	des2ID = 684168
	des2 := rMaps[2][des2ID]
	fmt.Printf("srcID: %d, src: %s\n", srcID, common.BytesToAddress([]byte(src)).Hex())
	fmt.Printf("des0ID: %d, des0: %s, des0ID2: %d\n", des0ID, common.BytesToAddress([]byte(des0)).Hex(), subgraphs[2].AddressMap[des0])
	fmt.Printf("des2ID: %d, des2: %s\n", des2ID, common.BytesToAddress([]byte(des2)).Hex())
	/*
		found, _ := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des0)), 2)
		if !found {
			fmt.Printf("search src->des0 not found\n")
		} else {
			fmt.Printf("search src->des0 found\n")
		}

		found, _ = search.FindOnePath(subgraphs, common.BytesToAddress([]byte(des0)), common.BytesToAddress([]byte(des2)), 2)
		if !found {
			fmt.Printf("search des0->des2 not found\n")
		} else {
			fmt.Printf("search des0->des2 found\n")
		}
	*/
	found, path := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des2)), 2)
	if !found {
		fmt.Printf("search src->des2 not found\n")
		//found, _ := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des0)), 2)
		//fmt.Printf("search src->des0: %t\n", found)
		//found, _ = search.FindOnePath(subgraphs, common.BytesToAddress([]byte(des0)), common.BytesToAddress([]byte(des2)), 2)
		//fmt.Printf("search des0->des2: %t\n", found)
	}
	fmt.Printf("result subgraphIndex num: %d\n", len(path))
	for i := range path {
		fmt.Printf("result subgraphIndex: %d\n", path[i].SubgraphIdx)
		for _, addr := range path[i].Path {
			fmt.Println(common.BytesToAddress([]byte(addr)).Hex())
		}
	}
}

func __TestFindOnePath(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	subgraphs_r, err := g.BlockIDsWithTokenToSubgraphs(context.Background(), []uint16{161, 162, 163}, model.EtherAddress, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	subgraphs := make([]*model.Subgraph, 3)
	for _, subgraph := range subgraphs_r {
		subgraphs[subgraph.BlockID-161] = subgraph
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())
	/*
		count := 0
		for i := 0; i < len(subgraphs[1].NodePtrs)-1; i++ {
			if subgraphs[1].NodePtrs[i+1]-subgraphs[1].NodePtrs[i] > 5000 {
				count++
			}
		}
		fmt.Println(count)
		return
	*/
	rMaps := model.ReverseAddressMaps(nil, subgraphs)
	var des2ID uint32
	des2ID = 0
	for des2ID == 0 {
		var srcID uint32
		for j := 0; j < 3; j++ {
			superNodeCount0500 := 0
			superNodeCount1000 := 0
			superNodeCount2000 := 0
			superNodeCount3000 := 0
			for i := 0; i < len(subgraphs[j].NodePtrs)-1; i++ {
				if subgraphs[j].NodePtrs[i+1]-subgraphs[j].NodePtrs[i] > 50 {
					superNodeCount0500++
				}
				if subgraphs[j].NodePtrs[i+1]-subgraphs[j].NodePtrs[i] > 1000 {
					superNodeCount1000++
				}
				if subgraphs[j].NodePtrs[i+1]-subgraphs[j].NodePtrs[i] > 2000 {
					superNodeCount2000++
				}
				if subgraphs[j].NodePtrs[i+1]-subgraphs[j].NodePtrs[i] > 3000 {
					superNodeCount3000++
				}
			}
			fmt.Printf("super node count %d %d %d %d\n", superNodeCount0500, superNodeCount1000, superNodeCount2000, superNodeCount3000)
		}

		WETHSubgraphs, err := g.BlockIDsWithTokenToSubgraphs(context.Background(), []uint16{161, 162, 163}, common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), graph.DefaultQueryConfig())
		if err != nil {
			t.Error(err)
		}
		for j := 0; j < 3; j++ {
			fmt.Printf("WETHSubgraph len(Columns) %d, len(NodePtrs) %d\n", len(WETHSubgraphs[j].Columns), len(WETHSubgraphs[j].NodePtrs))
		}

		for i := range subgraphs[0].NodePtrs {
			if subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] < 200 && subgraphs[0].NodePtrs[i+1]-subgraphs[0].NodePtrs[i] > 5 {
				//_, ok := subgraphs[2].AddressMap[rMaps[0][i]]
				if rand.Intn(20) == 0 {
					srcID = uint32(i)
					break
				}
			}
		}
		//srcID = 38
		src := rMaps[0][srcID]
		var des0ID uint32
		_, srcClosure0 := search.ClosureInSubgraphFromSrc(subgraphs[0], common.BytesToAddress([]byte(src)))
		_, srcClosure1 := search.ClosureInSubgraphFromSrc(subgraphs[1], common.BytesToAddress([]byte(src)))
		_, srcClosure2 := search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(src)))
		for k := range srcClosure0 {
			_, ok := subgraphs[2].AddressMap[rMaps[0][k]]
			if rand.Intn(4) == 0 && ok {
				des0ID = k
				break
			}
		}
		//des0ID = 4619228
		des0 := rMaps[0][des0ID]

		_, des0Closure2 := search.ClosureInSubgraphFromSrc(subgraphs[2], common.BytesToAddress([]byte(des0)))
		for k := range des0Closure2 {
			v, ok0 := subgraphs[0].AddressMap[rMaps[2][k]]
			if ok0 {
				_, ok0 = srcClosure0[v]
			}
			v, ok1 := subgraphs[1].AddressMap[rMaps[2][k]]
			if ok0 {
				_, ok1 = srcClosure1[v]
			}
			_, ok2 := srcClosure2[k]
			if !ok0 && !ok1 && !ok2 {
				des2ID = k
				break
			}
		}
		//des2ID = 2999771
		if des2ID == 0 {
			continue
		}
		des2 := rMaps[2][des2ID]
		fmt.Printf("srcID: %d, src: %s\n", srcID, common.BytesToAddress([]byte(src)).Hex())
		fmt.Printf("des0ID: %d, des0: %s, des0ID2: %d\n", des0ID, common.BytesToAddress([]byte(des0)).Hex(), subgraphs[2].AddressMap[des0])
		fmt.Printf("des2ID: %d, des2: %s\n", des2ID, common.BytesToAddress([]byte(des2)).Hex())
		/*
			found, _ := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des0)), 2)
			if !found {
				fmt.Printf("search src->des0 not found\n")
			} else {
				fmt.Printf("search src->des0 found\n")
			}

			found, _ = search.FindOnePath(subgraphs, common.BytesToAddress([]byte(des0)), common.BytesToAddress([]byte(des2)), 2)
			if !found {
				fmt.Printf("search des0->des2 not found\n")
			} else {
				fmt.Printf("search des0->des2 found\n")
			}
		*/
		found, path := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des2)), 1)
		if !found {
			fmt.Printf("search src->des2 not found\n")
			//found, _ := search.FindOnePath(subgraphs, common.BytesToAddress([]byte(src)), common.BytesToAddress([]byte(des0)), 2)
			//fmt.Printf("search src->des0: %t\n", found)
			//found, _ = search.FindOnePath(subgraphs, common.BytesToAddress([]byte(des0)), common.BytesToAddress([]byte(des2)), 2)
			//fmt.Printf("search des0->des2: %t\n", found)
		}
		fmt.Printf("result subgraphIndex num: %d\n", len(path))
		for i := range path {
			fmt.Printf("result subgraphIndex: %d\n", path[i].SubgraphIdx)
			for _, addr := range path[i].Path {
				fmt.Println(common.BytesToAddress([]byte(addr)).Hex())
			}
		}
	}

}

func _TestDensity(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	USDCAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	USDTAddress := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	blockIDs := make([]uint16, 0, 18)
	for i := uint16(161); i < 179; i++ {
		blockIDs = append(blockIDs, i)
	}
	tokens := []common.Address{USDCAddress, USDTAddress}
	subgraphs, err := g.BlockIDsWithTokensToSubgraphs(context.Background(), blockIDs, tokens, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())
	for _, subgraph := range subgraphs {
		fmt.Printf("len(AddressMap): %d\n", len(subgraph.AddressMap))
	}
	mergedSubgraphs := make([]*model.Subgraph, len(blockIDs))
	for i := 0; i < len(blockIDs); i++ {
		mergedSubgraphs[i], err = model.MergeSubgraphs(subgraphs[i*len(tokens) : (i+1)*len(tokens)])
		if err != nil {
			t.Error(err)
		}
		//fmt.Println(len(mergedSubgraphs[i].AddressMap), len(mergedSubgraphs[i].NodePtrs), len(mergedSubgraphs[i].Columns))
	}
	rMaps := model.ReverseAddressMaps(nil, mergedSubgraphs)

	file, err := os.OpenFile("closure-test-20-new.txt", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}
	defer file.Close()
	//addrFile, err := os.OpenFile("addr-test.txt", os.O_CREATE|os.O_RDWR, 0666)
	addrFile, err := os.OpenFile("addr-test.txt", os.O_RDONLY, 0666)
	if err != nil {
		t.Error(err)
	}
	defer addrFile.Close()
	addrReader := bufio.NewReader(addrFile)

	num := 5000
	count := 0
	closureNorm := 0
	/*
		for address, id := range mergedSubgraphs[0].AddressMap {
			odegree := mergedSubgraphs[0].NodePtrs[id+1] - mergedSubgraphs[0].NodePtrs[id]
			if odegree > 50 || odegree < 3 {
				continue
			}
			addrFile.WriteString(common.BytesToAddress([]byte(address)).Hex() + "\n")
			//_, closure := search.ClosureInSubgraphFromSrc(mergedSubgraphs[0], common.BytesToAddress([]byte(address)))
			//start := time.Now()
			closures := search.GetClosures(mergedSubgraphs, common.BytesToAddress([]byte(address)), 16)
			//end0 := time.Now()
			//search.GetClosures(mergedSubgraphs, common.BytesToAddress([]byte(address)), 4)
			//end1 := time.Now()
			nodeSum := 0
			conClosure := make(map[string]uint8)
			for i, closure := range closures {
				for id, v := range closure {
					hoplength := v.HopLength()
					if hoplength > model.MaxHopLimit {
						t.Errorf("path length break limit")
					}
					if h, ok := conClosure[rMaps[i][id]]; !ok || ok && hoplength < h {
						conClosure[rMaps[i][id]] = hoplength
					}
				}
			}*/
	for {
		line, _, err := addrReader.ReadLine()
		if err == io.EOF {
			break
		}
		address := common.HexToAddress(string(line))
		id := mergedSubgraphs[0].AddressMap[string(address.Bytes())]
		odegree := mergedSubgraphs[0].NodePtrs[id+1] - mergedSubgraphs[0].NodePtrs[id]
		if odegree > 50 || odegree < 3 {
			continue
		}
		closures := search.GetClosures(mergedSubgraphs, []common.Address{address}, nil, nil, 1)
		nodeSum := 0
		conClosure := make(map[string]uint8)
		for i, closure := range closures {
			for id, v := range closure {
				hoplength := v.HopLength()
				if hoplength > model.MaxHopLimit {
					t.Errorf("path length break limit")
				}

				if h, ok := conClosure[rMaps[i][id]]; !ok || ok && hoplength < h {
					conClosure[rMaps[i][id]] = hoplength
				}

			}
		}

		hopCounts := make([]int, model.MaxHopLimit+1)
		for _, h := range conClosure {
			hopCounts[h] += 1
		}
		nodeSum = len(conClosure)
		_, err = file.WriteString(strconv.Itoa(nodeSum) + " ")
		if err != nil {
			t.Error(err)
		}
		for _, c := range hopCounts {
			file.WriteString(strconv.Itoa(c) + " ")
		}
		file.WriteString("\n")
		closureNorm += nodeSum
		count++
		if count == num {
			break
		}
	}
	closureNorm /= num
	fmt.Println("closureNorm", closureNorm)
}

func _TestPhishing(t *testing.T) {

}
