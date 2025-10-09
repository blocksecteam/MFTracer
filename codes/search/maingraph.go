package search

import (
	"fmt"
	"transfer-graph/model"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/common"
)

type MainGraph map[uint32]map[uint32]struct{}

func (hrb HopResultBack) ToMainGraph() MainGraph {
	ret := make(MainGraph, len(hrb))
	for thisNode, v := range hrb {
		for preNode := range v.pres {
			if _, ok := ret[preNode]; !ok {
				ret[preNode] = make(map[uint32]struct{})
			}
			ret[preNode][thisNode] = struct{}{}
		}
	}
	return ret
}

func AddressSetToMainGraph(subgraph *model.Subgraph, AddressSet []common.Address) MainGraph {
	ret := make(MainGraph)
	for i, src := range AddressSet {
		srcID, ok := subgraph.AddressMap[utils.AddrToAddrString(src)]
		if !ok {
			continue
		} else if _, ok := ret[srcID]; !ok {
			ret[srcID] = make(map[uint32]struct{})
		}
		for j, des := range AddressSet {
			if desID, ok := subgraph.AddressMap[utils.AddrToAddrString(des)]; ok && i != j && subgraph.IsLinked(srcID, desID) {
				ret[srcID][desID] = struct{}{}
			}
		}
	}
	return ret
}

func GetMainGraph(subgraphs []*model.Subgraph, srcAddresses, desAddresses []common.Address, rMaps [][]string, allowed, forbidden []common.Address, parallel int) []MainGraph {
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	subgraphsR := make([]*model.Subgraph, len(subgraphs))
	rMapsR := make([][]string, len(rMaps))
	closuresF := GetClosures(subgraphs, srcAddresses, allowed, forbidden, parallel)
	for i, r := range closuresF {
		fmt.Printf("search len(closuresF[%d]) = %d\n", i, len(r))
	}
	closuresFR := make([]HopResultBack, len(closuresF))
	for i := range closuresF {
		closuresFR[len(closuresFR)-1-i] = closuresF[i].ToBack()
		subgraphsR[len(subgraphsR)-1-i] = subgraphs[i]
		rMapsR[len(rMapsR)-1-i] = rMaps[i]
	}
	closuresBR := getClosuresBack(closuresFR, subgraphsR, desAddresses, rMapsR, parallel, true, false)
	closuresB := make([]HopResultBack, len(closuresBR))
	for i := range closuresBR {
		closuresB[len(closuresB)-1-i] = closuresBR[i]
	}
	for i, r := range closuresB {
		fmt.Printf("search len(closuresB[%d]) = %d\n", i, len(r))
	}
	closuresM := getClosuresBack(closuresB, subgraphs, srcAddresses, rMaps, parallel, false, false)
	for i, r := range closuresM {
		fmt.Printf("search len(closuresM[%d]) = %d\n", i, len(r))
	}

	ret := make([]MainGraph, len(closuresM))
	for i := range closuresM {
		ret[i] = closuresM[i].ToMainGraph()
	}
	return ret
}

func GetMainGraphPrune(subgraphs []*model.Subgraph, srcAddresses, desAddresses []common.Address, rMaps [][]string, allowed, forbidden []common.Address, pruneIter, parallel int) []MainGraph {
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	subgraphsR := make([]*model.Subgraph, len(subgraphs))
	rMapsR := make([][]string, len(subgraphs))
	for i := range subgraphs {
		subgraphsR[i] = subgraphs[len(subgraphs)-1-i]
		rMapsR[i] = rMaps[len(rMaps)-1-i]
	}
	fmt.Println(forbidden[0].Hex())
	closures := GetClosures(subgraphs, srcAddresses, allowed, forbidden, parallel)
	clsgraphF := make([]HopResultBack, len(subgraphs))
	clsgraphFR := make([]HopResultBack, len(subgraphs))
	clsgraphB := make([]HopResultBack, len(subgraphs))
	var clsgraphBR []HopResultBack
	for i := range closures {
		clsgraphF[i] = closures[i].ToBack()
	}
	for i := 0; i < pruneIter; i++ {
		for j := range clsgraphF {
			clsgraphFR[j] = clsgraphF[len(clsgraphF)-1-j]
		}
		clsgraphBR = getClosuresBack(clsgraphFR, subgraphsR, desAddresses, rMapsR, parallel, true, false)
		for j := range clsgraphBR {
			clsgraphB[j] = clsgraphBR[len(clsgraphBR)-1-j]
		}
		clsgraphF = getClosuresBack(clsgraphB, subgraphs, srcAddresses, rMaps, parallel, false, false)
	}
	ret := make([]MainGraph, len(clsgraphF))
	for i := range clsgraphF {
		ret[i] = clsgraphF[i].ToMainGraph()
	}
	return ret
}

func GetMainGraphReversePrune(subgraphs []*model.Subgraph, desAddresses, srcAddresses []common.Address, rMaps [][]string, pruneIter, parallel int) []MainGraph {
	if rMaps == nil {
		rMaps = model.ReverseAddressMaps(nil, subgraphs)
	}
	reversedSubgraphsR := make([]*model.Subgraph, len(subgraphs))
	rMapsR := make([][]string, len(subgraphs))
	for i := range subgraphs {
		reversedSubgraphsR[i] = model.ReverseSubgraph(subgraphs[len(subgraphs)-1-i])
		rMapsR[i] = rMaps[len(rMaps)-1-i]
	}
	closuresBR := getClosuresBack(nil, reversedSubgraphsR, desAddresses, rMapsR, parallel, true, true)
	closuresB := make([]HopResultBack, len(subgraphs))
	closuresFR := make([]HopResultBack, len(subgraphs))
	var closuresF []HopResultBack
	for i := 0; i < pruneIter; i++ {
		for j := range closuresBR {
			closuresB[len(closuresB)-1-j] = closuresBR[j]
		}
		closuresF = getClosuresBack(closuresB, subgraphs, srcAddresses, rMaps, parallel, false, false)
		for j := range closuresFR {
			closuresFR[j] = closuresF[len(closuresF)-1-j]
		}
		closuresBR = getClosuresBack(closuresFR, subgraphs, desAddresses, rMaps, parallel, true, false)
	}
	ret := make([]MainGraph, len(subgraphs))
	for i := range closuresBR {
		ret[i] = closuresBR[len(closuresBR)-1-i].ToReverse().ToMainGraph()
	}
	return ret
}
