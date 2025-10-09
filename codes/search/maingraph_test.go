package search_test

import (
	"context"
	"fmt"
	"testing"
	"time"
	"transfer-graph/graph"
	"transfer-graph/model"
	"transfer-graph/search"

	"github.com/ethereum/go-ethereum/common"
)

func TestGetMainGraph(t *testing.T) {
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		t.Error(err)
	}
	defer g.Close()
	start := time.Now()
	fmt.Printf("query db for subgraphs start\n")
	USDCAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	USDTAddress := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	WETHAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	blockIDs := make([]uint16, 0, 10)
	for i := uint16(165); i < 175; i++ {
		blockIDs = append(blockIDs, i)
	}
	tokens := []common.Address{USDCAddress, USDTAddress, WETHAddress, model.EtherAddress}
	//tokens := []common.Address{model.EtherAddress}
	subgraphs, err := g.BlockIDsWithTokensToSubgraphs(context.Background(), blockIDs, tokens, graph.DefaultQueryConfig())
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("query db for subgraphs done: %f\n", time.Since(start).Seconds())

	mergedSubgraphs := make([]*model.Subgraph, len(blockIDs))
	for i := 0; i < len(blockIDs); i++ {
		mergedSubgraphs[i], err = model.MergeSubgraphs(subgraphs[i*len(tokens) : (i+1)*len(tokens)])
		if err != nil {
			t.Error(err)
		}
	}

	rMaps := model.ReverseAddressMaps(nil, mergedSubgraphs)

	//srcAddress := common.HexToAddress("0x23aA024DB73BdD73E931063AC99909B693F12064")
	//desAddress := common.HexToAddress("0x80C67432656d59144cEFf962E8fAF8926599bCF8")

	srcAddress := common.HexToAddress("0x345e3789538cFB1D5b030cFcE8cA18532C871aaB")
	desAddress := common.HexToAddress("0x4B23d52eFf7C67F5992C2aB6D3f69b13a6a33561")
	//srcID := subgraphs[2].AddressMap[string(srcAddress.Bytes())] 0xE4eDb277e41dc89aB076a1F049f4a3EfA700bCE8
	//firstHop := search.ConvertAddressToHopResult(srcAddress, subgraphs[2])
	//hops, closure := search.ClosureInSubgraph(subgraphs[2], firstHop, nil)
	//fmt.Println(len(hops), len(closure))
	/*
		for i := subgraphs[2].NodePtrs[srcID]; i < subgraphs[2].NodePtrs[srcID+1]; i++ {
			fmt.Println(common.BytesToAddress([]byte(rMaps[2][subgraphs[2].Columns[i]])).Hex())
		}
	*/

	start = time.Now()
	mgs := search.GetMainGraph(mergedSubgraphs, []common.Address{srcAddress}, []common.Address{desAddress}, rMaps, nil, nil, 8)
	fmt.Printf("get MainGraph done: %f\n", time.Since(start).Seconds())
	for _, mg := range mgs {
		edgeCounter := 0
		for _, dmap := range mg {
			edgeCounter += len(dmap)
		}
		fmt.Println("number of nodes:", len(mg), "\t\tnumber of edges: ", edgeCounter)
	}
	/*
		for i := 9; i >= 0; i-- {
			for node := range mgs[i] {
				fmt.Print(common.BytesToAddress([]byte(rMaps[i][node])).Hex(), " ")
				for des := range mgs[i][node] {
					fmt.Print(common.BytesToAddress([]byte(rMaps[i][des])).Hex(), " ")
				}
				fmt.Printf("\n")
			}
			fmt.Printf("%d\n", i)
		}
	*/
}
