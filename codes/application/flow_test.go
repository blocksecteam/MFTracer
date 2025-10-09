package application_test

import (
	"context"
	"fmt"
	"testing"
	"time"
	"transfer-graph/application"
	"transfer-graph/graph"
	"transfer-graph/model"
	"transfer-graph/search"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/common"
)

const DBPath = "/data/test-db-20"

func TestQueryEdges(t *testing.T) {
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
	fmt.Printf("query db for subgraphs done: %f, size: %.2fGB\n", time.Since(start).Seconds(), float64(utils.SizeOf(subgraphs))/float64(1<<30))

	/*
		mergedSubgraphs := make([]*model.Subgraph, len(blockIDs))
		for i := 0; i < len(blockIDs); i++ {
			mergedSubgraphs[i], err = model.MergeSubgraphs(subgraphs[i*len(tokens) : (i+1)*len(tokens)])
			if err != nil {
				t.Error(err)
			}
		}
	*/
	subgraphss := make([][]*model.Subgraph, len(blockIDs))
	for i := 0; i < len(blockIDs); i++ {
		subgraphss[i] = subgraphs[i*len(tokens) : (i+1)*len(tokens)]
	}
	mergedSubgraphs, err := model.MergeSubgraphsBatch(subgraphss, 16)
	if err != nil {
		t.Error(err)
	}

	//mergedSubgraphs := subgraphs

	rMaps := model.ReverseAddressMaps(nil, mergedSubgraphs)

	srcAddress := common.HexToAddress("0x23aA024DB73BdD73E931063AC99909B693F12064")
	//desAddress := common.HexToAddress("0x80C67432656d59144cEFf962E8fAF8926599bCF8")
	desAddress := common.HexToAddress("0x205E94337bC61657b4b698046c3c2c5C1d2Fb8F1")

	//srcAddress := common.HexToAddress("0x345e3789538cFB1D5b030cFcE8cA18532C871aaB")
	//desAddress := common.HexToAddress("0x4B23d52eFf7C67F5992C2aB6D3f69b13a6a33561")
	//desAddress := common.HexToAddress("0x8BCeaA96CA68Cd325305b46f887FA433700C7C04")
	//interAddress := common.HexToAddress("0x7e462d1C8E09FDBe7EaA2Ec93E3fe9Aec27fe458")

	start = time.Now()
	mgs := search.GetMainGraphPrune(mergedSubgraphs, []common.Address{srcAddress}, []common.Address{desAddress}, rMaps, nil, nil, 1, 16)
	fmt.Printf("get MainGraph done: %f\n", time.Since(start).Seconds())
	addrSet := make(map[string]struct{})
	for i, mg := range mgs {
		edgeCounter := 0
		for src, dmap := range mg {
			addrSet[rMaps[i][src]] = struct{}{}
			edgeCounter += len(dmap)
			for des := range dmap {
				addrSet[rMaps[i][des]] = struct{}{}
			}
		}
		fmt.Println("number of nodes:", len(mg), "\t\tnumber of edges: ", edgeCounter)
	}

	zeroAddress := common.HexToAddress("0x0000000000000000000000000000000000000000")
	_, ok0 := addrSet[string(WETHAddress.Bytes())]
	_, ok1 := addrSet[string(zeroAddress.Bytes())]
	fmt.Println("number of addrs:", len(addrSet), "WETH:", ok0, "zero:", ok1)
	txs := make([][]*model.Tx, len(mergedSubgraphs))
	tss := make([][]*model.Transfer, len(mergedSubgraphs))
	start = time.Now()
	for i := 0; i < len(mergedSubgraphs); i++ {
		qconfig := graph.DefaultQueryConfig()
		qconfig.FetchThreads = 8
		txs[i], tss[i], err = graph.QueryMGEdgesParallel(g, mgs[i], mergedSubgraphs[i], rMaps[i], subgraphs[i*len(tokens):(i+1)*len(tokens)], 4, context.Background(), qconfig)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("len(txs) = %d, len(tss) = %d\n", len(txs[i]), len(tss[i]))
	}
	fmt.Printf("query Edges done: %f\n", time.Since(start).Seconds())
	fgs := make([]*application.TimeFlowGraph, len(mergedSubgraphs))
	start = time.Now()
	fgs[0] = application.NewTimeFlowGraph([]common.Address{srcAddress}, []common.Address{desAddress}, txs[0], tss[0])
	fgs[0].EvolveToEnd()
	fmt.Println(fgs[0].BalanceOf(srcAddress), fgs[0].BalanceOf(desAddress), len(fgs[0].Balances()))
	/*
		for src, desMap := range mgs[3] {
			for des := range desMap {
				fmt.Println(common.BytesToAddress([]byte(rMaps[3][src])).Hex(), common.BytesToAddress([]byte(rMaps[3][des])).Hex())
			}
		}
		for _, tx := range txs[3] {
			fmt.Println(tx.From.Hex(), tx.To.Hex(), tx.Value.ToInt().Uint64())
		}
		for i := 0; i < 20; i++ {
			fmt.Println(i)
			fgs[3].EvolveByStep(1)
			for addr, v := range fgs[3].Balances() {
				fmt.Println(common.BytesToAddress([]byte(addr)).Hex(), v[0], v[1])
			}
		}
	*/
	for i := 1; i < len(mergedSubgraphs); i++ {
		fgs[i] = application.NewTimeFlowGraphByInherit(fgs[i-1], txs[i], tss[i])
		fgs[i].EvolveToEnd()
		fmt.Println(fgs[i].BalanceOf(srcAddress), fgs[i].BalanceOf(desAddress), len(fgs[i].Balances()))
	}
	fmt.Printf("flow evolve done: %f\n", time.Since(start).Seconds())
	/*
		for i := application.HeavyTypeByTotalIn; i < 1; i++ {
			for _, fg := range fgs {
				heavyAddrs := fg.HeavyNodes(10, i)
				for j := range heavyAddrs {
					balance := fg.BalanceOf(heavyAddrs[j])
					fmt.Println(heavyAddrs[j].Hex(), balance[0], balance[1], balance[0]-balance[1])
				}
				fmt.Printf("\n")
			}
			fmt.Printf("\n")
		}
	*/
}
