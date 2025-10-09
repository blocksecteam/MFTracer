package application

import (
	"context"
	"errors"
	"transfer-graph/graph"
	"transfer-graph/model"
	"transfer-graph/search"

	"github.com/cockroachdb/pebble"
)

func QueryEgdes(g *graph.GraphDB, mg search.MainGraph, mergedSubgraph *model.Subgraph, mergedRMap []string, originSubgraphs []*model.Subgraph, ctx context.Context, qconfig *graph.QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	if mergedRMap == nil {
		mergedRMap = model.ReverseAddressMap(mergedSubgraph.AddressMap)
	}
	var ETHSubgraph *model.Subgraph = nil
	tokenSubgraphs := make([]*model.Subgraph, 0, len(originSubgraphs))
	for _, originSubgraph := range originSubgraphs {
		if originSubgraph.Token.Cmp(model.EtherAddress) == 0 {
			ETHSubgraph = originSubgraph
		} else {
			tokenSubgraphs = append(tokenSubgraphs, originSubgraph)
		}
	}
	var retTxs []*model.Tx
	retTss := make([]*model.Transfer, 0, len(mg)*8)
	if ETHSubgraph != nil {
		retTxs = make([]*model.Tx, 0, len(mg)*8)
		for src, desMap := range mg {
			srcID, ok := ETHSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := ETHSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if ETHSubgraph.IsLinked(srcID, desID) {
					txs, _, err := g.BlockIDWithTokenToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcID, desID, true, qconfig)
					if !(err == nil || errors.Is(err, pebble.ErrNotFound)) {
						return nil, nil, err
					}
					retTxs = append(retTxs, txs...)
					_, transfers, err := g.BlockIDWithTokenToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcID, desID, false, qconfig)
					if !(err == nil || errors.Is(err, pebble.ErrNotFound)) {
						return nil, nil, err
					}
					retTss = append(retTss, transfers...)
				}
			}
		}
	}
	for _, tokenSubgraph := range tokenSubgraphs {
		for src, desMap := range mg {
			srcID, ok := tokenSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := tokenSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if tokenSubgraph.IsLinked(srcID, desID) {
					_, transfers, err := g.BlockIDWithTokenToTxTs(ctx, mergedSubgraph.BlockID, tokenSubgraph.Token, srcID, desID, false, qconfig)
					if err != nil {
						return nil, nil, err
					}
					retTss = append(retTss, transfers...)
				}
			}
		}
	}
	return retTxs, retTss, nil
}

func QueryEgdesP(g *graph.GraphDB, mg search.MainGraph, mergedSubgraph *model.Subgraph, mergedRMap []string, originSubgraphs []*model.Subgraph, ctx context.Context, qconfig *graph.QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	if mergedRMap == nil {
		mergedRMap = model.ReverseAddressMap(mergedSubgraph.AddressMap)
	}
	var ETHSubgraph *model.Subgraph = nil
	tokenSubgraphs := make([]*model.Subgraph, 0, len(originSubgraphs))
	for _, originSubgraph := range originSubgraphs {
		if originSubgraph.Token.Cmp(model.EtherAddress) == 0 {
			ETHSubgraph = originSubgraph
		} else {
			tokenSubgraphs = append(tokenSubgraphs, originSubgraph)
		}
	}
	var retTxs []*model.Tx
	retTss := make([]*model.Transfer, 0, len(mg)*8)
	if ETHSubgraph != nil {
		srcDesPairs := make([][2]uint32, 0, len(mg)*8)
		for src, desMap := range mg {
			srcID, ok := ETHSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := ETHSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if ETHSubgraph.IsLinked(srcID, desID) {
					srcDesPairs = append(srcDesPairs, [2]uint32{srcID, desID})
				}
			}
		}
		txs, _, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcDesPairs, true, qconfig)
		if err != nil {
			return nil, nil, err
		}
		retTxs = txs
		_, tss, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcDesPairs, false, qconfig)
		if err != nil {
			return nil, nil, err
		}
		retTss = append(retTss, tss...)
	}
	for _, tokenSubgraph := range tokenSubgraphs {
		srcDesPairs := make([][2]uint32, 0, len(mg)*8)
		for src, desMap := range mg {
			srcID, ok := tokenSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := tokenSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if tokenSubgraph.IsLinked(srcID, desID) {
					srcDesPairs = append(srcDesPairs, [2]uint32{srcID, desID})
				}
			}
		}
		_, tss, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, tokenSubgraph.BlockID, tokenSubgraph.Token, srcDesPairs, false, qconfig)
		if err != nil {
			return nil, nil, err
		}
		retTss = append(retTss, tss...)
	}
	return retTxs, retTss, nil
}
