package pricedb

import (
	"context"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
)

func fetchPriceFixedBlock(tokens []common.Address, block uint64, pdb *PriceDB, pdbParallel int, pdbCtx context.Context) (map[string]float64, error) {
	blocks := make([]uint64, len(tokens))
	for i := range tokens {
		blocks[i] = block
	}
	prices, err := pdb.TokensWithBlocks(tokens, blocks, pdbParallel, pdbCtx)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]float64, len(tokens))
	for i, token := range tokens {
		ret[string(token.Bytes())] = prices[i]
	}
	return ret, nil
}

func classTokenBlockID(txs []*model.Tx, tss []*model.Transfer) map[uint64][]common.Address {
	ret := make(map[uint64][]common.Address)
	retSet := make(map[uint64]map[string]struct{})
	for _, tx := range txs {
		blockID := tx.Block / BlockSpan
		if _, ok := retSet[blockID]; !ok {
			retSet[blockID] = make(map[string]struct{})
		}
		retSet[blockID][string(model.EtherAddress.Bytes())] = struct{}{}
	}
	for _, ts := range tss {
		blockID := ts.Block() / BlockSpan
		if _, ok := retSet[blockID]; !ok {
			retSet[blockID] = make(map[string]struct{})
		}
		retSet[blockID][string(ts.Token.Bytes())] = struct{}{}
	}
	for blockID, tokens := range retSet {
		ret[blockID] = make([]common.Address, 0, len(tokens))
		for token := range tokens {
			ret[blockID] = append(ret[blockID], common.BytesToAddress([]byte(token)))
		}
	}
	return ret
}

type PriceCache struct {
	Prices      map[uint64]map[string]float64
	Decimalss   map[string]uint8
	pdb         *PriceDB
	pdbParallel int
	pdbCtx      context.Context
}

func NewPriceCache(txs []*model.Tx, tss []*model.Transfer, pdb *PriceDB, pdbParallel int, pdbCtx context.Context) (*PriceCache, error) {
	tokenMapByBlockID := classTokenBlockID(txs, tss)
	ret := &PriceCache{
		Prices:      make(map[uint64]map[string]float64, len(tokenMapByBlockID)),
		pdb:         pdb,
		pdbParallel: pdbParallel,
		pdbCtx:      pdbCtx,
	}
	for blockID, tokens := range tokenMapByBlockID {
		prices, err := fetchPriceFixedBlock(tokens, blockID*BlockSpan, pdb, pdbParallel, pdbCtx)
		if err != nil {
			return nil, err
		}
		ret.Prices[blockID] = prices
	}
	var err error
	ret.Decimalss, err = pdb.SimpleReadAllDecimals()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (pc *PriceCache) Price(block uint64, token common.Address) float64 {
	return pc.Prices[block/BlockSpan][string(token.Bytes())]
	//return 2e+09
}

func (pc *PriceCache) Decimals(token common.Address) (uint8, bool) {
	ret, ok := pc.Decimalss[string(token.Bytes())]
	return ret, ok
}

func (pc *PriceCache) UpdateCache(txs []*model.Tx, tss []*model.Transfer) error {
	tokenMapByBlockID := classTokenBlockID(txs, tss)
	for blockID, tokens := range tokenMapByBlockID {
		prices, err := fetchPriceFixedBlock(tokens, blockID*BlockSpan, pc.pdb, pc.pdbParallel, pc.pdbCtx)
		if err != nil {
			return err
		}
		for k, v := range prices {
			pc.Prices[blockID][k] = v
		}
	}
	if pc.Decimalss == nil {
		var err error
		pc.Decimalss, err = pc.pdb.SimpleReadAllDecimals()
		if err != nil {
			return err
		}
	}
	return nil
}

func (pc *PriceCache) FlashCache() {
	pc.Prices = nil
}

func (pc *PriceCache) SetPdb(pdb *PriceDB, pdbParallel int, pdbCtx context.Context) {
	pc.pdb = pdb
	pc.pdbParallel = pdbParallel
	pc.pdbCtx = pdbCtx
}

func (pc *PriceCache) Free() {
	pc.pdb = nil
	pc.Prices = nil
	pc.Decimalss = nil
}
