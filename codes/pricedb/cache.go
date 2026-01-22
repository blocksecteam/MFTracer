package pricedb

import (
	"context"
	"math"
	"math/big"
	"transfer-graph-evm/model"
	"transfer-graph-evm/utils"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func fetchPriceFixedBlock(tokens []model.Address, block uint64, pdb *PriceDB, pdbParallel int, pdbCtx context.Context) (map[string]float64, error) {
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

func classTokenBlockID(txs []*model.Tx, tss []*model.Transfer) map[uint64][]model.Address {
	ret := make(map[uint64][]model.Address)
	retSet := make(map[uint64]map[string]struct{})
	for _, tx := range txs {
		blockID := tx.Block / BlockSpan
		if _, ok := retSet[blockID]; !ok {
			retSet[blockID] = make(map[string]struct{})
		}
		retSet[blockID][string(model.NativeTokenAddress.Bytes())] = struct{}{}
	}
	for _, ts := range tss {
		blockID := ts.Block() / BlockSpan
		if _, ok := retSet[blockID]; !ok {
			retSet[blockID] = make(map[string]struct{})
		}
		retSet[blockID][string(ts.Token.Bytes())] = struct{}{}
	}
	for blockID, tokens := range retSet {
		ret[blockID] = make([]model.Address, 0, len(tokens))
		for token := range tokens {
			ret[blockID] = append(ret[blockID], model.BytesToAddress([]byte(token)))
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

func NewPriceCacheHooked() *PriceCache {
	ret := &PriceCache{
		Prices:    make(map[uint64]map[string]float64),
		Decimalss: make(map[string]uint8),
	}
	ret.Prices[0] = map[string]float64{
		string(utils.USDTAddress.Bytes()): 1.0 * float64(PriceFactor),
	}
	ret.Decimalss[string(utils.USDTAddress.Bytes())] = model.DollarDecimals
	return ret
}

func (pc *PriceCache) Price(block uint64, token model.Address) float64 {
	//return pc.Prices[block/BlockSpan][string(token.Bytes())]
	return pc.Prices[0][string(token.Bytes())]
}

func (pc *PriceCache) Decimals(token model.Address) (uint8, bool) {
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

func (pc *PriceCache) ComputeRealValue(amount *hexutil.Big, block uint64, token model.Address) float64 {
	price := pc.Price(block, token)
	decimals, ok := pc.Decimals(token)
	if price == 0 || !ok {
		return 0.0
	}
	fprice := big.NewFloat(price)
	famount := big.NewFloat(0).SetInt(amount.ToInt())
	value, _ := fprice.Mul(fprice, famount).Int(nil)
	pfactor := big.NewInt(int64(PriceFactor))
	if model.DollarDecimals > decimals {
		dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(model.DollarDecimals-decimals)), nil)
		value = value.Mul(value, dfactor)
	} else if model.DollarDecimals < decimals {
		dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(decimals-model.DollarDecimals)), nil)
		value = value.Div(value, dfactor)
	}
	value = value.Div(value, pfactor)
	//note: for balance computation, force value within int64
	if !value.IsInt64() {
		return float64(math.MaxInt64) / math.Pow10(model.DollarDecimals)
	} else {
		return float64(value.Int64()) / math.Pow10(model.DollarDecimals)
	}
}

func (pc *PriceCache) ComputeRealValueWhetherInt(amount *hexutil.Big, block uint64, token model.Address) (float64, bool) {
	price := pc.Price(block, token)
	decimals, ok := pc.Decimals(token)
	if price == 0 || !ok {
		return 0.0, true
	}
	tenBase := big.NewInt(10)
	fprice := big.NewFloat(price)
	famount := big.NewFloat(0).SetInt(amount.ToInt())
	value, _ := fprice.Mul(fprice, famount).Int(nil)
	pfactor := big.NewInt(int64(PriceFactor))
	if model.DollarDecimals > decimals {
		dfactor := big.NewInt(0).Exp(tenBase, big.NewInt(int64(model.DollarDecimals-decimals)), nil)
		value = value.Mul(value, dfactor)
	} else if model.DollarDecimals < decimals {
		dfactor := big.NewInt(0).Exp(tenBase, big.NewInt(int64(decimals-model.DollarDecimals)), nil)
		value = value.Div(value, dfactor)
	}
	value = value.Div(value, pfactor)
	mod := big.NewInt(0).Mod(amount.ToInt(), big.NewInt(0).Exp(tenBase, big.NewInt(int64(decimals)), nil)).Int64()
	//note: for balance computation, force value within int64
	if !value.IsInt64() {
		return float64(math.MaxInt64) / math.Pow10(model.DollarDecimals), mod == 0
	} else {
		return float64(value.Int64()) / math.Pow10(model.DollarDecimals), mod == 0
	}
}
