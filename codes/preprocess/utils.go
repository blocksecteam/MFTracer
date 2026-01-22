package preprocess

import (
	"context"
	"math"
	"math/big"
	"transfer-graph-evm/model"
	"transfer-graph-evm/pricedb"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

var superNodeOutDegreeLimit = model.GetConfigOutDegreeLimit()

var superTxTransferLimit = 5

// an virtual edge with value < this won't be considered
var DollarActivateThreshold = uint64(10 * math.Pow10(model.DollarDecimals))

func getFromAddressOfTxMapKey(txMapKey string) model.Address {
	return model.BytesToAddress([]byte(txMapKey[:len(txMapKey)/2]))
}

func getToAddressOfTxMapKey(txMapKey string) model.Address {
	return model.BytesToAddress([]byte(txMapKey[len(txMapKey)/2:]))
}

func makeTsMapKey(from, to, token model.Address) string {
	return string(token.Bytes()) + string(from.Bytes()) + string(to.Bytes())
}

func getTwoTokenGidAsString(tokenA, tokenB model.Address, blockID uint16) (string, []model.Address) {
	tokens := make([]model.Address, 2)
	tokens[0] = tokenA
	tokens[1] = tokenB
	return string(model.MakeCompositeGIDWithBlockID(blockID, tokens)), tokens
}

func isSemanticProcessed(tss []*model.Transfer) bool {
	for _, ts := range tss {
		if ts.Type > uint16(model.TransferTypeERC1155Batch) {
			return true
		}
	}
	return false
}

func computeValue(amount *hexutil.Big, price float64, decimals uint8) uint64 {
	if price == 0 {
		return 0
	}
	fprice := big.NewFloat(price)
	famount := big.NewFloat(0).SetInt(amount.ToInt())
	value, _ := fprice.Mul(fprice, famount).Int(nil)
	pfactor := big.NewInt(int64(pricedb.PriceFactor))
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
		return math.MaxInt64
	} else {
		return value.Uint64()
	}
}

func fetchPrice(
	tx *model.Tx,
	tss []*model.Transfer,
	pdb *pricedb.PriceDB,
	pdbParallel int,
	ctx context.Context,
) (map[string]float64, error) {

	tokens := make([]model.Address, len(tss)+1)
	blocks := make([]uint64, len(tss)+1)
	for i, ts := range tss {
		tokens[i] = ts.Token
		blocks[i] = tx.Block
	}
	tokens[len(tss)] = model.EtherAddress
	blocks[len(tss)] = tx.Block
	prices, err := pdb.TokensWithBlocks(tokens, blocks, pdbParallel, ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]float64, len(tokens))
	for i, token := range tokens {
		ret[string(token.Bytes())] = prices[i]
	}
	return ret, nil
}
