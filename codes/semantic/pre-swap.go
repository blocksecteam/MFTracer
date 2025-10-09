package semantic

import (
	"context"
	"math/big"
	sparser "topswap/parser"
	"transfer-graph/model"
	"transfer-graph/pricedb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var topswapParser = (&sparser.Parser{}).DoInit()

func AddTopswap(
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	tsSlice []*model.Transfer,
	pdb *pricedb.PriceDB,
	pdbParallel int,
	ctx context.Context,
) (map[string][]*model.Transfer, []*model.Transfer, error) {
	swapTsCouples := make([][2]*model.Transfer, 0)
	valueIndice := make([]int, 0)
	for _, txs := range txMap {
		for _, tx := range txs {
			parseRets, ok := topswapParse(tx)
			if !ok {
				continue
			}
			for _, parseRet := range parseRets {
				if parseRet.FromAddress == parseRet.ToAddress {
					continue
				}
				var swapTransferA, swapTransferB *model.Transfer = nil, nil
				valueIndex := -1
				if parseRet.TokenIn.Cmp(model.EmptyAddress) != 0 {
					swapTransferA = &model.Transfer{
						Pos:   tx.Pos(),
						Type:  uint16(model.TransferVirtualTypeSwap),
						From:  parseRet.FromAddress,
						To:    parseRet.ToAddress,
						Token: parseRet.TokenIn,
						Value: (*hexutil.Big)(parseRet.ExactInValue),
					}
					if parseRet.ExactInValue != nil {
						valueIndex = 0
					}
				}
				if parseRet.TokenOut.Cmp(model.EmptyAddress) != 0 {
					swapTransferB = &model.Transfer{
						Pos:   tx.Pos(),
						Type:  uint16(model.TransferVirtualTypeSwap),
						From:  parseRet.FromAddress,
						To:    parseRet.ToAddress,
						Token: parseRet.TokenOut,
						Value: (*hexutil.Big)(parseRet.ExactOutValue),
					}
					if valueIndex == -1 && parseRet.ExactOutValue != nil {
						valueIndex = 1
					}
				}
				if valueIndex != -1 {
					swapTsCouples = append(swapTsCouples, [2]*model.Transfer{swapTransferA, swapTransferB})
					valueIndice = append(valueIndice, valueIndex)
				}
			}
		}
	}
	tokens := make([]common.Address, len(swapTsCouples))
	blocks := make([]uint64, len(swapTsCouples))
	for i, swapTsCouple := range swapTsCouples {
		tokens[i] = swapTsCouple[valueIndice[i]].Token
		blocks[i] = swapTsCouple[valueIndice[i]].Block()
	}
	prices, err := pdb.TokensWithBlocks(tokens, blocks, pdbParallel, ctx)
	if err != nil {
		return nil, nil, err
	}
	decimalss, err := pdb.SimpleReadAllDecimals()
	if err != nil {
		return nil, nil, err
	}
	txidMap := make(map[uint64]uint16)
	for i, swapTsCouple := range swapTsCouples {
		decimals, ok := decimalss[string(tokens[i].Bytes())]
		if !ok {
			continue
		}
		price := big.NewFloat(prices[i])
		amount := big.NewFloat(0).SetInt(swapTsCouple[valueIndice[i]].Value.ToInt())
		value, _ := price.Mul(price, amount).Int(nil)
		pfactor := big.NewInt(int64(pricedb.PriceFactor))
		if model.DollarDeciamls > decimals {
			dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(model.DollarDeciamls-decimals)), nil)
			value = value.Mul(value, dfactor)
		} else if model.DollarDeciamls < decimals {
			dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(decimals-model.DollarDeciamls)), nil)
			value = value.Div(value, dfactor)
		}
		value = value.Div(value, pfactor)
		var tid uint16 = 0
		if id, ok := txidMap[swapTsCouple[valueIndice[i]].Pos]; ok {
			tid = id
		}
		for j := range swapTsCouple {
			if swapTsCouple[j] != nil {
				swapTsCouple[j].Value = (*hexutil.Big)(value)
				swapTsCouple[j].Txid = tid
				tsMapKey := makeTsMapKey(swapTsCouple[j].From, swapTsCouple[j].To, swapTsCouple[j].Token)
				if _, ok := tsMap[tsMapKey]; !ok {
					tsMap[tsMapKey] = make([]*model.Transfer, 0, 1)
				}
				tsMap[tsMapKey] = append(tsMap[tsMapKey], swapTsCouple[j])
				tsSlice = append(tsSlice, swapTsCouple[j])
			}
		}
		txidMap[swapTsCouple[valueIndice[i]].Pos] = tid + 1
	}
	return tsMap, tsSlice, nil
}
