package utils

import (
	"math/big"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
)

var WETHAddress = common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
var USDCAddress = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
var USDTAddress = common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")

func PriceByUSDToInt_Temp(token common.Address, value *big.Int) uint64 {
	if token.Cmp(model.EtherAddress) == 0 || token.Cmp(WETHAddress) == 0 {
		var temp *big.Int = big.NewInt(0)
		var fact *big.Int = big.NewInt(2000)
		temp = temp.Mul(value, fact)
		fact, ok := fact.SetString("1000000000000", 10)
		if !ok {
			return 0
		}
		temp = temp.Div(temp, fact)
		return temp.Uint64()
	} else if token.Cmp(USDCAddress) == 0 || token.Cmp(USDTAddress) == 0 {
		return value.Uint64()
	}
	return 0
}
