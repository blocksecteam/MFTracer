package flow

import (
	"math"
	"math/big"
	"transfer-graph/model"
	"transfer-graph/pricedb"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func ComputeTokenValue(amount *hexutil.Big, price float64, decimals uint8) float64 {
	if price == 0 {
		return 0
	}
	fprice := big.NewFloat(price)
	famount := big.NewFloat(0).SetInt(amount.ToInt())
	value, _ := fprice.Mul(fprice, famount).Int(nil)
	pfactor := big.NewInt(int64(pricedb.PriceFactor))
	if model.DollarDeciamls > decimals {
		dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(model.DollarDeciamls-decimals)), nil)
		value = value.Mul(value, dfactor)
	} else if model.DollarDeciamls < decimals {
		dfactor := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(decimals-model.DollarDeciamls)), nil)
		value = value.Div(value, dfactor)
	}
	value = value.Div(value, pfactor)
	//note: for balance computation, force value within int64
	if !value.IsInt64() {
		return float64(math.MaxInt64) / math.Pow10(model.DollarDeciamls)
	} else {
		return float64(value.Int64()) / math.Pow10(model.DollarDeciamls)
	}
}

func ComputeDollarValue(amount *hexutil.Big) float64 {
	value := amount.ToInt()
	//note: for balance computation, force value within int64
	if !value.IsInt64() {
		return float64(math.MaxInt64) / math.Pow10(model.DollarDeciamls)
	} else {
		return float64(value.Int64()) / math.Pow10(model.DollarDeciamls)
	}
}
