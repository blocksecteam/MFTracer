package pricedb_test

import (
	"fmt"
	"testing"
	"transfer-graph-evm/model"
	"transfer-graph-evm/pricedb"
)

func _TestRequest(t *testing.T) {
	tokens := make([]model.Address, 4)
	tokens[0] = model.NativeTokenAddress
	tokens[1] = model.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	tokens[2] = model.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	tokens[3] = model.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	prices, err := pricedb.FetchPrice(19328659, 1709155095, tokens)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(prices)
}
