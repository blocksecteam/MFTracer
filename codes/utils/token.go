package utils

import (
	"transfer-graph-evm/model"
)

var USDTAddressHex string = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
var USDTAddress = model.HexToAddress(USDTAddressHex)

var USDTDecimals uint8 = 6

var WETHAddress = model.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
