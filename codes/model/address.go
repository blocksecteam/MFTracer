package model

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type Address common.Address

func (a Address) Bytes() []byte {
	return common.Address(a).Bytes()
}

func (a Address) Hex() string {
	return common.Address(a).Hex()
}

func (a Address) String() string {
	return common.Address(a).String()
}

func (a Address) Cmp(b Address) int {
	return bytes.Compare(a.Bytes(), b.Bytes())
}

func (a Address) Equal(b Address) bool {
	return bytes.Equal(a.Bytes(), b.Bytes())
}

func BytesToAddress(b []byte) Address {
	return Address(common.BytesToAddress(b))
}

func BigToAddress(b *big.Int) Address {
	return Address(common.BigToAddress(b))
}

func HexToAddress(s string) Address {
	return Address(common.HexToAddress(s))
}

var NativeTokenAddress = Address{}
var NativeTokenNotation = "ETH"

var AddressByteLength = len(NativeTokenAddress.Bytes())

func IsNativeToken(address Address) bool {
	return bytes.Equal(address.Bytes(), NativeTokenAddress.Bytes())
}
