package utils

import (
	"github.com/ethereum/go-ethereum/common"
)

func MinU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MaxU64(a, b uint64) uint64 {
	if a < b {
		return b
	}
	return a
}

func AddrStringToHex(addr string) string {
	return common.BytesToAddress([]byte(addr)).Hex()
}

func AddrStringToAddr(addr string) common.Address {
	return common.BytesToAddress([]byte(addr))
}

func AddrToAddrString(addr common.Address) string {
	return string(addr.Bytes())
}
