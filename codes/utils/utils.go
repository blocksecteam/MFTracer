package utils

import (
	"transfer-graph-evm/model"
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

func AddrStringToStr(addr string) string {
	return model.BytesToAddress([]byte(addr)).String()
}

func AddrStringToAddr(addr string) model.Address {
	return model.BytesToAddress([]byte(addr))
}

func AddrToAddrString(addr model.Address) string {
	return string(addr.Bytes())
}
