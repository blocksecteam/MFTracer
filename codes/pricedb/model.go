package pricedb

import (
	"encoding/binary"
	"transfer-graph-evm/model"

	"github.com/ethereum/go-ethereum/crypto"
)

const (
	BlockSpan              = 10000
	tokenHashLength        = 8
	NeedPricePrefix   bool = true
	NeedDecimalPrefix bool = true
	PriceFactor            = 1000000
)

var (
	PricePrefix   = []byte{'P'}
	DecimalPrefix = []byte{'D'}
)

func handlePricePrefix(ori []byte) []byte {
	if NeedPricePrefix {
		return append(PricePrefix, ori...)
	} else {
		return ori
	}
}

func handleDecimalPrefix(ori []byte) []byte {
	if NeedDecimalPrefix {
		return append(DecimalPrefix, ori...)
	} else {
		return ori
	}
}

func GetBlockID(block uint64) uint16 {
	return uint16(block / BlockSpan)
}

func GetBlockIDAsByte(block uint64) []byte {
	buff := make([]byte, 2)
	blockID := uint16(block / BlockSpan)
	binary.BigEndian.PutUint16(buff, blockID)
	return buff
}

func GetTokenHash(token model.Address) []byte {
	buff := crypto.Keccak256(token.Bytes())
	return buff[0:tokenHashLength]
}

func MakePIDWithBlockID(blockID uint16, token model.Address) []byte {
	buff := make([]byte, 0, 2+tokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokenHash(token)...)
	return handlePricePrefix(buff)
}

func MakePID(block uint64, token model.Address) []byte {
	return MakePIDWithBlockID(uint16(block/BlockSpan), token)
}

func MakeDID(token model.Address) []byte {
	return handleDecimalPrefix(token.Bytes())
}

func ExtractTokenFromDID(did []byte) model.Address {
	if NeedDecimalPrefix {
		return model.BytesToAddress(did[1:])
	} else {
		return model.BytesToAddress(did)
	}
}

func ExtractTokenFromDIDAsString(did []byte) string {
	if NeedDecimalPrefix {
		return string(model.BytesToAddress(did[1:]).Bytes())
	} else {
		return string(model.BytesToAddress(did).Bytes())
	}
}
