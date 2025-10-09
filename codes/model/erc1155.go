package model

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var ERC1155TransferEvents = `[{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "operator", "type": "address"}, {"indexed": true, "internalType": "address", "name": "from", "type": "address"}, {"indexed": true, "internalType": "address", "name": "to", "type": "address"}, {"indexed": false, "internalType": "uint256[]", "name": "ids", "type": "uint256[]"}, {"indexed": false, "internalType": "uint256[]", "name": "values", "type": "uint256[]"}], "name": "TransferBatch", "type": "event"}, {"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "operator", "type": "address"}, {"indexed": true, "internalType": "address", "name": "from", "type": "address"}, {"indexed": true, "internalType": "address", "name": "to", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "id", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "value", "type": "uint256"}], "name": "TransferSingle", "type": "event"}]`

func TransformERC1155(t uint16, operator string, data string) (m map[string]interface{}, err error) {
	abi, err := abi.JSON(bytes.NewReader([]byte(ERC1155TransferEvents)))
	if err != nil {
		return nil, err
	}
	h, err := hexutil.Decode(data)
	if err != nil {
		return nil, err
	}

	m = make(map[string]interface{})
	switch t {
	case uint16(TransferTypeERC1155Single):
		err := abi.Events["TransferSingle"].Inputs.NonIndexed().UnpackIntoMap(m, h)
		if err != nil {
			return nil, err
		}
	case uint16(TransferTypeERC1155Batch):
		err := abi.Events["TransferBatch"].Inputs.NonIndexed().UnpackIntoMap(m, h)
		if err != nil {
			return nil, err
		}
	default:

	}
	for k, v := range m {
		if b, ok := v.(*big.Int); ok {
			m[k] = b.Text(10)
		}
		if b, ok := v.([]*big.Int); ok {
			a := make([]string, 0, len(b))
			for _, e := range b {
				a = append(a, e.Text(10))
			}
			m[k] = a
		}
	}
	m["operator"] = operator
	return m, err
}
