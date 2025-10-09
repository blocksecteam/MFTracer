package encoding

import (
	"bytes"
	"fmt"
	"math/big"
	"time"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rlp"
)

type TransferMarshalRlp struct {
	Pos   uint64         `json:"pos"` // block << 16 | index
	Txid  uint16         `json:"id"`
	Type  uint16         `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *big.Int       `json:"value"`
	Token common.Address `json:"token"`
}

func EncodeTransferRlp(t *model.Transfer) ([]byte, error) {
	m := &TransferMarshalRlp{
		Txid:  t.Txid,
		From:  t.From,
		To:    t.To,
		Type:  t.Type,
		Value: t.Value.ToInt(),
		Token: t.Token,
		Pos:   t.Pos,
	}

	buf := bytes.NewBuffer(nil)
	if err := rlp.Encode(buf, &m); err != nil {
		return nil, fmt.Errorf("rlp encode error: %s", err.Error())
	}
	return buf.Bytes(), nil
}

func DecodeTransferRlp(b []byte) (*model.Transfer, error) {
	m := TransferMarshalRlp{}
	if err := rlp.DecodeBytes(b, &m); err != nil {
		return nil, err
	}

	t := &model.Transfer{}
	t.Txid = m.Txid
	t.From = m.From
	t.To = m.To
	t.Type = m.Type
	v := hexutil.Big(*m.Value)
	t.Value = &v
	t.Pos = m.Pos
	return t, nil
}

type TxMarshalRlp struct {
	Block      uint64         `json:"block"`
	Index      uint16         `json:"index"`
	Time       uint64         `json:"time"`
	Func       string         `json:"func"`
	From       common.Address `json:"from"`
	To         common.Address `json:"to"`
	TxHash     common.Hash    `json:"txHash"`
	Value      *big.Int       `json:"value"`
	IsCreation bool           `json:"isCreation"`
	Fee        *big.Int       `json:"fee"`
}

func EncodeTxRlp(tx *model.Tx) ([]byte, error) {
	t, err := time.Parse(time.RFC3339, tx.Time)
	if err != nil {
		return nil, fmt.Errorf("parse time failed: %s (Time = '%s')", err.Error(), tx.Time)
	}
	t = t.UTC()

	m := &TxMarshalRlp{
		Func:   tx.Func,
		Index:  tx.Index,
		From:   tx.From,
		Block:  tx.Block,
		To:     tx.To,
		Time:   uint64(t.Unix()),
		TxHash: tx.TxHash,
		Value:  tx.Value.ToInt(),
		Fee:    tx.Fee.ToInt(),
	}

	buf := bytes.NewBuffer(nil)
	if err := rlp.Encode(buf, &m); err != nil {
		return nil, fmt.Errorf("rlp encode error: %s", err.Error())
	}
	return buf.Bytes(), nil
}

func DecodeTxRlp(b []byte) (*model.Tx, error) {
	m := TxMarshalRlp{}
	if err := rlp.DecodeBytes(b, &m); err != nil {
		return nil, err
	}

	tx := model.Tx{}
	tx.Func = m.Func
	tx.Param = nil
	tx.Index = m.Index
	tx.From = m.From
	tx.To = m.To
	tx.Time = time.Unix(int64(m.Time), 0).Format(time.RFC3339)
	tx.TxHash = m.TxHash
	v := hexutil.Big(*m.Value)
	tx.Value = &v
	f := hexutil.Big(*m.Value)
	tx.Fee = &f
	return &tx, nil
}
