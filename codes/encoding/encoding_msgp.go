package encoding

import (
	"encoding/json"
	"math/big"
	"time"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/tinylib/msgp/msgp"
)

const (
	TransferBufferSize = 128
	TxBufferSize       = 256

	// Enable this for experiment
	supportExtras = false
)

// ORDER IS IMPORTANT.
// If order is modified, modify msgp encoding/decoding accordingly
type TransferMsgp struct {
	Pos   uint64 `msg:"p"`
	Txid  uint16 `msg:"x"`
	Type  uint16 `msg:"i"`
	From  []byte `msg:"f"`
	To    []byte `msg:"t"`
	Token []byte `msg:"c"`
	Value []byte `msg:"v"`
}

type TxMsgp struct {
	Block      uint64 `msg:"b"`
	Index      uint16 `msg:"i"`
	TxHash     []byte `msg:"h"`
	Time       uint64 `msg:"ts"`
	From       []byte `msg:"f"`
	To         []byte `msg:"t"`
	IsCreation bool   `msg:"c"`
	Value      []byte `msg:"v"`
	Fee        []byte `msg:"g"`
	Func       string `msg:"fn"`
}

type SubgraphMsgp struct {
	BlockID    uint16            `msg:"b"`
	Token      []byte            `msg:"t"`
	Timestamps []uint32          `msg:"s"`
	Columns    []uint32          `msg:"c"`
	NodePtrs   []uint32          `msg:"p"`
	AddressMap map[string]uint32 `msg:"m"`
}

func EncodeSubgraphMsgp(g *model.Subgraph) ([]byte, error) {
	b := make([]byte, 0)
	size := msgp.GuessSize(g.BlockID)
	size += msgp.GuessSize(g.Token.Bytes())
	size += len(g.Timestamps) * msgp.GuessSize(g.Timestamps[0])
	size += len(g.Columns) * msgp.GuessSize(g.Columns[0])
	size += len(g.NodePtrs) * msgp.GuessSize(g.NodePtrs[0])
	size += msgp.GuessSize(g.AddressMap)
	size += 512
	o := msgp.Require(b, size)
	var err error

	o = msgp.AppendUint16(o, g.BlockID)
	o = msgp.AppendBytes(o, g.Token.Bytes())
	timestamps := make([]uint64, len(g.Timestamps))
	for i, t := range g.Timestamps {
		temp := uint64(t[0])
		temp = temp << 32
		temp = temp | uint64(t[1])
		timestamps[i] = temp
	}
	o, err = msgp.AppendIntf(o, timestamps)
	if err != nil {
		return nil, msgp.WrapError(err, "Timestamps")
	}
	o, err = msgp.AppendIntf(o, g.Columns)
	if err != nil {
		return nil, msgp.WrapError(err, "Columns")
	}
	o, err = msgp.AppendIntf(o, g.NodePtrs)
	if err != nil {
		return nil, msgp.WrapError(err, "NodePtrs")
	}
	addressMap := make(map[string]interface{}, len(g.AddressMap))
	for k, v := range g.AddressMap {
		addressMap[k] = interface{}(v)
	}
	o, err = msgp.AppendMapStrIntf(o, addressMap)
	if err != nil {
		return nil, msgp.WrapError(err, "AddressMap")
	}
	return o, nil
}

func DecodeSubgraphMsgp(b []byte) (g *model.Subgraph, o []byte, err error) {
	var v []byte
	o = b
	g = &model.Subgraph{}

	g.BlockID, o, err = msgp.ReadUint16Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "BlockID")
		return
	}

	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "Token")
		return
	}
	g.Token = common.BytesToAddress(v)

	vi2, o, err := msgp.ReadIntfBytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Timestamps")
		return
	}
	viArray2 := vi2.([]interface{})
	g.Timestamps = make([][2]uint32, len(viArray2))
	for i := range viArray2 {
		var temp uint64
		switch viArray2[i].(type) {
		case uint64:
			temp = viArray2[i].(uint64)
		case int64:
			temp = uint64(viArray2[i].(int64))
		}
		g.Timestamps[i][0] = uint32(temp >> 32)
		g.Timestamps[i][1] = uint32(temp & 0x00000000ffffffff)
	}

	vi0, o, err := msgp.ReadIntfBytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Columns")
		return
	}
	viArray0 := vi0.([]interface{})
	g.Columns = make([]uint32, len(viArray0))
	for i := range viArray0 {
		switch viArray0[i].(type) {
		case uint64:
			g.Columns[i] = uint32(viArray0[i].(uint64))
		case int64:
			g.Columns[i] = uint32(viArray0[i].(int64))
		}
	}

	vi1, o, err := msgp.ReadIntfBytes(o)
	if err != nil {
		err = msgp.WrapError(err, "NodePtrs")
		return
	}
	viArray1 := vi1.([]interface{})
	g.NodePtrs = make([]uint32, len(viArray1))
	for i := range viArray1 {
		switch value := viArray1[i].(type) {
		case uint64:
			g.NodePtrs[i] = uint32(value)
		case int64:
			g.NodePtrs[i] = uint32(value)
		}
	}

	vm, o, err := msgp.ReadMapStrIntfBytes(o, nil)
	if err != nil {
		err = msgp.WrapError(err, "AddressMap")
		return
	}
	g.AddressMap = make(map[string]uint32, len(vm))
	for key, value := range vm {
		switch value := value.(type) {
		case uint64:
			g.AddressMap[key] = uint32(value)
		case int64:
			g.AddressMap[key] = uint32(value)
		}
	}

	return
}

func EncodeTransferMsgp(t *model.Transfer) ([]byte, error) {
	b := make([]byte, 0)
	o := msgp.Require(b, TransferBufferSize)

	o = msgp.AppendUint64(o, t.Pos)
	o = msgp.AppendUint16(o, t.Txid)
	o = msgp.AppendUint16(o, t.Type)

	a := []byte{}
	if t.From != model.EmptyAddress {
		a = common.TrimLeftZeroes(t.From[:])
	}
	o = msgp.AppendBytes(o, a)
	a = []byte{}
	if t.To != model.EmptyAddress {
		a = common.TrimLeftZeroes(t.To[:])
	}
	o = msgp.AppendBytes(o, a)
	a = []byte{}
	if t.Token != model.EtherAddress {
		a = common.TrimLeftZeroes(t.Token[:])
	}
	o = msgp.AppendBytes(o, a)
	o = msgp.AppendBytes(o, t.Value.ToInt().Bytes())

	// extras
	if len(t.Extras) > 0 {
		r, err := json.Marshal(t.Extras)
		if err != nil {
			return nil, err
		}
		o = msgp.AppendBytes(o, r)
	}
	return o, nil
}

func DecodeTransferMsgp(b []byte) (t *model.Transfer, o []byte, err error) {
	var v []byte
	o = b
	t = &model.Transfer{}

	t.Pos, o, err = msgp.ReadUint64Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Pos")
		return
	}
	t.Txid, o, err = msgp.ReadUint16Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Txid")
		return
	}
	t.Type, o, err = msgp.ReadUint16Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Type")
		//err = msgp.WrapError(err, fmt.Sprintf("Type[%x, %x, %s]", t.Pos, t.Txid, hexutil.Encode(b)))
		return
	}
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "From")
		return
	}
	t.From = common.BytesToAddress(v)
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "To")
		return
	}
	t.To = common.BytesToAddress(v)
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "Token")
		return
	}
	t.Token = common.BytesToAddress(v)
	//if t.Token.Cmp(model.EmptyAddress) == 0 && t.Type == uint16(model.TransferTypeExternal) {
	if t.Token.Cmp(model.EmptyAddress) == 0 &&
		(t.Type == uint16(model.TransferTypeExternal) ||
			t.Type == uint16(model.TransferTypeInternal) ||
			model.IsVirualTransfer(t.Type)) {
		t.Token = model.EtherAddress
	}
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "Value")
		return
	}
	t.Value = (*hexutil.Big)(big.NewInt(0).SetBytes(v))

	// extras
	if supportExtras {
		v, o, err = msgp.ReadBytesZC(o)
		if err != nil {
			err = msgp.WrapError(err, "Extras")
			return
		}
		if len(v) > 0 {
			err = json.Unmarshal(v, &t.Extras)
		}
	}
	return
}

func EncodeTxMsgp(tx *model.Tx) ([]byte, error) {
	b := make([]byte, 0)
	o := msgp.Require(b, TxBufferSize)

	o = msgp.AppendUint64(o, tx.Block)
	o = msgp.AppendUint16(o, tx.Index)
	o = msgp.AppendBytes(o, tx.TxHash[:])
	o = msgp.AppendUint64(o, tx.GetTimeU64())
	o = msgp.AppendBytes(o, tx.From[:])
	o = msgp.AppendBytes(o, tx.To[:])
	o = msgp.AppendBool(o, tx.IsCreation)
	o = msgp.AppendBytes(o, tx.Value.ToInt().Bytes())
	o = msgp.AppendBytes(o, tx.GetFee(model.Ethereum).Bytes())
	o = msgp.AppendString(o, tx.Func)
	return o, nil
}

func DecodeTxMsgp(b []byte) (tx *model.Tx, o []byte, err error) {
	var v []byte
	var ts uint64
	o = b
	tx = &model.Tx{}

	// Block & Index
	tx.Block, o, err = msgp.ReadUint64Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Block")
		return
	}
	tx.Index, o, err = msgp.ReadUint16Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Index")
		return
	}
	// TxHash
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "TxHash")
		return
	}
	tx.TxHash = common.BytesToHash(v)
	// Time
	ts, o, err = msgp.ReadUint64Bytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Time")
		return
	}
	tx.Time = time.Unix(int64(ts), 0).Format(time.RFC3339)
	// From
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "From")
		return
	}
	tx.From = common.BytesToAddress(v)
	// To
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "To")
		return
	}
	tx.To = common.BytesToAddress(v)
	// IsCreation
	tx.IsCreation, o, err = msgp.ReadBoolBytes(o)
	if err != nil {
		err = msgp.WrapError(err, "IsCreation")
		return
	}
	// Value
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "Value")
		return
	}
	tx.Value = (*hexutil.Big)(big.NewInt(0).SetBytes(v))
	// Fee
	v, o, err = msgp.ReadBytesZC(o)
	if err != nil {
		err = msgp.WrapError(err, "Fee")
		return
	}
	tx.Fee = (*hexutil.Big)(big.NewInt(0).SetBytes(v))
	// Func
	tx.Func, o, err = msgp.ReadStringBytes(o)
	if err != nil {
		err = msgp.WrapError(err, "Func")
		return
	}
	return
}
