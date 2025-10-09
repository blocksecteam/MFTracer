package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"transfer-graph/model"
	"transfer-graph/utils"
)

const MaxTPerRecord = 64

func MetadataToStorage(t *model.Metadata) ([]byte, error) {
	m, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("MetadataToStorage json.Marshal: %s", err.Error())
	}
	buf := bytes.NewBuffer(nil)
	err = utils.Compress(bytes.NewBuffer(m), buf)
	if err != nil {
		return nil, fmt.Errorf("TxCalldataToExtra Compress: %s", err.Error())
	}
	return buf.Bytes(), nil
}

type Encoding interface {
	EncodeSubgraph(g *model.Subgraph) ([]byte, error)
	DecodeSubgraph(b []byte) (*model.Subgraph, error)
	EncodeTx(tx *model.Tx) ([]byte, error)
	DecodeTx(b []byte) (*model.Tx, error)
	EncodeTransfer(t *model.Transfer) ([]byte, error)
	DecodeTransfer(b []byte) (*model.Transfer, error)
	EncodeMetadata(m *model.Metadata) ([]byte, error)
	DecodeMetadata(b []byte) (*model.Metadata, error)

	EncodeTxs(txs []*model.Tx) ([][]byte, error)
	DecodeTxs(b []byte) ([]*model.Tx, error)
	EncodeTransfers(tsfs []*model.Transfer) ([][]byte, error)
	DecodeTransfers(b []byte) ([]*model.Transfer, error)
}

type Msgp struct{}

func (Msgp) EncodeSubgraph(g *model.Subgraph) ([]byte, error) {
	return EncodeSubgraphMsgp(g)
}

func (Msgp) DecodeSubgraph(b []byte) (*model.Subgraph, error) {
	g, _, err := DecodeSubgraphMsgp(b)
	return g, err
}

func (Msgp) EncodeTx(tx *model.Tx) ([]byte, error) {
	return EncodeTxMsgp(tx)
}

func (Msgp) DecodeTx(b []byte) (*model.Tx, error) {
	t, _, err := DecodeTxMsgp(b)
	return t, err
}

func (Msgp) EncodeTransfer(t *model.Transfer) ([]byte, error) {
	return EncodeTransferMsgp(t)
}

func (Msgp) DecodeTransfer(b []byte) (*model.Transfer, error) {
	t, _, err := DecodeTransferMsgp(b)
	return t, err
}

func (Msgp) EncodeMetadata(m *model.Metadata) ([]byte, error) {
	return MetadataToStorage(m)
}

func (Msgp) DecodeMetadata(b []byte) (*model.Metadata, error) {
	panic("not implemented") // TODO: Implement
}

func (Msgp) EncodeTxs(txs []*model.Tx) ([][]byte, error) {
	return DefaultEncodeTxs(txs)
}

func (Msgp) DecodeTxs(b []byte) ([]*model.Tx, error) {
	return DefaultDecodeTxs(b)
}

func (Msgp) EncodeTransfers(tsfs []*model.Transfer) ([][]byte, error) {
	return DefaultEncodeTransfers(tsfs)
}

func (Msgp) DecodeTransfers(b []byte) ([]*model.Transfer, error) {
	return DefaultDecodeTransfers(b)
}

var DefaultEncoding Encoding = Msgp{}

func DefaultEncodeTxs(txs []*model.Tx) ([][]byte, error) {
	length := len(txs)
	if length == 0 {
		return nil, fmt.Errorf("try to encode empty txs")
	}
	ret := make([][]byte, 0)
	for i := 0; i < length; i += MaxTPerRecord {
		buff := make([]byte, 0)
		txLens := make([]uint16, 0)
		for j := 0; j < MaxTPerRecord && i+j < length; j++ {
			k := i + j
			b, err := EncodeTxMsgp(txs[k])
			if err != nil {
				return nil, fmt.Errorf("encode txs[%d] failed", k)
			}
			buff = append(buff, b...)
			txLens = append(txLens, uint16(len(b)))
		}
		thisLen := len(txLens)
		lenBuff := make([]byte, 0, 2+thisLen*2)
		lenBuff = binary.BigEndian.AppendUint16(lenBuff, uint16(thisLen))
		for _, txLen := range txLens {
			lenBuff = binary.BigEndian.AppendUint16(lenBuff, txLen)
		}
		ret = append(ret, append(lenBuff, buff...))
	}
	return ret, nil
}

func DefaultDecodeTxs(b []byte) ([]*model.Tx, error) {
	length := int(binary.BigEndian.Uint16(b[:2]))
	txLens := make([]int, length)
	for i := 0; i < length; i++ {
		txLen := binary.BigEndian.Uint16(b[2+i*2 : 2+i*2+2])
		txLens[i] = int(txLen)
	}
	ret := make([]*model.Tx, length)
	formerLen := 2 + length*2
	for i := 0; i < length; i++ {
		tx, _, err := DecodeTxMsgp(b[formerLen : formerLen+txLens[i]])
		if err != nil {
			return nil, fmt.Errorf("decode txs[%d] failed: %s", i, err.Error())
		}
		ret[i] = tx
		formerLen += txLens[i]
	}
	return ret, nil
}

func DefaultEncodeTransfers(tsfs []*model.Transfer) ([][]byte, error) {
	length := len(tsfs)
	if length == 0 {
		return nil, fmt.Errorf("try to encode empty tsfs")
	}
	ret := make([][]byte, 0)
	for i := 0; i < length; i += MaxTPerRecord {
		buff := make([]byte, 0)
		tsfLens := make([]uint16, 0)
		for j := 0; j < MaxTPerRecord && i+j < length; j++ {
			k := i + j
			b, err := EncodeTransferMsgp(tsfs[k])
			if err != nil {
				return nil, fmt.Errorf("encode tsfs[%d] failed", k)
			}
			buff = append(buff, b...)
			tsfLens = append(tsfLens, uint16(len(b)))
		}
		thisLen := len(tsfLens)
		lenBuff := make([]byte, 0, 2+thisLen*2)
		lenBuff = binary.BigEndian.AppendUint16(lenBuff, uint16(thisLen))
		for _, tsfLen := range tsfLens {
			lenBuff = binary.BigEndian.AppendUint16(lenBuff, tsfLen)
		}
		ret = append(ret, append(lenBuff, buff...))
	}
	return ret, nil
}

func DefaultDecodeTransfers(b []byte) ([]*model.Transfer, error) {
	//fmt.Println(len(b), b)
	length := int(binary.BigEndian.Uint16(b[:2]))
	tsfLens := make([]int, length)
	for i := 0; i < length; i++ {
		tsLen := binary.BigEndian.Uint16(b[2+i*2 : 2+i*2+2])
		tsfLens[i] = int(tsLen)
	}
	ret := make([]*model.Transfer, length)
	formerLen := 2 + length*2
	for i := 0; i < length; i++ {
		//fmt.Println(len(b[formerLen:formerLen+tsfLens[i]]), b[formerLen:formerLen+tsfLens[i]])
		tsf, _, err := DecodeTransferMsgp(b[formerLen : formerLen+tsfLens[i]])
		if err != nil {
			return nil, fmt.Errorf("decode tsfs[%d] failed: %s", i, err.Error())
		}
		ret[i] = tsf
		formerLen += tsfLens[i]
	}
	return ret, nil
}
