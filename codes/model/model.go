package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	cmath "github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
)

type Chain string

const (
	Ethereum = Chain("Ethereum")
	BSC      = Chain("BSC")
)

const (
	BlockSpan                     = uint64(100000)
	TokenHashLength               = 8
	SuperNodeOutDegreeLimitLevel1 = 20
	SuperNodeOutDegreeLimitLevel2 = 30
	SuperNodeOutDegreeLimitLevel3 = 50
	SuperNodeOutDegreeLimitLevel4 = 100
	SuperNodeOutDegreeLimitLevel5 = 200
	SuperNodeOutDegreeLimitLevel6 = 500
	MaxHopLimit                   = uint8(20)
	DollarDeciamls                = 6
)

type TransferType uint16

const (
	TransferTypeExternal TransferType = iota + 1
	TransferTypeInternal
	TransferTypeEvent
	TransferTypeWETHDeposit
	TransferTypeWETHWithdraw
	TransferTypeERC1155Single
	TransferTypeERC1155Batch

	TransferVirtualTypeSwap
	TransferVirtualTypeWithinTx
)

func IsVirualTransfer(tsType uint16) bool {
	return tsType >= uint16(TransferVirtualTypeSwap)
}

var (
	SubgraphPrefix  = []byte{'G'}
	TransferPrefix  = []byte{'S'}
	TokenListPrefix = []byte{'T'}
	TxMetaPrefix    = []byte{'E'}
	NodeMetaPrefix  = []byte{'M'}

	MetadataKey      = []byte("METADATA")
	EtherAddress     = common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	EmptyAddress     = common.Address{}
	CompositeAddress = common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	LastAddress      = common.HexToAddress("0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF")
)

var (
	SupportTokenList []common.Address
	SupportTokenMap  map[string]struct{} // string(Address.Bytes())
)

// set by file formatted as r"([addr]\n)*[addr]"
func SetSupportTokens(dataDir, fileName string) {
	file, err := os.ReadFile(path.Join(dataDir, fileName))
	if err != nil {
		panic(err.Error())
	}
	tokens := strings.Split(string(file), "\n")
	SupportTokenList = make([]common.Address, len(tokens))
	SupportTokenMap = make(map[string]struct{}, len(tokens))
	for i, token := range tokens {
		SupportTokenList[i] = common.HexToAddress(token)
		SupportTokenMap[string(SupportTokenList[i].Bytes())] = struct{}{}
	}
}

type Transfer struct {
	Pos   uint64         `json:"pos"` // format: block << 16 | index
	Txid  uint16         `json:"txid"`
	Type  uint16         `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Token common.Address `json:"token"`
	Value *hexutil.Big   `json:"value"`

	Extras map[string]interface{} `json:"extras"`
}

func (t *Transfer) Block() uint64 {
	return t.Pos >> 16
}

func (t *Transfer) Index() uint16 {
	return uint16(t.Pos & 0xFFF)
}

func GetBlockID(block uint64) []byte {
	buff := make([]byte, 2)
	blockID := uint16(block / BlockSpan)
	binary.BigEndian.PutUint16(buff, blockID)
	return buff
}

func GetTokenHash(token common.Address) []byte {
	buff := crypto.Keccak256(token.Bytes())
	return buff[0:TokenHashLength]
}

func GetTokensHash(tokens []common.Address) []byte {
	if len(tokens) == 1 {
		return nil
	}
	tokenBytes := make([][]byte, len(tokens))
	for i, token := range tokens {
		tokenBytes[i] = token.Bytes()
	}
	sort.Slice(tokenBytes, func(i, j int) bool {
		return (strings.Compare(string(tokenBytes[i]), string(tokenBytes[j])) == -1)
	})
	buff := make([]byte, 0, len(tokens)*len(tokenBytes[0]))
	for _, tokenByte := range tokenBytes {
		buff = append(buff, tokenByte...)
	}
	buff = crypto.Keccak256(buff)
	return buff[0:TokenHashLength]
}

func GetETHHash(isTx bool) []byte {
	if isTx {
		return []byte{'E', 'T', 'H', 'E', 'T', 'H', 'T', 'X'}
	} else {
		return []byte{'E', 'T', 'H', 'E', 'T', 'H', 'T', 'S'}
	}
}

func MakeGID(block uint64, token common.Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = append(buff, GetBlockID(block)...)
	buff = append(buff, GetTokenHash(token)...)
	return append(SubgraphPrefix, buff...)
}

func MakeSID(block uint64, token common.Address, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = append(buff, GetBlockID(block)...)
	buff = append(buff, GetTokenHash(token)...)
	buff = binary.BigEndian.AppendUint32(buff, srcID)
	buff = binary.BigEndian.AppendUint32(buff, desID)
	return append(TransferPrefix, buff...)
}

func MakeGIDWithBlockID(blockID uint16, token common.Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokenHash(token)...)
	return append(SubgraphPrefix, buff...)
}

func MakeCompositeGIDWithBlockID(blockID uint16, tokens []common.Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokensHash(tokens)...)
	return append(SubgraphPrefix, buff...)
}

func MakeSIDWithBlockID(blockID uint16, token common.Address, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokenHash(token)...)
	buff = binary.BigEndian.AppendUint32(buff, srcID)
	buff = binary.BigEndian.AppendUint32(buff, desID)
	return append(TransferPrefix, buff...)
}

func MakeETHGIDWithBlockID(blockID uint16) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetETHHash(true)...)
	return append(SubgraphPrefix, buff...)
}

func MakeETHSIDWithBlockID(blockID uint16, isTx bool, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetETHHash(isTx)...)
	buff = binary.BigEndian.AppendUint32(buff, srcID)
	buff = binary.BigEndian.AppendUint32(buff, desID)
	return append(TransferPrefix, buff...)
}

func MakeSIDPlural(SID []byte, index uint16) []byte {
	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff, index)
	return append(SID, buff...)
}

func GetSIDPluralSuffix(index uint16) []byte {
	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff, index)
	return buff
}

func MakeGIDWithBlockIDPack(blockID uint16, token common.Address) []byte {
	isETH := (token.Cmp(EtherAddress) == 0)
	if isETH {
		return MakeETHGIDWithBlockID(blockID)
	} else {
		return MakeGIDWithBlockID(blockID, token)
	}
}

func MakeSIDWithBlockIDPack(blockID uint16, token common.Address, srcID, desID uint32, isTx bool) []byte {
	isETH := (token.Cmp(EtherAddress) == 0)
	if isETH {
		return MakeETHSIDWithBlockID(blockID, isTx, srcID, desID)
	} else {
		return MakeSIDWithBlockID(blockID, token, srcID, desID)
	}
}

func MakeGIDPrefixWithBlockID(blockID uint16) []byte {
	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff, blockID)
	return append(SubgraphPrefix, buff...)
}

func SIDTypeIsETHTx(sid []byte) bool {
	if len(sid) < 3+TokenHashLength || !bytes.Equal(sid[3:3+TokenHashLength], GetETHHash(true)) {
		return false
	}
	return true
}

func TokenIsETH(token common.Address) bool {
	return token.Cmp(EtherAddress) == 0
}

func GetLGGID() ([]byte, []byte) {
	buffl := make([]byte, 0, 2+TokenHashLength)
	buffl = binary.BigEndian.AppendUint16(buffl, 0)
	buffl = append(buffl, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
	buffg := make([]byte, 0, 2+TokenHashLength)
	buffg = binary.BigEndian.AppendUint16(buffg, math.MaxUint16)
	buffg = append(buffg, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	return append(SubgraphPrefix, buffl...), append(SubgraphPrefix, buffg...)
}

type Fee struct {
	GasUsed   uint64       `json:"gasUsed"`
	GasPrice  *hexutil.Big `json:"gasPrice"`
	BaseFee   *hexutil.Big `json:"baseFee,omitempty"`
	GasFeeCap *hexutil.Big `json:"gasFeeCap,omitempty"`
	GasTipCap *hexutil.Big `json:"gasTipCap,omitempty"`
}

type Tx struct {
	Block      uint64         `json:"block"`
	Time       string         `json:"time"`
	Index      uint16         `json:"index"`
	TxHash     common.Hash    `json:"txHash"`
	From       common.Address `json:"from"`
	To         common.Address `json:"to"`
	IsCreation bool           `json:"isCreation"`
	Value      *hexutil.Big   `json:"value"`
	Fee        *hexutil.Big   `json:"fee,omitempty"`
	Func       string         `json:"func"`

	// extra information
	Param   hexutil.Bytes `json:"param,omitempty"`
	FeeInfo *Fee          `json:"feeInfo,omitempty"`
}

func (tx *Tx) Pos() uint64 {
	return tx.Block<<16 | uint64(tx.Index)
}

func (tx *Tx) GetTime() time.Time {
	t, err := time.Parse(time.RFC3339, tx.Time)
	if err != nil {
		panic(fmt.Errorf("parse time failed: %s", err.Error()))
	}
	return t
}

func (tx *Tx) GetTimeU64() uint64 {
	t, err := time.Parse(time.RFC3339, tx.Time)
	if err != nil {
		panic(fmt.Errorf("parse time failed: %s", err.Error()))
	}
	return uint64(t.UTC().Unix())
}

func (tx *Tx) GetFee(chain Chain) *big.Int {
	var fee *big.Int

	baseFee := (*big.Int)(tx.FeeInfo.BaseFee)
	gasTipCap := (*big.Int)(tx.FeeInfo.GasTipCap)
	gasFeeCap := (*big.Int)(tx.FeeInfo.GasFeeCap)

	if gasFeeCap != nil && gasFeeCap.Cmp(common.Big0) > 0 {
		effectiveTip := cmath.BigMin(gasTipCap, big.NewInt(0).Sub(gasFeeCap, baseFee))
		base := big.NewInt(0).Mul(baseFee, big.NewInt(int64(tx.FeeInfo.GasUsed)))
		extra := big.NewInt(0).Mul(effectiveTip, big.NewInt(int64(tx.FeeInfo.GasUsed)))
		fee = big.NewInt(0).Add(base, extra)
	} else {
		fee = big.NewInt(0).Mul((*big.Int)(tx.FeeInfo.GasPrice), big.NewInt(int64(tx.FeeInfo.GasUsed)))
	}
	tx.Fee = (*hexutil.Big)(fee)
	return fee
}

// RawMessage is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can
// be used to delay JSON decoding or precompute a JSON encoding.
type RawMessage []byte

// MarshalJSON returns m as the JSON encoding of m.
func (m RawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return m, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (m *RawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	*m = append((*m)[0:0], data...)
	return nil
}

type Metadata map[string]RawMessage

func TxCalldataToMetadata(calldata []byte) Metadata {
	return Metadata(map[string]RawMessage{
		"calldata": RawMessage("\"" + hexutil.Encode(calldata) + "\""),
	})
}

func MakeTxMetadataKey(txid []byte) []byte {
	return append(TxMetaPrefix, txid...)
}

func MakeNodeMetadataKey(txid []byte) []byte {
	return append(NodeMetaPrefix, txid...)
}

type CompositeConfiguration struct {
	PrevailingNumber      int
	PrevailingComposition [][]int
	AdditionalComposition [][]common.Address
}

func DefaultCompositeConfiguration() *CompositeConfiguration {
	ret := &CompositeConfiguration{
		PrevailingNumber:      0,
		PrevailingComposition: nil,
		AdditionalComposition: nil,
	}
	return ret
}

func EmptyCompositeConfiguration() *CompositeConfiguration {
	return &CompositeConfiguration{
		PrevailingNumber:      0,
		PrevailingComposition: nil,
		AdditionalComposition: nil,
	}
}

func (cc *CompositeConfiguration) IsEmpty() bool {
	return cc.PrevailingNumber == 0 && cc.AdditionalComposition == nil
}

func (cc *CompositeConfiguration) SetPrevailingNumber(n int) {
	cc.PrevailingNumber = n
}

func (cc *CompositeConfiguration) SetPrevailingComposition(c [][]int) {
	cc.PrevailingComposition = c
}

func (cc *CompositeConfiguration) SetAdditionalComposition(a [][]common.Address) {
	cc.AdditionalComposition = a
}
