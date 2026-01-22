package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/fbsobreira/gotron-sdk/pkg/common"
)

const (
	BlockSpan       = uint64(100000)
	TokenHashLength = 8
	DollarDecimals  = 6
)

var (
	SearchOutDegreeLimit = GetConfigOutDegreeLimit()
	SearchDepth          = GetConfigSearchDepth()
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
	return false
	//return tsType >= uint16(TransferVirtualTypeSwap)
}

var (
	SubgraphPrefix  = []byte{'G'}
	TransferPrefix  = []byte{'S'}
	TokenListPrefix = []byte{'T'}
	TxMetaPrefix    = []byte{'E'}
	NodeMetaPrefix  = []byte{'M'}

	MetadataKey = []byte("METADATA")

	EmptyAddress     = Address{}
	CompositeAddress = Address{}
	EtherAddress     = HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
)

var (
	SupportTokenList []Address
	SupportTokenMap  map[string]struct{} // string(Address.Bytes())
)

func IsSupportToken(token Address) bool {
	_, exists := SupportTokenMap[string(token.Bytes())]
	return exists
}

// set by file formatted as r"([addr]\n)*[addr]"
func SetSupportTokens(dataDir, fileName string) {
	file, err := os.ReadFile(path.Join(dataDir, fileName))
	if err != nil {
		panic(err.Error())
	}
	tokens := strings.Split(string(file), "\n")
	SupportTokenList = make([]Address, len(tokens))
	SupportTokenMap = make(map[string]struct{}, len(tokens))
	for i, token := range tokens {
		SupportTokenList[i] = HexToAddress(token)
		SupportTokenMap[string(SupportTokenList[i].Bytes())] = struct{}{}
	}
}

type Transfer struct {
	Pos       uint64       `json:"pos"` // format: block << 16 | index
	Txid      uint16       `json:"txid"`
	Type      uint16       `json:"type"`
	From      Address      `json:"from"`
	To        Address      `json:"to"`
	Token     Address      `json:"token"`
	Value     *hexutil.Big `json:"value"`
	Timestamp string       `json:"timestamp"` // RFC3339 format
	TxHash    common.Hash  `json:"txHash"`

	Extras map[string]interface{} `json:"extras"`
}

func MakeTransferPos(block uint64, index uint16) uint64 {
	return (block << 16) | uint64(index)
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

func GetTokenHash(token Address) []byte {
	buff := crypto.Keccak256(token.Bytes())
	return buff[0:TokenHashLength]
}

func GetTokensHash(tokens []Address) []byte {
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

func GetNativeTokenHash(isTx bool) []byte {
	if isTx {
		return []byte{'E', 'V', 'M', 'E', 'V', 'M', 'T', 'X'}
	} else {
		return []byte{'E', 'V', 'M', 'E', 'V', 'M', 'T', 'S'}
	}
}

func MakeGID(block uint64, token Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = append(buff, GetBlockID(block)...)
	buff = append(buff, GetTokenHash(token)...)
	return append(SubgraphPrefix, buff...)
}

func MakeSID(block uint64, token Address, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = append(buff, GetBlockID(block)...)
	buff = append(buff, GetTokenHash(token)...)
	buff = binary.BigEndian.AppendUint32(buff, srcID)
	buff = binary.BigEndian.AppendUint32(buff, desID)
	return append(TransferPrefix, buff...)
}

func MakeGIDWithBlockID(blockID uint16, token Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokenHash(token)...)
	return append(SubgraphPrefix, buff...)
}

func MakeCompositeGIDWithBlockID(blockID uint16, tokens []Address) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokensHash(tokens)...)
	return append(SubgraphPrefix, buff...)
}

func MakeSIDWithBlockID(blockID uint16, token Address, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetTokenHash(token)...)
	buff = binary.BigEndian.AppendUint32(buff, srcID)
	buff = binary.BigEndian.AppendUint32(buff, desID)
	return append(TransferPrefix, buff...)
}

func MakeNativeTokenGIDWithBlockID(blockID uint16) []byte {
	buff := make([]byte, 0, 2+TokenHashLength)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetNativeTokenHash(true)...)
	return append(SubgraphPrefix, buff...)
}

func MakeNativeTokenSIDWithBlockID(blockID uint16, isTx bool, srcID, desID uint32) []byte {
	buff := make([]byte, 0, 2+TokenHashLength+8)
	buff = binary.BigEndian.AppendUint16(buff, blockID)
	buff = append(buff, GetNativeTokenHash(isTx)...)
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

func MakeGIDWithBlockIDPack(blockID uint16, token Address) []byte {
	isNativeToken := IsNativeToken(token)
	if isNativeToken {
		return MakeNativeTokenGIDWithBlockID(blockID)
	} else {
		return MakeGIDWithBlockID(blockID, token)
	}
}

func MakeSIDWithBlockIDPack(blockID uint16, token Address, srcID, desID uint32, isTx bool) []byte {
	if IsNativeToken(token) {
		return MakeNativeTokenSIDWithBlockID(blockID, isTx, srcID, desID)
	} else {
		return MakeSIDWithBlockID(blockID, token, srcID, desID)
	}
}

func MakeGIDPrefixWithBlockID(blockID uint16) []byte {
	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff, blockID)
	return append(SubgraphPrefix, buff...)
}

func SIDTypeIsNativeTokenTx(sid []byte) bool {
	if len(sid) < 3+TokenHashLength || !bytes.Equal(sid[3:3+TokenHashLength], GetNativeTokenHash(true)) {
		return false
	}
	return true
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

type Tx struct {
	Block      uint64       `json:"block"`
	Time       string       `json:"time"`
	Index      uint16       `json:"index"`
	TxHash     common.Hash  `json:"txHash"`
	From       Address      `json:"from"`
	To         Address      `json:"to"`
	IsCreation bool         `json:"isCreation"`
	Value      *hexutil.Big `json:"value"`
	Fee        *hexutil.Big `json:"fee,omitempty"`
	Func       string       `json:"func"`

	// extra information
	Param hexutil.Bytes `json:"param,omitempty"`
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
	AdditionalComposition [][]Address
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

func (cc *CompositeConfiguration) SetAdditionalComposition(a [][]Address) {
	cc.AdditionalComposition = a
}
