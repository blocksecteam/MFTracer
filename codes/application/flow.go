package application

import (
	"fmt"
	"math/big"
	"sort"
	"transfer-graph/model"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/common"
)

type SortedEgdes struct {
	Txs     []*model.Tx
	Tss     []*model.Transfer
	Indices []int
	Length  int
}

func SortEgdesByTimestamp(txs []*model.Tx, tss []*model.Transfer) *SortedEgdes {
	indices := make([]int, len(txs)+len(tss))
	for i := 0; i < len(txs)+len(tss); i++ {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		i = indices[i]
		j = indices[j]
		if i < len(txs) && j < len(txs) {
			return txs[i].Pos() < txs[j].Pos()
		} else if i < len(txs) && j >= len(txs) {
			return txs[i].Pos() <= tss[j-len(txs)].Pos
		} else if i >= len(txs) && j < len(txs) {
			return tss[i-len(txs)].Pos < txs[j].Pos()
		} else {
			i = i - len(txs)
			j = j - len(txs)
			/*
				if tss[i].Pos < tss[j].Pos {
					return true
				} else if tss[i].Pos > tss[j].Pos {
					return false
				} else {
					if tss[i].Type <= uint16(model.TransferTypeInternal) && tss[j].Type <= uint16(model.TransferTypeInternal) {
						return tss[i].Type < tss[j].Type || tss[i].Type == tss[j].Type && tss[i].Txid < tss[j].Txid
					} else if tss[i].Type <= uint16(model.TransferTypeInternal) && tss[j].Type > uint16(model.TransferTypeInternal) {
						return true
					} else if tss[i].Type > uint16(model.TransferTypeInternal) && tss[j].Type <= uint16(model.TransferTypeInternal) {
						return false
					} else {
						return tss[i].Txid < tss[j].Txid
					}
				}
			*/
			if tss[i].Pos < tss[j].Pos || tss[i].Pos == tss[j].Pos && tss[i].Txid < tss[j].Txid {
				return true
			} else {
				return false
			}
		}
	})
	return &SortedEgdes{
		Txs:     txs,
		Tss:     tss,
		Indices: indices,
		Length:  len(indices),
	}
}

type EdgeDigest struct {
	From  string
	To    string
	Token common.Address
	Value *big.Int
	Type  model.TransferType
}

func (se *SortedEgdes) At(i int) (*model.Tx, *model.Transfer, bool) {
	i = se.Indices[i]
	if i < len(se.Txs) {
		return se.Txs[i], nil, true
	} else {
		return nil, se.Tss[i-len(se.Txs)], false
	}
}

func (se *SortedEgdes) Digest(i int) *EdgeDigest {
	tx, ts, isTx := se.At(i)
	if isTx {
		return &EdgeDigest{
			From:  string(tx.From.Bytes()),
			To:    string(tx.To.Bytes()),
			Token: model.EtherAddress,
			Value: tx.Value.ToInt(),
			Type:  0,
		}
	} else {
		return &EdgeDigest{
			From:  string(ts.From.Bytes()),
			To:    string(ts.To.Bytes()),
			Token: ts.Token,
			Value: ts.Value.ToInt(),
			Type:  model.TransferType(ts.Type),
		}
	}
}

func tokenToUSD(token common.Address, value *big.Int) uint64 {
	return utils.PriceByUSDToInt_Temp(token, value)
}

type TimeFlowGraph struct {
	balances map[string][2]uint64
	flowStep int
	src      map[string]struct{}
	des      map[string]struct{}
	edges    *SortedEgdes
	edgeIdx  int
}

func NewTimeFlowGraph(src, des []common.Address, txs []*model.Tx, tss []*model.Transfer) *TimeFlowGraph {
	ret := &TimeFlowGraph{
		balances: make(map[string][2]uint64),
		flowStep: 0,
		src:      make(map[string]struct{}, len(src)),
		des:      make(map[string]struct{}, len(des)),
		edges:    SortEgdesByTimestamp(txs, tss),
		edgeIdx:  0,
	}
	for i := range src {
		srcStr := string(src[i].Bytes())
		ret.src[srcStr] = struct{}{}
		ret.balances[srcStr] = [2]uint64{0, 0}
	}
	for i := range des {
		ret.des[string(des[i].Bytes())] = struct{}{}
	}
	return ret
}

func NewTimeFlowGraphByInherit(p *TimeFlowGraph, txs []*model.Tx, tss []*model.Transfer) *TimeFlowGraph {
	ret := &TimeFlowGraph{
		balances: make(map[string][2]uint64, len(p.balances)),
		flowStep: 0,
		src:      make(map[string]struct{}, len(p.src)),
		des:      make(map[string]struct{}, len(p.des)),
		edges:    SortEgdesByTimestamp(txs, tss),
		edgeIdx:  0,
	}
	for k, v := range p.balances {
		ret.balances[k] = v
	}
	for k := range p.src {
		ret.src[k] = struct{}{}
	}
	for k := range p.des {
		ret.des[k] = struct{}{}
	}
	return ret
}

func (fg *TimeFlowGraph) EvolveByStep(step int) {
	for i := 0; i < step; i++ {
		fmt.Println("[Debug] {EvolveByStep}", fg.edgeIdx, fg.edges.Length)
		for ; fg.edgeIdx < fg.edges.Length; fg.edgeIdx++ {
			edge := fg.edges.Digest(fg.edgeIdx)
			if edge.Type == model.TransferTypeExternal || edge.Type > model.TransferTypeWETHWithdraw {
				continue
			}
			var sremain uint64
			var sbalance [2]uint64
			var ok bool
			if sbalance, ok = fg.balances[edge.From]; !ok {
				continue
			} else {
				sremain = sbalance[0] - sbalance[1]
			}
			moneyShift := tokenToUSD(edge.Token, edge.Value)
			if _, ok := fg.src[edge.From]; moneyShift > sremain && !ok {
				moneyShift = sremain
			}
			fg.balances[edge.From] = [2]uint64{sbalance[0], sbalance[1] + moneyShift}
			if dbalance, ok := fg.balances[edge.To]; !ok {
				fg.balances[edge.To] = [2]uint64{moneyShift, 0}
			} else {
				fg.balances[edge.To] = [2]uint64{dbalance[0] + moneyShift, dbalance[1]}
			}
			fg.flowStep++
			fg.edgeIdx++
			break
		}
		if fg.edgeIdx == fg.edges.Length {
			break
		}
	}
}

func (fg *TimeFlowGraph) EvolveToEnd() {
	for ; fg.edgeIdx < fg.edges.Length; fg.edgeIdx++ {
		edge := fg.edges.Digest(fg.edgeIdx)
		if edge.Type == model.TransferTypeExternal || edge.Type > model.TransferTypeWETHWithdraw {
			continue
		}
		var sremain uint64
		var sbalance [2]uint64
		var ok bool
		if sbalance, ok = fg.balances[edge.From]; !ok {
			continue
		} else {
			sremain = sbalance[0] - sbalance[1]
		}
		moneyShift := tokenToUSD(edge.Token, edge.Value)
		if _, ok := fg.src[edge.From]; moneyShift > sremain && !ok {
			moneyShift = sremain
		}
		fg.balances[edge.From] = [2]uint64{sbalance[0], sbalance[1] + moneyShift}
		if dbalance, ok := fg.balances[edge.To]; !ok {
			fg.balances[edge.To] = [2]uint64{moneyShift, 0}
		} else {
			fg.balances[edge.To] = [2]uint64{dbalance[0] + moneyShift, dbalance[1]}
		}
		fg.flowStep++
	}
}

func (fg *TimeFlowGraph) Balances() map[string][2]uint64 {
	return fg.balances
}

func (fg *TimeFlowGraph) BalanceOf(address common.Address) [2]uint64 {
	if balance, ok := fg.balances[string(address.Bytes())]; ok {
		return balance
	} else {
		return [2]uint64{0, 0}
	}
}

type HeavyType uint8

const (
	HeavyTypeByTotalIn HeavyType = iota
	HeavyTypeByTotalOut
	HeavyTypeByBalance
)

func (fg *TimeFlowGraph) HeavyNodes(num int, ht HeavyType) []common.Address {
	type comp struct {
		node string
		ti   uint64
		to   uint64
	}
	sortedNodes := make([]comp, 0, len(fg.balances))
	for node, v := range fg.balances {
		sortedNodes = append(sortedNodes, comp{
			node: node,
			ti:   v[0],
			to:   v[1],
		})
	}
	var less func(i, j int) bool
	switch ht {
	case HeavyTypeByTotalIn:
		less = func(i, j int) bool {
			return sortedNodes[i].ti > sortedNodes[j].ti
		}
	case HeavyTypeByTotalOut:
		less = func(i, j int) bool {
			return sortedNodes[i].to > sortedNodes[j].to
		}
	default:
		less = func(i, j int) bool {
			return int(sortedNodes[i].ti)-int(sortedNodes[i].to) > int(sortedNodes[j].ti)-int(sortedNodes[j].to)
		}
	}
	sort.Slice(sortedNodes, less)
	ret := make([]common.Address, num)
	for i := 0; i < num && i < len(sortedNodes); i++ {
		ret[i] = common.BytesToAddress([]byte(sortedNodes[i].node))
	}
	return ret
}
