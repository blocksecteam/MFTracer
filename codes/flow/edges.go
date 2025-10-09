package flow

import (
	"context"
	"math"
	"sort"
	"transfer-graph/model"
	"transfer-graph/pricedb"
	"transfer-graph/utils"
)

// WETH patched here
func flowTx(tx *model.Tx, price float64, decimals uint8) *FlowDigest {
	if tx.From.Cmp(utils.WETHAddress) == 0 || tx.To.Cmp(utils.WETHAddress) == 0 {
		return nil
	}
	if tx.Value.ToInt().IsUint64() && tx.Value.ToInt().Uint64() == 0 {
		return nil
	}
	return &FlowDigest{
		From:  string(tx.From.Bytes()),
		To:    string(tx.To.Bytes()),
		Value: ComputeTokenValue(tx.Value, price, decimals),
	}
}

// WETH patched here
func flowTs(ts *model.Transfer, price float64, decimals uint8) *FlowDigest {
	if ts.From.Cmp(utils.WETHAddress) == 0 || ts.To.Cmp(utils.WETHAddress) == 0 {
		return nil
	}
	if ts.Value.ToInt().IsUint64() && ts.Value.ToInt().Uint64() == 0 {
		return nil
	}
	if model.IsVirualTransfer(ts.Type) {
		return &FlowDigest{
			From:  string(ts.From.Bytes()),
			To:    string(ts.To.Bytes()),
			Value: ComputeDollarValue(ts.Value),
		}
	} else {
		return &FlowDigest{
			From:  string(ts.From.Bytes()),
			To:    string(ts.To.Bytes()),
			Value: ComputeTokenValue(ts.Value, price, decimals),
		}
	}
}

type EgdesSortedByTime struct {
	Txs        []*model.Tx
	Tss        [][]*model.Transfer
	Length     int
	Index      int
	PriceCache *pricedb.PriceCache
	reverse    bool
}

func NewEgdesSortedByTime(txs []*model.Tx, tss []*model.Transfer, reverse bool, pdb *pricedb.PriceDB, pdbParallel int, pdbCtx context.Context) *EgdesSortedByTime {
	var err error
	allPoses := make(map[uint64]struct{}, len(txs))
	txMapByPos := make(map[uint64]*model.Tx, len(txs))
	for _, tx := range txs {
		txMapByPos[tx.Pos()] = tx
		allPoses[tx.Pos()] = struct{}{}
	}
	tsMapByPos := make(map[uint64][]*model.Transfer)
	for _, ts := range tss {
		if _, ok := tsMapByPos[ts.Pos]; !ok {
			tsMapByPos[ts.Pos] = make([]*model.Transfer, 0, 1)
			allPoses[ts.Pos] = struct{}{}
		}
		tsMapByPos[ts.Pos] = append(tsMapByPos[ts.Pos], ts)
	}
	posAscend := make([]uint64, 0, len(allPoses))
	for pos := range allPoses {
		posAscend = append(posAscend, pos)
	}
	sort.Slice(posAscend, func(i, j int) bool {
		return posAscend[i] < posAscend[j]
	})
	ret := &EgdesSortedByTime{
		Txs:    make([]*model.Tx, len(posAscend)),
		Tss:    make([][]*model.Transfer, len(posAscend)),
		Length: len(posAscend),
		Index:  0,
	}
	for i, pos := range posAscend {
		if tx, ok := txMapByPos[pos]; ok {
			ret.Txs[i] = tx
		} else {
			ret.Txs[i] = nil
		}
		if tsSlice, ok := tsMapByPos[pos]; ok {
			sort.Slice(tsSlice, func(i, j int) bool {
				if !model.IsVirualTransfer(tsSlice[i].Type) && model.IsVirualTransfer(tsSlice[j].Type) {
					return false
				} else if model.IsVirualTransfer(tsSlice[i].Type) && !model.IsVirualTransfer(tsSlice[j].Type) {
					return true
				} else {
					return tsSlice[i].Txid < tsSlice[j].Txid
				}
			})
			ret.Tss[i] = tsSlice
		} else {
			ret.Tss[i] = nil
		}
	}
	if reverse {
		for i := 0; i < len(ret.Txs)/2; i++ {
			ret.Txs[i], ret.Txs[len(ret.Txs)-1-i] = ret.Txs[len(ret.Txs)-1-i], ret.Txs[i]
		}
		for i := 0; i < len(ret.Tss)/2; i++ {
			ret.Tss[i], ret.Tss[len(ret.Tss)-1-i] = ret.Tss[len(ret.Tss)-1-i], ret.Tss[i]
		}
	}
	ret.reverse = reverse
	ret.PriceCache, err = pricedb.NewPriceCache(txs, tss, pdb, pdbParallel, pdbCtx)
	if err != nil {
		panic(err)
	}
	return ret
}

func (se *EgdesSortedByTime) At(i int) (*model.Tx, []*model.Transfer) {
	return se.Txs[i], se.Tss[i]
}

func (se *EgdesSortedByTime) flowAt(i int, activity flowActivity) []*FlowDigest {
	tx, tss := se.At(i)
	//fmt.Println(tx.From, tx.To, tx.TxHash)
	/*
		if tss != nil && model.IsVirualTransfer(tss[0].Type) {
			patchFlag := false
			vcount := 0
			for _, ts := range tss {
				if !model.IsVirualTransfer(ts.Type) {
					break
				}
				if ts.To.Cmp(utils.WETHAddress) == 0 {
					patchFlag = true
				}
				vcount++
			}
			if patchFlag {
				tss = tss[vcount:]
				if len(tss) == 0 {
					tss = nil
				}
			}
		}*/
	ret := make([]*FlowDigest, 0)
	if tss != nil && model.IsVirualTransfer(tss[0].Type) {
		var thisTxid uint32 = math.MaxUint16 + 1
		for j, ts := range tss {
			if !model.IsVirualTransfer(ts.Type) {
				break
			}
			if uint32(ts.Txid) == thisTxid || !activity.check(ts.From) {
				continue
			}
			if edgeDi := flowTs(ts, 0, 0); edgeDi != nil {
				activity.add(ts.To)
				edgeDi.EdgePointer = makeESBTPointer(i, j, false)
				ret = append(ret, edgeDi)
				thisTxid = uint32(ts.Txid)
			}
		}
	} else {
		if tx != nil && activity.check(tx.From) && (tss == nil || tss != nil && tss[0].Type != uint16(model.TransferTypeExternal)) {
			price := se.PriceCache.Price(tx.Block, model.EtherAddress)
			decimals, ok := se.PriceCache.Decimals(model.EtherAddress)
			if price != 0 && ok {
				if edgeDi := flowTx(tx, price, decimals); edgeDi != nil {
					activity.add(tx.To)
					edgeDi.EdgePointer = makeESBTPointer(i, 0, true)
					ret = append(ret, edgeDi)
				}
			}
		}
		for j, ts := range tss {
			if !activity.check(ts.From) {
				continue
			}
			price := se.PriceCache.Price(ts.Block(), ts.Token)
			decimals, ok := se.PriceCache.Decimals(ts.Token)
			if price == 0 || !ok {
				continue
			}
			if edgeDi := flowTs(ts, price, decimals); edgeDi != nil {
				activity.add(ts.To)
				edgeDi.EdgePointer = makeESBTPointer(i, j, false)
				ret = append(ret, edgeDi)
			}
		}
	}
	if se.reverse {
		for i := range ret {
			ret[i].From, ret[i].To = ret[i].To, ret[i].From
		}
	}
	return ret
}

func (se *EgdesSortedByTime) flow(activity flowActivity) []*FlowDigest {
	if se.Index >= se.Length {
		return nil
	}
	se.Index++
	return se.flowAt(se.Index-1, activity)
}

func (se *EgdesSortedByTime) Finished() bool {
	return se.Index >= se.Length
}

func makeESBTPointer(index int, tsIndex int, isTx bool) uint64 {
	if isTx {
		return uint64(uint32(index)) << 32
	} else {
		return (uint64(uint32(index)) << 32) | uint64(uint32(tsIndex+1))
	}
}

func (se *EgdesSortedByTime) AtPointer(pointer uint64) (*model.Tx, *model.Transfer) {
	index := pointer >> 32
	subIndex := uint32(pointer)
	if subIndex == 0 {
		return se.Txs[index], nil
	} else {
		return nil, se.Tss[index][subIndex-1]
	}
}

func (se *EgdesSortedByTime) AtPointers(pointers []uint64) ([]*model.Tx, []*model.Transfer) {
	txs := make([]*model.Tx, 0)
	tss := make([]*model.Transfer, 0, len(pointers))
	for _, pointer := range pointers {
		tx, ts := se.AtPointer(pointer)
		if tx != nil {
			txs = append(txs, tx)
		} else {
			tss = append(tss, ts)
		}
	}
	return txs, tss
}

func (se *EgdesSortedByTime) Free() {
	se.Txs = nil
	se.Tss = nil
	se.PriceCache.Free()
}
