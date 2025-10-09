package semantic

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"transfer-graph/model"
	"transfer-graph/pricedb"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	fgraph "github.com/yourbasic/graph"
	"gonum.org/v1/gonum/mat"
)

var superNodeOutDegreeLimit = model.SuperNodeOutDegreeLimitLevel4

var superTxTransferLimit = 5

// an virtual edge with value < this won't be considered
var DollarActivateThreshold = uint64(10 * math.Pow10(model.DollarDeciamls))

type txGraph struct {
	txFrom     string                 // tx.From address as string
	edges      map[uint64][][2]uint64 // from -> {{to, value}}
	addressMap map[string]uint64      // address -> id
	rMap       []string               // id -> address
	tokenSet   []common.Address
}

func newTxGraph(
	tx *model.Tx,
	tss []*model.Transfer,
	pCache *pricedb.PriceCache,
) *txGraph {

	tokenSet := make(map[string]struct{})
	var addressMapCounter uint64 = 0
	addressMap := make(map[string]uint64)
	edgeMap := make(map[uint64][][2]uint64)
	for _, ts := range tss {
		from := string(ts.From.Bytes())
		to := string(ts.To.Bytes())
		if _, ok := addressMap[from]; !ok {
			addressMap[from] = addressMapCounter
			edgeMap[addressMapCounter] = make([][2]uint64, 0, 1)
			addressMapCounter++
		}
		if _, ok := addressMap[to]; !ok {
			addressMap[to] = addressMapCounter
			addressMapCounter++
		}
		price := pCache.Price(tx.Block, ts.Token)
		if price == 0 {
			continue
		}
		decimals, ok := pCache.Decimals(ts.Token)
		if !ok {
			continue
		}
		value := computeValue(ts.Value, price, decimals)
		if value == 0 {
			continue
		}
		edgeMap[addressMap[from]] = append(edgeMap[addressMap[from]], [2]uint64{addressMap[to], value})
		tokenSet[string(ts.Token.Bytes())] = struct{}{}
	}
	g := &txGraph{
		txFrom:     string(tx.From.Bytes()),
		edges:      edgeMap,
		addressMap: addressMap,
	}
	g.rMap = make([]string, len(addressMap))
	for k, v := range addressMap {
		g.rMap[v] = k
	}
	g.tokenSet = make([]common.Address, 0, len(tokenSet))
	for k := range tokenSet {
		g.tokenSet = append(g.tokenSet, common.BytesToAddress([]byte(k)))
	}
	return g
}

func (g *txGraph) computeBalance() []int64 {
	balance := make([]int64, len(g.addressMap))
	for from, edges := range g.edges {
		for _, edge := range edges {
			balance[from] -= int64(edge[1])
			balance[edge[0]] += int64(edge[1])
		}
	}
	return balance
}

func (g *txGraph) computeMaxFlow() []int64 {
	maxFlow := make([]int64, len(g.addressMap))
	source, ok := g.addressMap[g.txFrom]
	if !ok {
		return maxFlow
	}
	fg := fgraph.New(len(g.addressMap))
	for from, edges := range g.edges {
		for _, edge := range edges {
			fg.AddCost(int(from), int(edge[0]), fg.Cost(int(from), int(edge[0]))+int64(edge[1]))
		}
	}
	ifg := fgraph.Sort(fg)
	for _, des := range g.addressMap {
		if des == source {
			continue
		}
		maxFlow[des], _ = fgraph.MaxFlow(ifg, int(source), int(des))
	}
	return maxFlow
}

func hasUnreachableNode(tx *model.Tx, tss []*model.Transfer, outDegreeAll map[string]int) bool {
	if len(tss) >= superTxTransferLimit {
		return true
	}
	srcReachable, _ := calculateTxClosure(tss, utils.AddrToAddrString(tx.To))
	for _, ts := range tss {
		if _, ok := srcReachable[utils.AddrToAddrString(ts.From)]; ts.From.Cmp(tx.From) == 0 || !ok {
			continue
		}
		if outDegreeAll[utils.AddrToAddrString(ts.From)] > superNodeOutDegreeLimit {
			return true
		}
	}
	return false
}

func AddWithinTx(
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	tsSlice []*model.Transfer,
	//inDegreeAll, inDegreeToken map[string]int,
	outDegreeAll, outDegreeToken map[string]int,
	tsMapByPos map[uint64][]*model.Transfer,
	pdb *pricedb.PriceDB,
	pdbParallel int,
	ctx context.Context,
) (map[string][]*model.Transfer, []*model.Transfer, error) {

	rTss := make([]*model.Transfer, 0)
	fTxs := make([]*model.Tx, 0)
	fTss := make([]*model.Transfer, 0)
	fTsMapByPos := make(map[uint64][]*model.Transfer)
	for _, txs := range txMap {
		/*
			toAddressString := txMapKey[len(txMapKey)/2:]
			if outDegreeAll[toAddressString] < superNodeOutDegreeLimit {
				continue
			}
		*/
		for _, tx := range txs {
			tss, ok := tsMapByPos[tx.Pos()]
			if !(!ok || isSemanticProcessed(tss) || (len(tss) == 1 && tss[0].Type == uint16(model.TransferTypeExternal))) && hasUnreachableNode(tx, tss, outDegreeAll) {
				fTxs = append(fTxs, tx)
				fTss = append(fTss, tss...)
				fTsMapByPos[tx.Pos()] = make([]*model.Transfer, 0, len(tss))
				// WETH patched here
				for _, ts := range tss {
					if !(ts.From.Cmp(utils.WETHAddress) == 0 || ts.To.Cmp(utils.WETHAddress) == 0) {
						fTsMapByPos[tx.Pos()] = append(fTsMapByPos[tx.Pos()], ts)
					}
				}
			} else {
				rTss = append(rTss, tss...)
			}
		}
	}
	pCache, err := pricedb.NewPriceCache(fTxs, fTss, pdb, pdbParallel, ctx)
	if err != nil {
		return nil, nil, err
	}
	defer pCache.FlashCache()
	for _, tx := range fTxs {
		txPos := tx.Pos()
		tss := fTsMapByPos[txPos]
		g := newTxGraph(tx, tss, pCache)
		if len(g.edges) == 0 || len(g.addressMap) == 0 {
			continue
		}
		balanceList := g.computeBalance()
		maxFlowList := g.computeMaxFlow()
		contribution := make([]int64, len(g.addressMap))
		for i := range balanceList {
			if balanceList[i] < 0 {
				continue
			}
			if balanceList[i] < maxFlowList[i] {
				contribution[i] = balanceList[i]
			} else {
				contribution[i] = maxFlowList[i]
			}
		}
		if tx.TxHash.Cmp(common.HexToHash("0x329fcf3060886020e19c00481602d26627175f1cf2d00189266baab46643f5e9")) == 0 {
			for _, ts := range tss {
				fmt.Println(ts.From.Hex(), ts.To.Hex(), ts.Value, ts.Token)
			}
			for i := range balanceList {
				fmt.Println(utils.AddrStringToHex(g.rMap[i]), balanceList[i], maxFlowList[i], contribution[i])
			}
		}
		var txidCounter uint16 = 0
		for id := range contribution {
			if contribution[id] < int64(DollarActivateThreshold) {
				continue
			}
			desAddress := common.BytesToAddress([]byte(g.rMap[id]))
			for _, token := range g.tokenSet {
				virtualTs := &model.Transfer{
					Pos:   txPos,
					Txid:  txidCounter,
					Type:  uint16(model.TransferVirtualTypeWithinTx),
					From:  tx.From,
					To:    desAddress,
					Token: token,
					Value: (*hexutil.Big)(big.NewInt(contribution[id])),
				}
				tsMapKey := makeTsMapKey(virtualTs.From, virtualTs.To, virtualTs.Token)
				if _, ok := tsMap[tsMapKey]; !ok {
					tsMap[tsMapKey] = make([]*model.Transfer, 0, 1)
				}
				rTss = append(rTss, virtualTs)
				/*
					tsMap[tsMapKey] = append(tsMap[tsMapKey], virtualTs)
					tsSlice = append(tsSlice, virtualTs)
					tsMapByPos[txPos] = append(tsMapByPos[txPos], virtualTs)
				*/
			}
			txidCounter++
		}
	}
	//return tsMap, tsSlice, nil

	rTsMap := make(map[string][]*model.Transfer)
	for _, ts := range rTss {
		tsMapKey := makeTsMapKey(ts.From, ts.To, ts.Token)
		if _, ok := rTsMap[tsMapKey]; !ok {
			rTsMap[tsMapKey] = make([]*model.Transfer, 0, 1)
		}
		rTsMap[tsMapKey] = append(rTsMap[tsMapKey], ts)
	}
	return rTsMap, rTss, nil
}

func calculateTxClosure(tss []*model.Transfer, txToAddress string) (map[string]struct{}, map[string]struct{}) {
	tokenSet := make(map[string]struct{})
	addressMapCounter := 0
	addressMap := make(map[string]int)
	edgeMap := make(map[int][]int)
	for _, ts := range tss {
		from := string(ts.From.Bytes())
		to := string(ts.To.Bytes())
		if _, ok := addressMap[from]; !ok {
			addressMap[from] = addressMapCounter
			edgeMap[addressMapCounter] = make([]int, 0, 1)
			addressMapCounter++
		}
		if _, ok := addressMap[to]; !ok {
			addressMap[to] = addressMapCounter
			addressMapCounter++
		}
		edgeMap[addressMap[from]] = append(edgeMap[addressMap[from]], addressMap[to])
		tokenSet[string(ts.Token.Bytes())] = struct{}{}
	}
	superNodeID, ok := addressMap[txToAddress]
	if !ok {
		return nil, nil
	}
	adjMatrix := mat.NewDense(len(addressMap), len(addressMap), nil)
	adjMatrix.Zero()
	for row, columns := range edgeMap {
		for _, column := range columns {
			adjMatrix.Set(row, column, 1)
		}
	}
	srcRowShadow := adjMatrix.RawRowView(superNodeID)
	srcRow := make([]float64, len(addressMap))
	copy(srcRow, srcRowShadow)
	srcRowVec := mat.NewDense(1, len(addressMap), srcRow)
	closure := make(map[int]struct{})
	for i := 0; i < len(addressMap); i++ {
		endFlag := true
		for desID, conn := range srcRowVec.RawRowView(0) {
			if _, ok := closure[desID]; !ok && conn >= 1 {
				endFlag = false
				closure[desID] = struct{}{}
			}
		}
		if endFlag {
			break
		}
		var t mat.Dense
		t.Mul(srcRowVec, adjMatrix)
		srcRowVec.CloneFrom(&t)
	}
	rMap := make([]string, len(addressMap))
	for k, v := range addressMap {
		rMap[v] = k
	}
	closureInAddr := make(map[string]struct{}, len(closure))
	for id := range closure {
		closureInAddr[rMap[id]] = struct{}{}
	}
	return closureInAddr, tokenSet
}
