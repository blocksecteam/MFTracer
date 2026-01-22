package graph

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"time"
	"transfer-graph-evm/data"
	"transfer-graph-evm/model"
	"transfer-graph-evm/preprocess"
	"transfer-graph-evm/pricedb"
	"transfer-graph-evm/utils"

	"github.com/ethereum/go-ethereum/log"
)

func SyncFromQresSimple(ctx context.Context, qres *model.QueryResult, blockID uint16, g *GraphDB, pdb *pricedb.PriceDB, pdbParallel int) error {
	startTime := time.Now()
	m := WriteMetrics{}
	ctx = context.WithValue(ctx, WriteMetricsKey, &m)

	txMap, tsMap, _, err := ConstructTxTss("", "", qres, blockID)
	if err != nil {
		utils.Logger.Error("ConstructTxTss failed", "blockID", blockID, "err", err.Error())
		return err
	}
	utils.Logger.Info("{SyncFromQresSimple} Construct S finished", "blockID", blockID, "txMap", len(txMap), "tsMap", len(tsMap))
	oDegreeAll, oDegreeToken := getOutDegrees(txMap, tsMap)
	utils.Logger.Info("{SyncFromQresSimple} Get Out Degrees finished\n")
	tsMapByPos := classTsByTx(qres.Transfers)
	utils.Logger.Info("{SyncFromQresSimple} Class Ts by Tx finished\n")
	tsMap, qres.Transfers, err = preprocess.AddWithinTx(txMap, tsMap, qres.Transfers, oDegreeAll, oDegreeToken, tsMapByPos, pdb, pdbParallel, ctx)
	if err != nil {
		utils.Logger.Error("AddWithinTx failed", "err", err.Error())
		return err
	}
	qres.Transfers, tsMap = filterTss(qres.Transfers, tsMap)

	subgraphMap, err := ConstructSubgraphs("", "", qres, blockID)
	if err != nil {
		utils.Logger.Error("ConstructSubgraphs failed", "blockID", blockID, "err", err.Error())
		return err
	}
	utils.Logger.Info("{SyncFromQresSimple} Construct G finished", "blockID", blockID, "subgraphMap", len(subgraphMap))

	greq := &GWriteRequest{
		Desc:     fmt.Sprintf("bootstrap: %d", blockID),
		Contents: make([]*model.Subgraph, 0, len(subgraphMap)),
	}
	for _, v := range subgraphMap {
		greq.Contents = append(greq.Contents, v)
	}
	sreq := &SWriteRequest{
		Desc:     fmt.Sprintf("bootstrap: %d", blockID),
		BlockID:  blockID,
		Contents: make([]*SRecord, 0),
	}
	NativeTokenAddressStr := string(model.NativeTokenAddress.Bytes())
	addrStrLength := len(NativeTokenAddressStr)
	for k, v := range txMap {
		src := k[:addrStrLength]
		des := k[addrStrLength:]
		sreq.Contents = append(sreq.Contents, &SRecord{
			Token:     model.NativeTokenAddress,
			SrcID:     subgraphMap[NativeTokenAddressStr].AddressMap[src],
			DesID:     subgraphMap[NativeTokenAddressStr].AddressMap[des],
			Transfers: nil,
			Txs:       v,
		})
	}
	for k, v := range tsMap {
		token := k[:addrStrLength]
		src := k[addrStrLength : addrStrLength*2]
		des := k[addrStrLength*2:]
		sreq.Contents = append(sreq.Contents, &SRecord{
			Token:     v[0].Token,
			SrcID:     subgraphMap[token].AddressMap[src],
			DesID:     subgraphMap[token].AddressMap[des],
			Transfers: v,
			Txs:       nil,
		})
	}

	constructTime := time.Now()
	if err := g.GWrite(ctx, greq); err != nil {
		utils.Logger.Error("SyncFromQresSimple GWrite() failed", "err", err.Error())
		return err
	}
	gwriteTime := time.Now()

	if err := g.SWrite(ctx, sreq); err != nil {
		utils.Logger.Error("SyncFromQresSimple SWrite() failed", "err", err.Error())
		return err
	}
	swriteTime := time.Now()

	utils.Logger.Info("{SyncFromQresSimple} SyncFromQresSimple() done", "blockID", blockID, "construct", constructTime.Sub(startTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))
	return nil
}

func getBlockIDValid(fileName string) uint16 {
	re := regexp.MustCompile(`(\d+)_(\d+).json.zst`)
	matches := re.FindStringSubmatch(fileName)
	if matches == nil {
		log.Crit("invalid file path (re no match): %s", fileName)
	}
	ss, es := matches[1], matches[2]
	sBlk, err := strconv.Atoi(ss)
	sBlock := uint64(sBlk)
	if err != nil {
		log.Crit("invalid file name (parse start failed), error:%s", err.Error())
	}
	eBlk, err := strconv.Atoi(es)
	eBlock := uint64(eBlk)
	if err != nil {
		log.Crit("invalid file name (parse end failed), error:%s", err.Error())
	}
	if sBlock%model.BlockSpan != 0 || eBlock%model.BlockSpan != 0 || eBlock-sBlock != model.BlockSpan {
		log.Crit("Subgraph File does not fit model.BlockSpan")
	}
	return uint16(sBlock / model.BlockSpan)
}

func getBlockID(fileName string) uint16 {
	re := regexp.MustCompile(`(\d+)_(\d+).json.zst`)
	matches := re.FindStringSubmatch(fileName)
	ss := matches[1]
	sBlk, _ := strconv.Atoi(ss)
	sBlock := uint64(sBlk)
	return uint16(sBlock / model.BlockSpan)
}

func loadQueryResult(fileName, dataDir string) (*model.QueryResult, uint16, error) {
	blockID := getBlockID(fileName)

	filePath := path.Join(dataDir, fileName)
	qres, err := data.LoadQueryResult(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("opensearch LoadQueryResult fail, file:%s, error:%s", filePath, err.Error())
	}
	return qres, blockID, nil
}

func GenerateSubgraphByTransfers(blockID uint16, token model.Address, tss []*model.Transfer) *model.Subgraph {
	return generateSubgraph(blockID, token, nil, tss)
}

func generateSubgraph(blockID uint16, token model.Address, txs []*model.Tx, tss []*model.Transfer) *model.Subgraph {
	ret := &model.Subgraph{
		BlockID: blockID,
		Token:   token,
	}
	ret.AddressMap = make(map[string]uint32)
	rows := make([]map[uint32][2]uint32, 0)
	addrCounter := uint32(0)
	for _, tx := range txs {
		//sStr := tx.From.Hex()
		sStr := string(tx.From.Bytes())
		sRow, sOk := ret.AddressMap[sStr]
		if !sOk {
			ret.AddressMap[sStr] = addrCounter
			sRow = addrCounter
			addrCounter++
			rows = append(rows, make(map[uint32][2]uint32))
		}
		//dStr := tx.To.Hex()
		dStr := string(tx.To.Bytes())
		dRow, dOk := ret.AddressMap[dStr]
		if !dOk {
			ret.AddressMap[dStr] = addrCounter
			dRow = addrCounter
			addrCounter++
			rows = append(rows, make(map[uint32][2]uint32))
		}
		timestamp := uint32(tx.Block % model.BlockSpan)
		if _, ok := rows[sRow][dRow]; !ok {
			rows[sRow][dRow] = [2]uint32{timestamp, timestamp}
			continue
		}
		if timestamp > rows[sRow][dRow][1] {
			rows[sRow][dRow] = [2]uint32{rows[sRow][dRow][0], timestamp}
		} else if timestamp < rows[sRow][dRow][0] {
			rows[sRow][dRow] = [2]uint32{timestamp, rows[sRow][dRow][1]}
		}
	}
	for _, ts := range tss {
		//sStr := ts.From.Hex()
		sStr := string(ts.From.Bytes())
		sRow, sOk := ret.AddressMap[sStr]
		if !sOk {
			ret.AddressMap[sStr] = addrCounter
			sRow = addrCounter
			addrCounter++
			rows = append(rows, make(map[uint32][2]uint32))
		}
		//dStr := ts.To.Hex()
		dStr := string(ts.To.Bytes())
		dRow, dOk := ret.AddressMap[dStr]
		if !dOk {
			ret.AddressMap[dStr] = addrCounter
			dRow = addrCounter
			addrCounter++
			rows = append(rows, make(map[uint32][2]uint32))
		}
		timestamp := uint32(ts.Block() % model.BlockSpan)
		if _, ok := rows[sRow][dRow]; !ok {
			rows[sRow][dRow] = [2]uint32{timestamp, timestamp}
			continue
		}
		if timestamp > rows[sRow][dRow][1] {
			rows[sRow][dRow] = [2]uint32{rows[sRow][dRow][0], timestamp}
		} else if timestamp < rows[sRow][dRow][0] {
			rows[sRow][dRow] = [2]uint32{timestamp, rows[sRow][dRow][1]}
		}
	}
	//fmt.Println(addrCounter, len(rows), len(txs), len(tss))
	ret.Timestamps = make([][2]uint32, 0, len(tss))
	ret.Columns = make([]uint32, 0, len(tss))
	ret.NodePtrs = make([]uint32, addrCounter+1)
	ret.NodePtrs[0] = 0
	type tempComp struct {
		column    uint32
		timestamp [2]uint32
	}
	for i, row_map := range rows {
		row := make([]tempComp, 0, len(row_map))
		for k, v := range row_map {
			row = append(row, tempComp{
				column:    k,
				timestamp: v,
			})
		}
		sort.Slice(row, func(i, j int) bool {
			return row[i].column < row[j].column
		})
		for _, v := range row {
			ret.Columns = append(ret.Columns, v.column)
			ret.Timestamps = append(ret.Timestamps, v.timestamp)
		}
		ret.NodePtrs[i+1] = ret.NodePtrs[i] + uint32(len(row))
	}
	return ret
}

func getInDegrees(txMap map[string][]*model.Tx, tsMap map[string][]*model.Transfer) (map[string]int, map[string]int) {
	allDegrees := make(map[string]int)
	tokenDegrees := make(map[string]int)
	NativeTokenAddress := string(model.NativeTokenAddress.Bytes())
	for txMapKey := range txMap {
		toAddress := txMapKey[len(txMapKey)/2:]
		if _, ok := allDegrees[toAddress]; !ok {
			allDegrees[toAddress] = 1
		} else {
			allDegrees[toAddress] += 1
		}
		tokenKey := NativeTokenAddress + toAddress
		if _, ok := tokenDegrees[tokenKey]; !ok {
			tokenDegrees[tokenKey] = 1
		} else {
			tokenDegrees[tokenKey] += 1
		}
	}
	for tsMapKey := range tsMap {
		toAddress := tsMapKey[len(tsMapKey)*2/3:]
		if _, ok := allDegrees[toAddress]; !ok {
			allDegrees[toAddress] = 1
		} else {
			allDegrees[toAddress] += 1
		}
		tokenKey := tsMapKey[:len(tsMapKey)/3] + tsMapKey[len(tsMapKey)*2/3:]
		if _, ok := tokenDegrees[tokenKey]; !ok {
			tokenDegrees[tokenKey] = 1
		} else {
			tokenDegrees[tokenKey] += 1
		}
	}
	return allDegrees, tokenDegrees
}

func getOutDegrees(txMap map[string][]*model.Tx, tsMap map[string][]*model.Transfer) (map[string]int, map[string]int) {
	allDegrees := make(map[string]int)
	tokenDegrees := make(map[string]int)
	NativeTokenAddress := string(model.NativeTokenAddress.Bytes())
	for k := range txMap {
		fromAddress := k[:len(k)/2]
		if _, ok := allDegrees[fromAddress]; !ok {
			allDegrees[fromAddress] = 1
		} else {
			allDegrees[fromAddress] += 1
		}
		tokenKey := NativeTokenAddress + fromAddress
		if _, ok := tokenDegrees[tokenKey]; !ok {
			tokenDegrees[tokenKey] = 1
		} else {
			tokenDegrees[tokenKey] += 1
		}
	}
	for k := range tsMap {
		fromAddress := k[len(k)/3 : len(k)*2/3]
		if _, ok := allDegrees[fromAddress]; !ok {
			allDegrees[fromAddress] = 1
		} else {
			allDegrees[fromAddress] += 1
		}
		tokenKey := k[:len(k)*2/3]
		if _, ok := tokenDegrees[tokenKey]; !ok {
			tokenDegrees[tokenKey] = 1
		} else {
			tokenDegrees[tokenKey] += 1
		}
	}
	return allDegrees, tokenDegrees
}

func classTsByTx(tss []*model.Transfer) map[uint64][]*model.Transfer {
	ret := make(map[uint64][]*model.Transfer)
	for _, ts := range tss {
		if _, ok := ret[ts.Pos]; !ok {
			ret[ts.Pos] = make([]*model.Transfer, 0, 1)
		}
		ret[ts.Pos] = append(ret[ts.Pos], ts)
	}
	return ret
}

func filterTss(tss []*model.Transfer, tsMap map[string][]*model.Transfer) ([]*model.Transfer, map[string][]*model.Transfer) {
	rTss := make([]*model.Transfer, 0, len(tss))
	rTsMap := make(map[string][]*model.Transfer)
	for _, ts := range tss {
		if model.IsSupportToken(ts.Token) {
			rTss = append(rTss, ts)
		}
	}
	for tsMapKey, transfers := range tsMap {
		if model.IsSupportToken(model.BytesToAddress([]byte(tsMapKey[:len(tsMapKey)/3]))) {
			rTsMap[tsMapKey] = transfers
		}
	}
	return rTss, rTsMap
}

func ConstructCompositeSubgraphs(subgraphs []*model.Subgraph, subgraphMap map[string]*model.Subgraph, compConfig *model.CompositeConfiguration, blockID uint16) (map[string]*CompositeGRecord, error) {
	if compConfig.IsEmpty() {
		return nil, nil
	}
	//ret := make([]*CompositeGRecord, 0)
	ret := make(map[string]*CompositeGRecord)
	if compConfig.PrevailingNumber > 0 {
		subgraphsSorted := make([]*model.Subgraph, len(subgraphs))
		copy(subgraphsSorted, subgraphs)
		sort.Slice(subgraphsSorted, func(i, j int) bool {
			return (len(subgraphsSorted[i].Columns) > len(subgraphsSorted[j].Columns))
		})
		subgraphsPrevailing := subgraphsSorted[:compConfig.PrevailingNumber]
		for _, comp := range compConfig.PrevailingComposition {
			subgraphsSelected := make([]*model.Subgraph, len(comp))
			tokens := make([]model.Address, len(comp))
			for i, idx := range comp {
				subgraphsSelected[i] = subgraphsPrevailing[idx]
				tokens[i] = subgraphsPrevailing[idx].Token
			}
			subgraphsComposite, err := model.MergeSubgraphs(subgraphsSelected)
			if err != nil {
				return nil, fmt.Errorf("ConstructCompositeSubgraphs: MergeSubgraphs fail, error:%s", err.Error())
			}
			ret[string(model.MakeCompositeGIDWithBlockID(blockID, tokens))] = &CompositeGRecord{
				Subgraph: subgraphsComposite,
				Tokens:   tokens,
			}
		}
	}
	for _, comp := range compConfig.AdditionalComposition {
		subgraphsSelected := make([]*model.Subgraph, len(comp))
		for i, token := range comp {
			subgraphsSelected[i] = subgraphMap[string(token.Bytes())]
		}
		subgraphsComposite, err := model.MergeSubgraphs(subgraphsSelected)
		if err != nil {
			return nil, fmt.Errorf("ConstructCompositeSubgraphs: MergeSubgraphs fail, error:%s", err.Error())
		}
		ret[string(model.MakeCompositeGIDWithBlockID(blockID, comp))] = &CompositeGRecord{
			Subgraph: subgraphsComposite,
			Tokens:   comp,
		}
	}
	return ret, nil
}

func ConstructSubgraphs(fileName, dataDir string, qres *model.QueryResult, blockID uint16) (map[string]*model.Subgraph, error) {
	if qres == nil {
		var err error
		qres, blockID, err = loadQueryResult(fileName, dataDir)
		if err != nil {
			return nil, fmt.Errorf("ConstructSubgraphs: loadQueryResult fail, file:%s, error:%s", fileName, err.Error())
		}
	}
	transferMap := make(map[string][]*model.Transfer)
	tokenMap := make(map[string]model.Address)
	for _, ts := range qres.Transfers {
		//tokenStr := ts.Token.Hex()
		tokenStr := string(ts.Token.Bytes())
		if _, ok := transferMap[tokenStr]; !ok {
			transferMap[tokenStr] = make([]*model.Transfer, 0, 1)
		}
		transferMap[tokenStr] = append(transferMap[tokenStr], ts)
		tokenMap[tokenStr] = ts.Token
	}
	/*
		ret := make([]*model.Subgraph, 0, len(tokenMap))
		for k, v := range transferMap {
			ret = append(ret, generateSubgraph(blockID, tokenMap[k], v))
		}
	*/
	ret := make(map[string]*model.Subgraph, len(tokenMap))
	for k, v := range transferMap {
		if k == string(model.NativeTokenAddress.Bytes()) {
			ret[k] = generateSubgraph(blockID, tokenMap[k], qres.Txs, v)
		} else {
			ret[k] = generateSubgraph(blockID, tokenMap[k], nil, v)
		}
	}
	return ret, nil
}

func ConstructSubgraphs_TestTool(fileName, dataDir string) (*model.Subgraph, error) {
	qres, blockID, err := loadQueryResult(fileName, dataDir)
	if err != nil {
		return nil, fmt.Errorf("ConstructSubgraphs: loadQueryResult fail, file:%s, error:%s", fileName, err.Error())
	}
	return generateSubgraph(blockID, model.EmptyAddress, qres.Txs, qres.Transfers), nil
}

func ConstructTxTss(fileName, dataDir string, qres *model.QueryResult, blockID uint16) (map[string][]*model.Tx, map[string][]*model.Transfer, uint16, error) {
	if qres == nil {
		var err error
		qres, blockID, err = loadQueryResult(fileName, dataDir)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("ConstructSubgraphs: loadQueryResult fail, file:%s, error:%s", fileName, err.Error())
		}
	}
	txs := make(map[string][]*model.Tx)
	tss := make(map[string][]*model.Transfer)
	for _, tx := range qres.Txs {
		//addrStr := tx.From.Hex() + tx.To.Hex()
		addrStr := string(tx.From.Bytes()) + string(tx.To.Bytes())
		if _, ok := txs[addrStr]; !ok {
			txs[addrStr] = make([]*model.Tx, 0, 1)
		}
		txs[addrStr] = append(txs[addrStr], tx)
	}
	for _, ts := range qres.Transfers {
		//addrStr := ts.Token.Hex() + ts.From.Hex() + ts.To.Hex()
		addrStr := string(ts.Token.Bytes()) + string(ts.From.Bytes()) + string(ts.To.Bytes())
		if _, ok := tss[addrStr]; !ok {
			tss[addrStr] = make([]*model.Transfer, 0, 1)
		}
		tss[addrStr] = append(tss[addrStr], ts)
	}
	return txs, tss, blockID, nil
}
