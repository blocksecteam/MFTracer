package graph

import (
	"context"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"time"
	"transfer-graph/model"
	"transfer-graph/opensearch"
	"transfer-graph/pricedb"
	"transfer-graph/semantic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/sync/errgroup"
)

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
	//blockID := getBlockIDValid(fileName)
	blockID := getBlockID(fileName)

	filePath := path.Join(dataDir, fileName)
	qres, err := opensearch.LoadQueryResult(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("opensearch LoadQueryResult fail, file:%s, error:%s", filePath, err.Error())
	}
	return qres, blockID, nil
}

func GenerateSubgraphByTransfers(blockID uint16, token common.Address, tss []*model.Transfer) *model.Subgraph {
	return generateSubgraph(blockID, token, nil, tss)
}

func generateSubgraph(blockID uint16, token common.Address, txs []*model.Tx, tss []*model.Transfer) *model.Subgraph {
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
		/*
			row := make([]uint32, 0, len(row_map))
			timestamps := make([]uint32, 0, len(row_map))
			for k, v := range row_map {
				row = append(row, k)
				timestamps = append(timestamps, v)
			}
			sort.SliceStable(row, func(i, j int) bool {
				return row[i] < row[j]
			})
			sort.SliceStable(timestamps, func(i, j int) bool {
				return row[i] < row[j]
			})
			ret.Timestamps = append(ret.Timestamps, timestamps...)
			ret.Columns = append(ret.Columns, row...)
		*/
		ret.NodePtrs[i+1] = ret.NodePtrs[i] + uint32(len(row))
	}
	/*
		if len(ret.Columns) == 0 {
			for _, v := range tss {
				vv, _ := json.Marshal(v)
				fmt.Print(string(vv))
			}
			fmt.Println(" ")
			//generateSubgraphTest(blockID, token, tss)
			//fmt.Println(" ")
		}
	*/
	return ret
}

func getInDegrees(txMap map[string][]*model.Tx, tsMap map[string][]*model.Transfer) (map[string]int, map[string]int) {
	allDegrees := make(map[string]int)
	tokenDegrees := make(map[string]int)
	EtherAddress := string(model.EtherAddress.Bytes())
	for txMapKey := range txMap {
		toAddress := txMapKey[len(txMapKey)/2:]
		if _, ok := allDegrees[toAddress]; !ok {
			allDegrees[toAddress] = 1
		} else {
			allDegrees[toAddress] += 1
		}
		tokenKey := EtherAddress + toAddress
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
	EtherAddress := string(model.EtherAddress.Bytes())
	for k := range txMap {
		fromAddress := k[:len(k)/2]
		if _, ok := allDegrees[fromAddress]; !ok {
			allDegrees[fromAddress] = 1
		} else {
			allDegrees[fromAddress] += 1
		}
		tokenKey := EtherAddress + fromAddress
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
		if _, ok := model.SupportTokenMap[string(ts.Token.Bytes())]; ok {
			rTss = append(rTss, ts)
		}
	}
	for tsMapKey, transfers := range tsMap {
		if _, ok := model.SupportTokenMap[tsMapKey[:len(tsMapKey)/3]]; ok {
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
			tokens := make([]common.Address, len(comp))
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
	tokenMap := make(map[string]common.Address)
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
		if k == string(model.EtherAddress.Bytes()) {
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

func SyncFromCache(dataDir string, sBlockID, eBlockID uint16, g *GraphDB, pdb *pricedb.PriceDB, pdbParallel int, compConfig *model.CompositeConfiguration, stats *opensearch.Statistics) {
	m := WriteMetrics{}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Crit("read datadir failed", "datadir", dataDir, "err", err.Error())
	}
	ctx := context.WithValue(context.Background(), WriteMetricsKey, &m)

	if compConfig == nil {
		compConfig = model.EmptyCompositeConfiguration()
	}

	sort.SliceStable(entries, func(i, j int) bool {
		fileStartI := getBlockID(entries[i].Name())
		fileStartJ := getBlockID(entries[j].Name())
		return fileStartI < fileStartJ
	})

	for _, f := range entries {
		if f.IsDir() {
			continue
		}

		startTime := time.Now()
		fileName := f.Name()
		filePath := path.Join(dataDir, fileName)
		fileBID := getBlockID(fileName)
		if fileBID < sBlockID || fileBID >= eBlockID {
			//fmt.Println("[Debug] {SyncFromCache} skip file out of range", f.Name())
			log.Info("SyncFromCache skip", "path", filePath)
			continue
		}
		fmt.Println("[Debug] {SyncFromCache} iterate file", f.Name())
		qres, fileBID, err := loadQueryResult(fileName, dataDir)
		if err != nil {
			fmt.Println("SyncFromCache: loadQueryResult fail", "err", err.Error())
			return
			//log.Crit("SyncFromCache: loadQueryResult fail", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} load finished, file: %s, tx: %d, ts: %d\n", f.Name(), len(qres.Txs), len(qres.Transfers))

		loadTime := time.Now()

		txMap, tsMap, _, err := ConstructTxTss(fileName, dataDir, qres, fileBID)
		if err != nil {
			fmt.Println("ConstructTxTss failed", "path", filePath, "err", err.Error())
			return
			//log.Crit("ConstructTxTss failed", "path", filePath, "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} Construct S finished, file: %s, txMap: %d, tsMap:%d\n", f.Name(), len(txMap), len(tsMap))

		tsMap, qres.Transfers, err = semantic.AddTopswap(txMap, tsMap, qres.Transfers, pdb, pdbParallel, ctx)
		if err != nil {
			fmt.Println("AddTopswap failed", "err", err.Error())
			return
			//log.Crit("AddTopswap failed", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} AddTopswap finished, tsMap: %d, tss: %d\n", len(tsMap), len(qres.Transfers))
		oDegreeAll, oDegreeToken := getOutDegrees(txMap, tsMap)
		fmt.Printf("[Debug] {SyncFromCache} Get Out Degrees finished\n")
		tsMapByPos := classTsByTx(qres.Transfers) // re-generate this every time after semantic.AddXxx(tsMapByPos) is called!
		fmt.Printf("[Debug] {SyncFromCache} Class Ts by Tx finished\n")
		tsMap, qres.Transfers, err = semantic.AddWithinTx(txMap, tsMap, qres.Transfers, oDegreeAll, oDegreeToken, tsMapByPos, pdb, pdbParallel, ctx)
		if err != nil {
			fmt.Println("AddWithinTx failed", "err", err.Error())
			return
			//log.Crit("AddWithinTx failed", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} AddWithinTx finished, tsMap: %d, tss: %d\n", len(tsMap), len(qres.Transfers))
		qres.Transfers, tsMap = filterTss(qres.Transfers, tsMap)

		subgraphMap, err := ConstructSubgraphs(fileName, dataDir, qres, fileBID)
		if err != nil {
			fmt.Println("ConstructSubgraphs failed", "path", filePath, "err", err.Error())
			return
			//log.Crit("ConstructSubgraphs failed", "path", filePath, "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} Construct G finished, file: %s, GMap: %d\n", f.Name(), len(subgraphMap))
		greq := &GWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			Contents: make([]*model.Subgraph, 0, len(subgraphMap)),
		}
		for _, v := range subgraphMap {
			greq.Contents = append(greq.Contents, v)
		}
		//subgraphsComposite, err := ConstructCompositeSubgraphs(greq.Contents, subgraphMap, compConfig)
		compositeSubgraphsMap, err := ConstructCompositeSubgraphs(greq.Contents, subgraphMap, compConfig, fileBID)
		if err != nil {
			fmt.Println("ConstructCompositeSubgraphs failed", "err", err.Error())
			return
			//log.Crit("ConstructCompositeSubgraphs failed", "err", err.Error())
		}
		greq.CompositeContents = compositeSubgraphsMap
		fmt.Printf("[Debug] {SyncFromCache} Construct CG finished, len(CGs): %d\n", len(compositeSubgraphsMap))
		sreq := &SWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			BlockID:  fileBID,
			Contents: make([]*SRecord, 0, len(txMap)+len(tsMap)),
		}
		//ETHAddressHex := model.EtherAddress.Hex()
		ETHAddressHex := string(model.EtherAddress.Bytes())
		addrStrLength := len(ETHAddressHex)
		for k, v := range txMap {
			src := k[:addrStrLength]
			des := k[addrStrLength:]
			sreq.Contents = append(sreq.Contents, &SRecord{
				Token:     model.EtherAddress,
				SrcID:     subgraphMap[ETHAddressHex].AddressMap[src],
				DesID:     subgraphMap[ETHAddressHex].AddressMap[des],
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
			fmt.Println("SyncFromCache GWrite() failed", "err", err.Error())
			return
			//log.Crit("SyncFromCache GWrite() failed", "err", err.Error())
		}
		gwriteTime := time.Now()

		if err := g.SWrite(ctx, sreq); err != nil {
			fmt.Println("SyncFromCache SWrite() failed", "err", err.Error())
			return
			//log.Crit("SyncFromCache SWrite() failed", "err", err.Error())
		}
		swriteTime := time.Now()

		//log.Info("SyncFromCache() 1 file done", "type", "normal", "path", filePath, "load", loadTime.Sub(startTime), "construct", constructTime.Sub(loadTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))
		fmt.Println("[Debug] {SyncFromCache} SyncFromCache() 1 file done", "type", "normal", "path", filePath, "load", loadTime.Sub(startTime), "construct", constructTime.Sub(loadTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))

		//m.Log()
		//stats.Dump(qres)
	}
	fmt.Println("[Debug] {SyncFromCache} SyncFromCache all finished")
}

func SyncFromCacheSplit(dataDir string, sBlockID, eBlockID uint16, g *GraphDB, pdb *pricedb.PriceDB, pdbParallel int, compConfig *model.CompositeConfiguration, stats *opensearch.Statistics) {
	m := WriteMetrics{}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Crit("read datadir failed", "datadir", dataDir, "err", err.Error())
	}
	ctx := context.WithValue(context.Background(), WriteMetricsKey, &m)

	if compConfig == nil {
		compConfig = model.EmptyCompositeConfiguration()
	}

	sort.SliceStable(entries, func(i, j int) bool {
		fileStartI := getBlockID(entries[i].Name())
		fileStartJ := getBlockID(entries[j].Name())
		return fileStartI < fileStartJ
	})

	for i := 0; i < len(entries); i++ {
		f := entries[i]
		if f.IsDir() {
			continue
		}

		startTime := time.Now()
		fileName := f.Name()
		filePath := path.Join(dataDir, fileName)
		fileBID := getBlockID(fileName)
		if fileBID < sBlockID || fileBID >= eBlockID {
			//fmt.Println("[Debug] {SyncFromCache} skip file out of range", f.Name())
			log.Info("SyncFromCache skip", "path", filePath)
			continue
		}
		fmt.Println("[Debug] {SyncFromCache} iterate file", fileName)
		qres, fileBID, err := loadQueryResult(fileName, dataDir)
		if err != nil {
			fmt.Println("SyncFromCache: loadQueryResult fail", "err", err.Error())
			return
			//log.Crit("SyncFromCache: loadQueryResult fail", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} load finished, file: %s, tx: %d, ts: %d\n", fileName, len(qres.Txs), len(qres.Transfers))
		for j := i + 1; j < len(entries); j++ {
			sf := entries[j]
			if sf.IsDir() {
				i++
				continue
			}
			sfileName := sf.Name()
			sfileBID := getBlockID(sfileName)
			if sfileBID != fileBID {
				break
			}
			fmt.Println("[Debug] {SyncFromCache} iterate file", sfileName)
			sqres, _, err := loadQueryResult(sfileName, dataDir)
			if err != nil {
				fmt.Println("SyncFromCache: loadQueryResult fail", "err", err.Error())
				return
				//log.Crit("SyncFromCache: loadQueryResult fail", "err", err.Error())
			}
			qres.Txs = append(qres.Txs, sqres.Txs...)
			qres.Transfers = append(qres.Transfers, sqres.Transfers...)
			i++
			fmt.Printf("[Debug] {SyncFromCache} load finished, file: %s, tx: %d, ts: %d\n", sfileName, len(qres.Txs), len(qres.Transfers))
		}

		loadTime := time.Now()

		txMap, tsMap, _, err := ConstructTxTss(fileName, dataDir, qres, fileBID)
		if err != nil {
			fmt.Println("ConstructTxTss failed", "path", filePath, "err", err.Error())
			return
			//log.Crit("ConstructTxTss failed", "path", filePath, "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} Construct S finished, file: %s, txMap: %d, tsMap:%d\n", f.Name(), len(txMap), len(tsMap))

		tsMap, qres.Transfers, err = semantic.AddTopswap(txMap, tsMap, qres.Transfers, pdb, pdbParallel, ctx)
		if err != nil {
			fmt.Println("AddTopswap failed", "err", err.Error())
			return
			//log.Crit("AddTopswap failed", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} AddTopswap finished, tsMap: %d, tss: %d\n", len(tsMap), len(qres.Transfers))
		oDegreeAll, oDegreeToken := getOutDegrees(txMap, tsMap)
		fmt.Printf("[Debug] {SyncFromCache} Get Out Degrees finished\n")
		tsMapByPos := classTsByTx(qres.Transfers) // re-generate this every time after semantic.AddXxx(tsMapByPos) is called!
		fmt.Printf("[Debug] {SyncFromCache} Class Ts by Tx finished\n")
		tsMap, qres.Transfers, err = semantic.AddWithinTx(txMap, tsMap, qres.Transfers, oDegreeAll, oDegreeToken, tsMapByPos, pdb, pdbParallel, ctx)
		if err != nil {
			fmt.Println("AddWithinTx failed", "err", err.Error())
			return
			//log.Crit("AddWithinTx failed", "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} AddWithinTx finished, tsMap: %d, tss: %d\n", len(tsMap), len(qres.Transfers))
		qres.Transfers, tsMap = filterTss(qres.Transfers, tsMap)

		subgraphMap, err := ConstructSubgraphs(fileName, dataDir, qres, fileBID)
		if err != nil {
			fmt.Println("ConstructSubgraphs failed", "path", filePath, "err", err.Error())
			return
			//log.Crit("ConstructSubgraphs failed", "path", filePath, "err", err.Error())
		}
		fmt.Printf("[Debug] {SyncFromCache} Construct G finished, file: %s, GMap: %d\n", f.Name(), len(subgraphMap))
		greq := &GWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			Contents: make([]*model.Subgraph, 0, len(subgraphMap)),
		}
		for _, v := range subgraphMap {
			greq.Contents = append(greq.Contents, v)
		}
		//subgraphsComposite, err := ConstructCompositeSubgraphs(greq.Contents, subgraphMap, compConfig)
		compositeSubgraphsMap, err := ConstructCompositeSubgraphs(greq.Contents, subgraphMap, compConfig, fileBID)
		if err != nil {
			fmt.Println("ConstructCompositeSubgraphs failed", "err", err.Error())
			return
			//log.Crit("ConstructCompositeSubgraphs failed", "err", err.Error())
		}
		greq.CompositeContents = compositeSubgraphsMap
		fmt.Printf("[Debug] {SyncFromCache} Construct CG finished, len(CGs): %d\n", len(compositeSubgraphsMap))
		sreq := &SWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			BlockID:  fileBID,
			Contents: make([]*SRecord, 0, len(txMap)+len(tsMap)),
		}
		//ETHAddressHex := model.EtherAddress.Hex()
		ETHAddressHex := string(model.EtherAddress.Bytes())
		addrStrLength := len(ETHAddressHex)
		for k, v := range txMap {
			src := k[:addrStrLength]
			des := k[addrStrLength:]
			sreq.Contents = append(sreq.Contents, &SRecord{
				Token:     model.EtherAddress,
				SrcID:     subgraphMap[ETHAddressHex].AddressMap[src],
				DesID:     subgraphMap[ETHAddressHex].AddressMap[des],
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
			fmt.Println("SyncFromCache GWrite() failed", "err", err.Error())
			return
			//log.Crit("SyncFromCache GWrite() failed", "err", err.Error())
		}
		gwriteTime := time.Now()

		if err := g.SWrite(ctx, sreq); err != nil {
			fmt.Println("SyncFromCache SWrite() failed", "err", err.Error())
			return
			//log.Crit("SyncFromCache SWrite() failed", "err", err.Error())
		}
		swriteTime := time.Now()

		//log.Info("SyncFromCache() 1 file done", "type", "normal", "path", filePath, "load", loadTime.Sub(startTime), "construct", constructTime.Sub(loadTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))
		fmt.Println("[Debug] {SyncFromCache} SyncFromCache() 1 file done", "type", "normal", "path", filePath, "load", loadTime.Sub(startTime), "construct", constructTime.Sub(loadTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))

		//m.Log()
		//stats.Dump(qres)
	}
	fmt.Println("[Debug] {SyncFromCache} SyncFromCache all finished")
}

func SyncFromCacheParallel(dataDir string, sBlockID, eBlockID uint16, g *GraphDB, parallel int) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Crit("read datadir failed", "datadir", dataDir, "err", err.Error())
	}

	sort.SliceStable(entries, func(i, j int) bool {
		fileStartI := getBlockID(entries[i].Name())
		fileStartJ := getBlockID(entries[j].Name())
		return fileStartI < fileStartJ
	})

	update := func(fileName, dataDir string) error {
		startTime := time.Now()
		filePath := path.Join(dataDir, fileName)
		qres, fileBID, err := loadQueryResult(fileName, dataDir)
		if err != nil {
			return fmt.Errorf("SyncFromCacheParallel: loadQueryResult fail, file:%s, error:%s", fileName, err.Error())
		}

		loadTime := time.Now()
		subgraphMap, err := ConstructSubgraphs(fileName, dataDir, qres, fileBID)
		if err != nil {
			return fmt.Errorf("ConstructSubgraphs failed, path:%s, err: %s", filePath, err.Error())
		}
		txMap, tsMap, _, err := ConstructTxTss(fileName, dataDir, qres, fileBID)
		if err != nil {
			return fmt.Errorf("ConstructTxTss failed, path:%s, err: %s", filePath, err.Error())
		}
		greq := &GWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			Contents: make([]*model.Subgraph, 0, len(subgraphMap)),
		}
		for _, v := range subgraphMap {
			greq.Contents = append(greq.Contents, v)
		}
		sreq := &SWriteRequest{
			Desc:     fmt.Sprintf("bootstrap: %s", filePath),
			BlockID:  fileBID,
			Contents: make([]*SRecord, 0, len(txMap)+len(tsMap)),
		}
		//ETHAddressHex := model.EtherAddress.Hex()
		ETHAddressHex := string(model.EtherAddress.Bytes())
		addrStrLength := len(ETHAddressHex)
		for k, v := range txMap {
			src := k[:addrStrLength]
			des := k[addrStrLength:]
			sreq.Contents = append(sreq.Contents, &SRecord{
				Token:     model.EtherAddress,
				SrcID:     subgraphMap[ETHAddressHex].AddressMap[src],
				DesID:     subgraphMap[ETHAddressHex].AddressMap[des],
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

		if err := g.GWrite(context.Background(), greq); err != nil {
			return fmt.Errorf("SyncFromCache GWrite() failed, err: %s", err.Error())
		}
		gwriteTime := time.Now()

		if err := g.SWrite(context.Background(), sreq); err != nil {
			return fmt.Errorf("SyncFromCache SWrite() failed, err: %s", err.Error())
		}
		swriteTime := time.Now()

		log.Info("SyncFromCache() done", "type", "normal", "path", filePath, "load", loadTime.Sub(startTime), "construct", constructTime.Sub(loadTime), "gwrite", gwriteTime.Sub(constructTime), "swrite", swriteTime.Sub(gwriteTime))
		return nil
	}

	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for _, f := range entries {
		if f.IsDir() {
			continue
		}

		fileName := f.Name()
		fileBID := getBlockID(fileName)
		if fileBID < sBlockID || fileBID >= eBlockID {
			log.Info("SyncFromCacheParallel skip", "file", fileName)
			continue
		}
		eg.Go(func() error { return update(fileName, dataDir) })
	}
	if err := eg.Wait(); err != nil {
		log.Crit("SyncFromCacheParallel bootstrap failed: %s", err.Error())
	}
}
