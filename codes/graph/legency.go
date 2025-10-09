package graph

/*
	func SyncFromCache(dataDir string, sBlockID, eBlockID uint16, g *GraphDB, stats *opensearch.Statistics) {
		m := WriteMetrics{}
		entries, err := os.ReadDir(dataDir)
		if err != nil {
			log.Crit("read datadir failed", "datadir", dataDir, "err", err.Error())
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

			fileName := f.Name()
			filePath := path.Join(dataDir, fileName)
			qres, fileBID, err := loadQueryResult(fileName, dataDir)
			if err != nil {
				log.Crit("SyncFromCache: loadQueryResult fail, file:%s, error:%s", err.Error())
			}
			if fileBID < sBlockID || fileBID >= eBlockID {
				log.Info("SyncFromCache skip", "path", filePath)
				continue
			}

			startTime := time.Now()
			subgraphMap, err := ConstructSubgraphs(fileName, dataDir, qres, fileBID)
			if err != nil {
				log.Crit("ConstructSubgraphs failed", "path", filePath, "err", err.Error())
			}
			txMap, tsMap, _, err := ConstructTxTss(fileName, dataDir, qres, fileBID)
			if err != nil {
				log.Crit("ConstructTxTss failed", "path", filePath, "err", err.Error())
			}
			subgraphs := make([]*model.Subgraph, 0, len(subgraphMap))
			for _, v := range subgraphMap {
				subgraphs = append(subgraphs, v)
			}
			greq := &GWriteRequest{
				Desc:     fmt.Sprintf("bootstrap: %s", filePath),
				Contents: subgraphs,
			}
			ETHAddressHex := model.EtherAddress.Hex()
			addrStrLength := len(ETHAddressHex)
			sreqMap := make(map[string]*SWriteRequest)
			sreqMap[ETHAddressHex] = &SWriteRequest{
				Desc:     fmt.Sprintf("bootstrap: %s", filePath),
				BlockID:  fileBID,
				Token:    model.EtherAddress,
				Contents: make([]*SRecord, 0),
			}
			for k, v := range txMap {
				src := k[:addrStrLength]
				des := k[addrStrLength:]
				sreqMap[ETHAddressHex].Contents = append(sreqMap[ETHAddressHex].Contents, &SRecord{
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
				if _, ok := sreqMap[token]; !ok {
					sreqMap[token] = &SWriteRequest{
						Desc:     fmt.Sprintf("bootstrap: %s", filePath),
						BlockID:  fileBID,
						Token:    v[0].Token,
						Contents: make([]*SRecord, 0),
					}
				}
				sreqMap[token].Contents = append(sreqMap[token].Contents, &SRecord{
					SrcID:     subgraphMap[token].AddressMap[src],
					DesID:     subgraphMap[token].AddressMap[des],
					Transfers: v,
					Txs:       nil,
				})
			}
			ctx := context.WithValue(context.Background(), WriteMetricsKey, &m)
			constructTime := time.Now()

			if err := g.GWrite(ctx, greq); err != nil {
				log.Crit("SyncFromCache GWrite() failed", "err", err.Error())
			}
			gwriteTime := time.Now()

			for _, sreq := range sreqMap {
				if err := g.SWrite(ctx, sreq); err != nil {
					log.Crit("SyncFromCache SWrite() failed", "err", err.Error())
				}
			}
			swriteTime := time.Now()

			log.Info("SyncFromCache() done", "type", "normal", "path", filePath, "construct", constructTime.Sub(startTime), "gwrite", gwriteTime.Sub(startTime), "swrite", swriteTime.Sub(startTime))

			m.Log()
			stats.Dump(qres)
		}
	}
*/
