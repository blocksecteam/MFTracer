package experiment

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"transfer-graph-evm/flow"
	"transfer-graph-evm/graph"
	"transfer-graph-evm/model"
	"transfer-graph-evm/pricedb"
	"transfer-graph-evm/search"
	"transfer-graph-evm/utils"
)

func TraceDownstream(ctx context.Context, sBlockId, eBlockId uint16, srcAddressFilePath, allowedAddressFilePath, forbiddenAddressFilePath, oFilePath string) error {
	utils.Logger.Info("Starting TraceDownstream", "start_block", sBlockId, "end_block", eBlockId, "source_address_file", srcAddressFilePath, "allowed_address_file", allowedAddressFilePath, "forbidden_address_file", forbiddenAddressFilePath, "output_file", oFilePath)
	DBPath := model.GetConfigDBPath()
	g, err := graph.NewGraphDB(DBPath, true)
	if err != nil {
		return err
	}
	defer g.Close()
	pDBPath := model.GetConfigPriceDBPath()
	p, err := pricedb.NewPriceDB(pDBPath, false)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	subgraphs, err := g.BlockIDRangeWithTokenToSubgraphs(context.Background(), sBlockId, eBlockId, utils.USDTAddress, graph.DefaultQueryConfig())
	if err != nil {
		return err
	}
	rMaps := model.ReverseAddressMaps(nil, subgraphs)
	utils.Logger.Info("Subgraphs loaded", "count", len(subgraphs), "start_block", sBlockId, "end_block", eBlockId)

	oFileBrief0, err := os.OpenFile(oFilePath+".topn.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer oFileBrief0.Close()
	oFileBrief1, err := os.OpenFile(oFilePath+".alln.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer oFileBrief1.Close()
	oFileBrief2, err := os.OpenFile(oFilePath+".alle.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer oFileBrief2.Close()

	srcAddrs, err := utils.ReadAddressFile(srcAddressFilePath)
	if err != nil {
		return err
	}
	srcAddrStrs := make([]string, len(srcAddrs))
	for i, addr := range srcAddrs {
		srcAddrStrs[i] = utils.AddrToAddrString(addr)
	}
	var allowedAddrs, forbiddenAddrs []model.Address
	if len(allowedAddressFilePath) != 0 {
		allowedAddrs, err = utils.ReadAddressFile(allowedAddressFilePath)
		if err != nil {
			return err
		}
	}
	if len(forbiddenAddressFilePath) != 0 {
		forbiddenAddrs, err = utils.ReadAddressFile(forbiddenAddressFilePath)
		if err != nil {
			return err
		}
	}

	utils.Logger.Info("Start querying main graph", "source_address_count", len(srcAddrs))
	mgs := search.GetMainGraphPrune(subgraphs, srcAddrs, nil, rMaps, allowedAddrs, forbiddenAddrs, 0, 12)
	qconfig := graph.DefaultQueryConfig()
	qconfig.FetchThreads = 12
	originSubgraphss := make([][]*model.Subgraph, len(subgraphs))
	for i := range subgraphs {
		originSubgraphss[i] = []*model.Subgraph{subgraphs[i]}
	}

	var fg *flow.FlowGraph
	var head = true
	for i := range mgs {
		utils.Logger.Info("Processing main graph", "index", i, "total", len(mgs))
		txsi, tssi, err := graph.QueryMGEdgesParallel(g, mgs[i], subgraphs[i], rMaps[i], originSubgraphss[i], 12, context.Background(), qconfig)
		if err != nil {
			return err
		}
		utils.Logger.Info("Edge query completed for main graph", "index", i, "transfer_count", len(tssi))
		fe := flow.NewEgdesSortedByTime(txsi, tssi, false, p, 1, ctx)
		if i == 0 {
			motherNode := &flow.ThresholdAgeFlowNode{
				Config: &flow.ThresholdAgeFlowNodeConfig{
					Threshold: model.GlobalTomlConfig.Flow.ActivateThreshold,
					AgeLimit:  model.GlobalTomlConfig.Flow.AgeLimit,
				},
			}
			fg = flow.NewFlowGraph(motherNode, fe, srcAddrStrs, nil)
		} else {
			fg.ResetEdges(fe)
		}
		utils.Logger.Info("Starting flow computation for main graph", "index", i, "touched_node_count", len(fg.Nodes))
		fg.FlowToEnd()
		writeEdgeResult(fg, oFileBrief2, head)
		head = false
		// release memory
		fg.ResetEdges(nil)
		fe.Free()
		fe = nil
		tssi = nil
		mgs[i].Free()
		mgs[i] = nil
		subgraphs[i].Free()
		subgraphs[i] = nil
		originSubgraphss[i][0].Free()
		originSubgraphss[i] = nil
		rMaps[i] = nil
		runtime.GC()
		utils.Logger.Info("Completed processing main graph", "index", i)
	}
	writeNodeResult(fg, nil, oFileBrief0, oFileBrief1, nil, 100000, true)
	utils.Logger.Info("TraceDownstream completed successfully", "total_volume", fg.TotalVolume(), "output_file", oFilePath)
	return nil
}

func writeNodeResult(fg *flow.FlowGraph, desAddrs []model.Address, oFileBrief0, oFileBrief1, oFileBrief3 *os.File, topN int, head bool) error {
	var err error
	bw0 := bufio.NewWriterSize(oFileBrief0, 1<<27) // 128MB buffer
	bw1 := bufio.NewWriterSize(oFileBrief1, 1<<27)
	bw3 := bufio.NewWriterSize(oFileBrief3, 1<<27)

	writeLabel := false
	switch fg.WhatIsMotherNode().(type) {
	case *flow.ThresholdAgeLabelFlowNode:
		writeLabel = true
	default:
	}

	if head {
		if !writeLabel {
			_, err = bw0.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return err
			}
			_, err = bw1.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return err
			}
			_, err = bw3.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return err
			}
		} else {
			_, err = bw0.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return err
			}
			_, err = bw1.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return err
			}
			_, err = bw3.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return err
			}
		}
	}

	topNodes := fg.TopNodes(topN, func(a, b flow.FlowNode) bool {
		return a.TotalI() > b.TotalI()
	})
	sort.Slice(topNodes, func(i, j int) bool {
		return topNodes[i].TotalI()-topNodes[i].TotalO() > topNodes[j].TotalI()-topNodes[j].TotalO()
	})

	for _, tn := range topNodes {
		_, err = fmt.Fprintf(bw0, "%s,%.2f,%.2f,%.2f",
			utils.AddrStringToStr(tn.Address()), tn.TotalI(), tn.TotalO(), tn.TotalI()-tn.TotalO())
		if err != nil {
			return err
		}
		if writeLabel {
			labels := tn.(*flow.ThresholdAgeLabelFlowNode).CompactLabels()
			labelStr := ""
			for l := range labels {
				if len(labelStr) > 0 {
					labelStr += "|"
				}
				labelStr += utils.AddrStringToStr(l)
			}
			_, err = fmt.Fprintf(bw0, ",%d,\"%s\"", len(labels), labelStr)
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(bw0, "\n")
		if err != nil {
			return err
		}
	}
	if err = bw0.Flush(); err != nil {
		return err
	}

	const flushEvery = 100000
	line := 0
	for a, n := range fg.Nodes {
		_, err = fmt.Fprintf(bw1, "%s,%.2f,%.2f,%.2f",
			utils.AddrStringToStr(a), n.TotalI(), n.TotalO(), n.TotalI()-n.TotalO())
		if err != nil {
			return err
		}
		if writeLabel {
			labels := n.(*flow.ThresholdAgeLabelFlowNode).CompactLabels()
			labelStr := ""
			for l := range labels {
				if len(labelStr) > 0 {
					labelStr += "|"
				}
				labelStr += utils.AddrStringToStr(l)
			}
			_, err = fmt.Fprintf(bw1, ",%d,\"%s\"", len(labels), labelStr)
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(bw1, "\n")
		if err != nil {
			return err
		}
		line++
		if line%flushEvery == 0 {
			if err = bw1.Flush(); err != nil {
				return err
			}
		}
	}
	if err = bw1.Flush(); err != nil {
		return err
	}

	desAddrSet := make(map[string]struct{})
	for _, addr := range desAddrs {
		desAddrSet[utils.AddrToAddrString(addr)] = struct{}{}
	}
	for a := range fg.Nodes {
		if _, ok := desAddrSet[a]; !ok {
			continue
		}
		n := fg.Nodes[a]
		_, err = fmt.Fprintf(bw3, "%s,%.2f,%.2f,%.2f",
			utils.AddrStringToStr(a), n.TotalI(), n.TotalO(), n.TotalI()-n.TotalO())
		if err != nil {
			return err
		}
		if writeLabel {
			labels := n.(*flow.ThresholdAgeLabelFlowNode).CompactLabels()
			labelStr := ""
			for l := range labels {
				if len(labelStr) > 0 {
					labelStr += "|"
				}
				labelStr += utils.AddrStringToStr(l)
			}
			_, err = fmt.Fprintf(bw3, ",%d,\"%s\"", len(labels), labelStr)
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(bw3, "\n")
		if err != nil {
			return err
		}
	}
	if err = bw3.Flush(); err != nil {
		return err
	}

	return nil
}

func writeNodeResultLight(fg *flow.FlowGraph, oFileBrief0, oFileBrief1, oFileBrief3, oFileBrief4 *os.File, topN int, head bool) (int, float64, error) {
	var err error
	bw0 := bufio.NewWriterSize(oFileBrief0, 1<<28) // 128MB buffer
	bw1 := bufio.NewWriterSize(oFileBrief1, 1<<28)
	bw3 := bufio.NewWriterSize(oFileBrief3, 1<<28)
	bw4 := bufio.NewWriterSize(oFileBrief4, 1<<28)

	writeLabel := false
	switch fg.WhatIsMotherNode().(type) {
	case *flow.ThresholdAgeLabelFlowNode:
		writeLabel = true
	default:
	}

	if head {
		if !writeLabel {
			_, err = bw0.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw1.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw3.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw4.WriteString("address,total_in,total_out,remaining\n")
			if err != nil {
				return 0, 0, err
			}
		} else {
			_, err = bw0.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw1.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw3.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return 0, 0, err
			}
			_, err = bw4.WriteString("address,total_in,total_out,remaining,label_len,label\n")
			if err != nil {
				return 0, 0, err
			}
		}
	}

	allNodes := make([]flow.FlowNode, 0, len(fg.Nodes))
	for _, n := range fg.Nodes {
		allNodes = append(allNodes, n)
	}
	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].TotalI()-allNodes[i].TotalO() > allNodes[j].TotalI()-allNodes[j].TotalO()
	})
	srcAddrSet := make(map[string]struct{})
	for _, addr := range fg.SrcAddresses() {
		srcAddrSet[addr] = struct{}{}
	}
	desAddrSet := make(map[string]struct{})
	for _, addr := range fg.DesAddresses() {
		desAddrSet[addr] = struct{}{}
	}

	const flushEvery = 100000
	line := 0
	unknownCount, unknownVolume := 0, 0.
	for _, n := range allNodes {
		a := n.Address()
		var lineString string
		lineString += fmt.Sprintf("%s,%.2f,%.2f,%.2f",
			utils.AddrStringToStr(a), n.TotalI(), n.TotalO(), n.TotalI()-n.TotalO())
		if err != nil {
			return 0, 0, err
		}
		if writeLabel {
			labels := n.(*flow.ThresholdAgeLabelFlowNode).RawLabels()
			labelStr := make([]byte, 0)
			for i := 0; i < len(labels); i += len(a) {
				labelStr = append(labelStr, []byte(model.BytesToAddress(labels[i:i+len(a)]).String())...)
				labelStr = append(labelStr, '|')
			}
			lineString += fmt.Sprintf(",%d,\"%s\"", len(labels), string(labelStr))
		}

		line++
		if line%flushEvery == 0 {
			if err = bw1.Flush(); err != nil {
				return 0, 0, err
			}
			utils.Logger.Info("Wrote node results", "lines_written", line)
		}

		_, err = fmt.Fprintf(bw1, "%s\n", lineString)
		if err != nil {
			return 0, 0, err
		}
		if line <= topN {
			_, err = fmt.Fprintf(bw0, "%s\n", lineString)
			if err != nil {
				return 0, 0, err
			}
		}
		_, oks := srcAddrSet[a]
		_, okd := desAddrSet[a]
		if n.TotalI()-n.TotalO() > 10000 && !oks && !okd {
			_, err = fmt.Fprintf(bw3, "%s\n", lineString)
			if err != nil {
				return 0, 0, err
			}
			unknownCount++
			unknownVolume += n.TotalI() - n.TotalO()
		}
		if okd {
			_, err = fmt.Fprintf(bw4, "%s\n", lineString)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	if err = bw0.Flush(); err != nil {
		return 0, 0, err
	}
	if err = bw1.Flush(); err != nil {
		return 0, 0, err
	}
	if err = bw3.Flush(); err != nil {
		return 0, 0, err
	}
	if err = bw4.Flush(); err != nil {
		return 0, 0, err
	}

	return unknownCount, unknownVolume, nil
}

func writeEdgeResult(fg *flow.FlowGraph, oFileBrief2 *os.File, head bool) error {
	var err error
	bw2 := bufio.NewWriterSize(oFileBrief2, 1<<28) // 128MB buffer
	if head {
		_, err = bw2.WriteString("pos,from,to,value,used_value,timestamp,tx_hash,age\n")
		if err != nil {
			return err
		}
	}

	const flushEvery = 100000
	for i := 0; i < len(fg.LeachDigests); i++ {
		ts, usedVal, age := fg.LeachOneTransferNative(i)
		_, err = fmt.Fprintf(bw2, "%d,%s,%s,%.2f,%.2f,%s,%s,%d\n",
			ts.Pos, ts.From.String(), ts.To.String(),
			float64(ts.Value.ToInt().Uint64())/math.Pow10(model.DollarDecimals), usedVal, ts.Timestamp, ts.TxHash.Hex(), age)
		if err != nil {
			return err
		}
		if i%flushEvery == 0 {
			if err = bw2.Flush(); err != nil {
				return err
			}
		}
	}
	if err = bw2.Flush(); err != nil {
		return err
	}
	return nil
}
