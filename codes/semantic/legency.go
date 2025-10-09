package semantic

import (
	"math/big"
	"transfer-graph/model"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"gonum.org/v1/gonum/mat"
)

/*
func addUniswapRouter02(

		compositeSubgraphMap map[string]*graph.CompositeGRecord,
		subgraphMap map[string]*model.Subgraph,
		txMap map[string][]*model.Tx,
		tsMap map[string][]*model.Transfer,
		blockID uint16) (map[string]*model.Subgraph, []*graph.CompositeGRecord, map[string][]*model.Transfer, error) {
		uniswapABI, err := abi.JSON(strings.NewReader(UniswapRouter02ABI))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("abi.JSON() failed: %s", err.Error())
		}
		swapTsTokensMap := make(map[string][]common.Address)
		swapTsMapTwoToken := make(map[string][]*model.Transfer)
		swapTsMapOneToken := make(map[string][]*model.Transfer)
		for _, funcName := range UniswapRouter02Funcs {
			functionABI, ok := uniswapABI.Methods[funcName]
			if !ok {
				return nil, nil, nil, fmt.Errorf("function '%s' not found in UniswapRouter02ABI", funcName)
			}
			functionSelector := "0x" + common.Bytes2Hex(functionABI.ID)
			paramsABI := functionABI.Inputs
			for k, txs := range txMap {
				if getToAddressOfTxMapKey(k).Cmp(UniswapRouter02Address) != 0 {
					continue
				}
				fromAddress := getFromAddressOfTxMapKey(k)
				for _, tx := range txs {
					if strings.Compare(tx.Func, functionSelector) != 0 {
						continue
					}
					argMap := make(map[string]interface{})
					if err = paramsABI.UnpackIntoMap(argMap, tx.Param); err != nil {
						return nil, nil, nil, fmt.Errorf("paramsABI.UnpackIntoMap() failed: %s", err.Error())
					}
					pathIntf, ok := argMap["path"]
					if !ok {
						return nil, nil, nil, fmt.Errorf("'path' not found in argMap")
					}
					toIntf, ok := argMap["to"]
					if !ok {
						return nil, nil, nil, fmt.Errorf("'to' not found in argMap")
					}
					path := pathIntf.([]common.Address)
					tokenA := path[0]
					tokenB := path[len(path)-1]
					swapTransfer := &model.Transfer{
						Pos:  (tx.Block << 16) | uint64(tx.Index),
						Type: uint16(model.TransferTypeSwap),
						From: fromAddress,
						To:   toIntf.(common.Address),
					}
					if tokenA.Cmp(tokenB) != 0 {
						gid, tokens := getTwoTokenGidAsString(tokenA, tokenB, blockID)
						if _, ok := swapTsMapTwoToken[gid]; !ok {
							swapTsMapTwoToken[gid] = make([]*model.Transfer, 0, 1)
							swapTsTokensMap[gid] = tokens
						}
						swapTsMapTwoToken[gid] = append(swapTsMapTwoToken[gid], swapTransfer)
					} else {
						tokenString := string(tokenA.Bytes())
						if _, ok := swapTsMapOneToken[tokenString]; !ok {
							swapTsMapOneToken[tokenString] = make([]*model.Transfer, 0, 1)
						}
						swapTsMapOneToken[tokenString] = append(swapTsMapOneToken[tokenString], swapTransfer)
					}
				}
			}
		}
		for gid, tss := range swapTsMapTwoToken {
			compositeSubgraph := graph.GenerateSubgraphByTransfers(blockID, model.CompositeAddress, tss)
			if originCompositeSubgraph, ok := compositeSubgraphMap[gid]; ok {
				newCompositeSubgraph, err := mergeTwoSubgraph(originCompositeSubgraph.Subgraph, compositeSubgraph)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("mergeTwoSubgraph failed: %s", err.Error())
				}
				compositeSubgraphMap[gid].Subgraph = newCompositeSubgraph
			} else {
				compositeSubgraphMap[gid] = &graph.CompositeGRecord{
					Subgraph: compositeSubgraph,
					Tokens:   swapTsTokensMap[gid],
				}
			}

			for _, ts := range tss {
				tsKey := string(token.Bytes()) + string(ts.From.Bytes()) + string(ts.To.Bytes())
				if _, ok := tsMap[tsKey]; !ok {
					tsMap[tsKey] = make([]*model.Transfer, 0, 1)
				}
				tsMap[tsKey] = append(tsMap[tsKey], ts)
			}

		}
	}
*/
/*
func addUniswapRouter02(
	subgraphMap map[string]*model.Subgraph,
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	blockID uint16) (map[string]*model.Subgraph, map[string][]*model.Transfer, error) {
	uniswapABI, err := abi.JSON(strings.NewReader(UniswapV2ABI))
	if err != nil {
		return nil, nil, fmt.Errorf("abi.JSON() failed: %s", err.Error())
	}
	swapTsMap := make(map[string][]*model.Transfer)
	for _, funcName := range UniswapV2Funcs {
		functionABI, ok := uniswapABI.Methods[funcName]
		if !ok {
			return nil, nil, fmt.Errorf("function '%s' not found in UniswapRouter02ABI", funcName)
		}
		functionSelector := "0x" + common.Bytes2Hex(functionABI.ID)
		paramsABI := functionABI.Inputs
		for k, txs := range txMap {
			fromAddress := getFromAddressOfTxMapKey(k)
			for _, tx := range txs {
				if strings.Compare(tx.Func, functionSelector) != 0 {
					continue
				}
				argMap := make(map[string]interface{})
				if err = paramsABI.UnpackIntoMap(argMap, tx.Param); err != nil {
					return nil, nil, fmt.Errorf("paramsABI.UnpackIntoMap() failed: %s", err.Error())
				}
				pathIntf, ok := argMap["path"]
				if !ok {
					return nil, nil, fmt.Errorf("'path' not found in argMap")
				}
				toIntf, ok := argMap["to"]
				if !ok {
					return nil, nil, fmt.Errorf("'to' not found in argMap")
				}
				path := pathIntf.([]common.Address)
				tokenA := path[0]
				tokenB := path[len(path)-1]
				swapTransfer := &model.Transfer{
					Pos:   tx.Pos(),
					Type:  uint16(model.TransferTypeSwap),
					From:  fromAddress,
					To:    toIntf.(common.Address),
					Value: (*hexutil.Big)(big.NewInt(0)),
				}
				if tokenA.Cmp(tokenB) != 0 {
					tokenAString := string(tokenA.Bytes())
					if _, ok := swapTsMap[tokenAString]; !ok {
						swapTsMap[tokenAString] = make([]*model.Transfer, 0, 1)
					}
					swapTsMap[tokenAString] = append(swapTsMap[tokenAString], swapTransfer)
					tokenBString := string(tokenB.Bytes())
					if _, ok := swapTsMap[tokenBString]; !ok {
						swapTsMap[tokenBString] = make([]*model.Transfer, 0, 1)
					}
					swapTsMap[tokenBString] = append(swapTsMap[tokenBString], swapTransfer)
				} else {
					tokenString := string(tokenA.Bytes())
					if _, ok := swapTsMap[tokenString]; !ok {
						swapTsMap[tokenString] = make([]*model.Transfer, 0, 1)
					}
					swapTsMap[tokenString] = append(swapTsMap[tokenString], swapTransfer)
				}
			}
		}
	}
	for key, tss := range swapTsMap {
		token := common.BytesToAddress([]byte(key))
		subgraph := GenerateSubgraphByTransfers(blockID, token, tss)
		if oriSubgraph, ok := subgraphMap[key]; ok {
			newSubgraph, err := mergeTwoSubgraph(oriSubgraph, subgraph)
			if err != nil {
				return nil, nil, fmt.Errorf("mergeTwoSubgraph failed: %s", err.Error())
			}
			subgraphMap[key] = newSubgraph
		} else {
			subgraphMap[key] = subgraph
		}
		for _, ts := range tss {
			tsKey := key + string(ts.From.Bytes()) + string(ts.To.Bytes())
			if _, ok := tsMap[tsKey]; !ok {
				tsMap[tsKey] = make([]*model.Transfer, 0, 1)
			}
			tsMap[tsKey] = append(tsMap[tsKey], ts)
		}
	}
	return subgraphMap, tsMap, nil
}

func AddSwap(
	subgraphMap map[string]*model.Subgraph,
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	blockID uint16) (map[string]*model.Subgraph, map[string][]*model.Transfer, error) {
	subgraphMap, tsMap, err := addUniswapRouter02(subgraphMap, txMap, tsMap, blockID)
	return subgraphMap, tsMap, err
}
*/

// abi.go
/*
const (
	UniswapV2ABI        = `[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"amountADesired","type":"uint256"},{"internalType":"uint256","name":"amountBDesired","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountTokenDesired","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountIn","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsIn","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"reserveA","type":"uint256"},{"internalType":"uint256","name":"reserveB","type":"uint256"}],"name":"quote","outputs":[{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETHSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermit","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermitSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityWithPermit","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapETHForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]`
	UniswapV3IV3SwapABI = `[{"inputs":[{"internalType":"address","name":"_factoryV2","type":"address"},{"internalType":"address","name":"factoryV3","type":"address"},{"internalType":"address","name":"_positionManager","type":"address"},{"internalType":"address","name":"_WETH9","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH9","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"approveMax","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"approveMaxMinusOne","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"approveZeroThenMax","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"approveZeroThenMaxMinusOne","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"bytes","name":"data","type":"bytes"}],"name":"callPositionManager","outputs":[{"internalType":"bytes","name":"result","type":"bytes"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"paths","type":"bytes[]"},{"internalType":"uint128[]","name":"amounts","type":"uint128[]"},{"internalType":"uint24","name":"maximumTickDivergence","type":"uint24"},{"internalType":"uint32","name":"secondsAgo","type":"uint32"}],"name":"checkOracleSlippage","outputs":[],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"uint24","name":"maximumTickDivergence","type":"uint24"},{"internalType":"uint32","name":"secondsAgo","type":"uint32"}],"name":"checkOracleSlippage","outputs":[],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"}],"internalType":"struct IV3SwapRouter.ExactInputParams","name":"params","type":"tuple"}],"name":"exactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct IV3SwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"}],"internalType":"struct IV3SwapRouter.ExactOutputParams","name":"params","type":"tuple"}],"name":"exactOutput","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct IV3SwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"factoryV2","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"getApprovalType","outputs":[{"internalType":"enum IApproveAndCall.ApprovalType","name":"","type":"uint8"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"token0","type":"address"},{"internalType":"address","name":"token1","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"amount0Min","type":"uint256"},{"internalType":"uint256","name":"amount1Min","type":"uint256"}],"internalType":"struct IApproveAndCall.IncreaseLiquidityParams","name":"params","type":"tuple"}],"name":"increaseLiquidity","outputs":[{"internalType":"bytes","name":"result","type":"bytes"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"token0","type":"address"},{"internalType":"address","name":"token1","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint256","name":"amount0Min","type":"uint256"},{"internalType":"uint256","name":"amount1Min","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"internalType":"struct IApproveAndCall.MintParams","name":"params","type":"tuple"}],"name":"mint","outputs":[{"internalType":"bytes","name":"result","type":"bytes"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"previousBlockhash","type":"bytes32"},{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"positionManager","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"pull","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"refundETH","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermit","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowed","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowedIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"sweepToken","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"}],"name":"sweepToken","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"sweepTokenWithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"sweepTokenWithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"int256","name":"amount0Delta","type":"int256"},{"internalType":"int256","name":"amount1Delta","type":"int256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"uniswapV3SwapCallback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"}],"name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"unwrapWETH9WithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"unwrapWETH9WithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"wrapETH","outputs":[],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]`
	UniswapV3ISwapABI   = `[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH9","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH9","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"}],"internalType":"struct ISwapRouter.ExactInputParams","name":"params","type":"tuple"}],"name":"exactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"}],"internalType":"struct ISwapRouter.ExactOutputParams","name":"params","type":"tuple"}],"name":"exactOutput","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"refundETH","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermit","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowed","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowedIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"sweepToken","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"sweepTokenWithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"int256","name":"amount0Delta","type":"int256"},{"internalType":"int256","name":"amount1Delta","type":"int256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"uniswapV3SwapCallback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"unwrapWETH9WithFee","outputs":[],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]`
)

var (
	UniswapV2Funcs = [9]string{
		"swapExactTokensForTokens",
		"swapTokensForExactTokens",
		"swapExactETHForTokens",
		"swapTokensForExactETH",
		"swapExactTokensForETH",
		"swapETHForExactTokens",
		"swapExactTokensForTokensSupportingFeeOnTransferTokens",
		"swapExactETHForTokensSupportingFeeOnTransferTokens",
		"swapExactTokensForETHSupportingFeeOnTransferTokens",
	}
	UniswapV3Funcs = [4]string{
		"exactInput",
		"exactOutput",
		"exactInputSingle",
		"exactOutputSingle",
	}
)

type UniswapV3ISwap_ExactInputParams struct {
	Path             []uint8        `json:"path"`
	Recipient        common.Address `json:"recipient"`
	Deadline         *big.Int       `json:"deadline"`
	AmountIn         *big.Int       `json:"amountIn"`
	AmountOutMinimum *big.Int       `json:"amountOutMinimum"`
}

type UniswapV3ISwap_ExactOutputParams struct {
	Path            []uint8        `json:"path"`
	Recipient       common.Address `json:"recipient"`
	Deadline        *big.Int       `json:"deadline"`
	AmountOut       *big.Int       `json:"amountOut"`
	AmountInMaximum *big.Int       `json:"amountInMaximum"`
}

type UniswapV3ISwap_ExactInputSingleParams struct {
	TokenIn           common.Address `json:"tokenIn"`
	TokenOut          common.Address `json:"tokenOut"`
	Fee               uint32         `json:"fee"`
	Recipient         common.Address `json:"recipient"`
	Deadline          *big.Int       `json:"deadline"`
	AmountIn          *big.Int       `json:"amountIn"`
	AmountOutMinimum  *big.Int       `json:"amountOutMinimum"`
	SqrtPriceLimitX96 *big.Int       `json:"sqrtPriceLimitX96"`
}

type UniswapV3ISwap_ExactOutputSingleParams struct {
	TokenIn           common.Address `json:"tokenIn"`
	TokenOut          common.Address `json:"tokenOut"`
	Fee               uint32         `json:"fee"`
	Recipient         common.Address `json:"recipient"`
	Deadline          *big.Int       `json:"deadline"`
	AmountOut         *big.Int       `json:"amountOut"`
	AmountInMaximum   *big.Int       `json:"amountInMaximum"`
	SqrtPriceLimitX96 *big.Int       `json:"sqrtPriceLimitX96"`
}

type UniswapV3IV3Swap_ExactInputParams struct {
	Path             []uint8        `json:"path"`
	Recipient        common.Address `json:"recipient"`
	AmountIn         *big.Int       `json:"amountIn"`
	AmountOutMinimum *big.Int       `json:"amountOutMinimum"`
}

type UniswapV3IV3Swap_ExactOutputParams struct {
	Path            []uint8        `json:"path"`
	Recipient       common.Address `json:"recipient"`
	AmountOut       *big.Int       `json:"amountOut"`
	AmountInMaximum *big.Int       `json:"amountInMaximum"`
}

type UniswapV3IV3Swap_ExactInputSingleParams struct {
	TokenIn           common.Address `json:"tokenIn"`
	TokenOut          common.Address `json:"tokenOut"`
	Fee               uint32         `json:"fee"`
	Recipient         common.Address `json:"recipient"`
	AmountIn          *big.Int       `json:"amountIn"`
	AmountOutMinimum  *big.Int       `json:"amountOutMinimum"`
	SqrtPriceLimitX96 *big.Int       `json:"sqrtPriceLimitX96"`
}

type UniswapV3IV3Swap_ExactOutputSingleParams struct {
	TokenIn           common.Address `json:"tokenIn"`
	TokenOut          common.Address `json:"tokenOut"`
	Fee               uint32         `json:"fee"`
	Recipient         common.Address `json:"recipient"`
	AmountOut         *big.Int       `json:"amountOut"`
	AmountInMaximum   *big.Int       `json:"amountInMaximum"`
	SqrtPriceLimitX96 *big.Int       `json:"sqrtPriceLimitX96"`
}
*/

//swap_test.go
/*
func _TestABI(t *testing.T) {
	const (
		ABIUniswapRouter02SwapExactTokensForTokens = `[{
				"inputs":[
					{"internalType":"uint256","name":"amountIn","type":"uint256"},
					{"internalType":"uint256","name":"amountOutMin","type":"uint256"},
					{"internalType":"address[]","name":"path","type":"address[]"},
					{"internalType":"address","name":"to","type":"address"},
					{"internalType":"uint256","name":"deadline","type":"uint256"}
				],
				"name":"swapExactTokensForTokens",
				"outputs":[
					{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}
				],
				"stateMutability":"nonpayable",
				"type":"function"
			}]`
		dataDir  = "/data/test-data-param/test"
		fileName = "16100000_16102000.json.zst"
	)
	//AddressUniswapRouter02 := common.HexToAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
	abi, err := abi.JSON(strings.NewReader(ABIUniswapRouter02SwapExactTokensForTokens))
	if err != nil {
		t.Error(err)
	}
	function, ok := abi.Methods["swapExactTokensForTokens"]
	if !ok {
		t.Errorf("'swapExactTokensForTokens' not found in abi.Methods\n")
	}
	params := function.Inputs
	qres, err := opensearch.LoadQueryResult(path.Join(dataDir, fileName))
	if err != nil {
		t.Error(err)
	}
	functionSelector := "0x" + common.Bytes2Hex(function.ID)
	for _, tx := range qres.Txs {
		if strings.Compare(tx.Func, functionSelector) == 0 && tx.To.Cmp(AddressUniswapRouter02) == 0 {
			fmt.Println(tx.TxHash.Hex())
			argMap := make(map[string]interface{})
			err = params.UnpackIntoMap(argMap, tx.Param)
			if err != nil {
				t.Error(err)
			}
			pathIntf, ok := argMap["path"]
			if !ok {
				t.Errorf("'path' not found in argMap\n")
			}
			for _, path := range pathIntf.([]common.Address) {
				fmt.Println(path.Hex())
			}
			fmt.Println(argMap["to"].(common.Address))
			fmt.Println(common.Bytes2Hex(tx.Param), tx.Func, tx.Value)
			break
		}
	}
}

func _TestUniswapV3ABI(t *testing.T) {
	const dataDir = "/data/test-data-param/test"
	const fileName = "16100000_16110000.json.zst"
	abi, err := abi.JSON(strings.NewReader(semantic.UniswapV3ISwapABI))
	if err != nil {
		t.Error(err)
	}
	function, ok := abi.Methods[semantic.UniswapV3Funcs[2]]
	if !ok {
		t.Errorf(semantic.UniswapV3Funcs[2] + "not found in abi.Methods\n")
	}
	//fmt.Println("get")
	params := function.Inputs
	qres, err := opensearch.LoadQueryResult(path.Join(dataDir, fileName))
	if err != nil {
		t.Error(err)
	}
	functionSelector := "0x" + common.Bytes2Hex(function.ID)
	//fmt.Println(functionSelector)
	found := false
	for _, tx := range qres.Txs {
		if strings.Compare(tx.Func, functionSelector) == 0 {
			found = true
			fmt.Println(tx.TxHash.Hex())
			argMap := make(map[string]interface{})
			err = params.UnpackIntoMap(argMap, tx.Param)
			if err != nil {
				t.Error(err)
			}
			paramsIntf, ok := argMap["params"]
			if !ok {
				t.Errorf("'params' not found in argMap\n")
			}
			//paramsStruct := paramsIntf.(semantic.UniswapV3ISwap_ExactInputParams)
			temp0, err := json.Marshal(paramsIntf)
			if err != nil {
				t.Error(err)
			}
			paramsStruct := &semantic.UniswapV3ISwap_ExactInputSingleParams{}
			err = json.Unmarshal(temp0, paramsStruct)
			if err != nil {
				t.Error(err)
			}
			fmt.Println(common.Bytes2Hex(tx.Param), tx.Func, tx.Value)
			fmt.Println(paramsStruct)
			fmt.Println(paramsStruct.AmountIn, paramsStruct.TokenIn, paramsStruct.TokenOut)
			break
		}
	}
	fmt.Println(found)
}

func TestUniswapUniversal(t *testing.T) {
	const dataDir = "/data/test-data-param/test"
	const fileName = "16100000_16110000.json.zst"
	qres, err := opensearch.LoadQueryResult(path.Join(dataDir, fileName))
	if err != nil {
		t.Error(err)
	}
	count := 0
	functionSelector := "0x3593564c"
	routerAddress1 := common.HexToAddress("0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD")
	routerAddress2 := common.HexToAddress("0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B")
	for _, tx := range qres.Txs {
		if strings.Compare(tx.Func, functionSelector) == 0 || tx.To.Cmp(routerAddress1) == 0 || tx.To.Cmp(routerAddress2) == 0 {
			fmt.Println(tx.TxHash.Hex())
			fmt.Println(common.Bytes2Hex(tx.Param))
			fmt.Println(tx.Func)
			//fmt.Println(tx.Value)
			count++
		}
		if count > 100 {
			break
		}
	}
	fmt.Println(count)
}
*/

// swap.go
/*
type swapParser func(paramABI abi.Arguments, functionName string, argBytes []byte, msgValue *hexutil.Big) (common.Address, common.Address, common.Address, *hexutil.Big, error)

func addSwapHook(
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	tsSlice []*model.Transfer,
	contractABIStrings []string,
	functionNamess [][]string,
	parseFunctions []swapParser) (map[string][]*model.Transfer, []*model.Transfer, error) {

	functionNameMap := make(map[string]string)
	paramABIMap := make(map[string]abi.Arguments)
	parseFunctionMap := make(map[string]swapParser)
	for i, contractABIString := range contractABIStrings {
		contractABI, err := abi.JSON(strings.NewReader(contractABIString))
		if err != nil {
			return nil, nil, fmt.Errorf("abi.JSON() failed: %s", err.Error())
		}
		for _, functionName := range functionNamess[i] {
			functionABI, ok := contractABI.Methods[functionName]
			if !ok {
				return nil, nil, fmt.Errorf("function '%s' not found in contractABI", functionName)
			}
			functionSelector := "0x" + common.Bytes2Hex(functionABI.ID)
			functionNameMap[functionSelector] = functionName
			paramABIMap[functionSelector] = functionABI.Inputs
			parseFunctionMap[functionSelector] = parseFunctions[i]
		}
	}

	for txMapKey, txs := range txMap {
		fromAddress := getFromAddressOfTxMapKey(txMapKey)
		for _, tx := range txs {
			functionName, ok := functionNameMap[tx.Func]
			if !ok {
				continue
			}
			toAddress, tokenA, tokenB, value, err := (parseFunctionMap[tx.Func])(paramABIMap[tx.Func], functionName, tx.Param, tx.Value)
			if err != nil {
				return nil, nil, fmt.Errorf("parse() failed: %s", err.Error())
			}
			swapTransferA := &model.Transfer{
				Pos:   tx.Pos(),
				Type:  uint16(model.TransferTypeSwap),
				From:  fromAddress,
				To:    toAddress,
				Token: tokenA,
				Value: value,
			}
			tsMapKeyA := makeTsMapKey(fromAddress, toAddress, tokenA)
			if _, ok := tsMap[tsMapKeyA]; !ok {
				tsMap[tsMapKeyA] = make([]*model.Transfer, 0, 1)
			}
			tsMap[tsMapKeyA] = append(tsMap[tsMapKeyA], swapTransferA)
			tsSlice = append(tsSlice, swapTransferA)
			if tokenA.Cmp(tokenB) != 0 {
				swapTransferB := &model.Transfer{
					Pos:   tx.Pos(),
					Type:  uint16(model.TransferTypeSwap),
					From:  fromAddress,
					To:    toAddress,
					Token: tokenB,
					Value: value,
				}
				tsMapKeyB := makeTsMapKey(fromAddress, toAddress, tokenB)
				if _, ok := tsMap[tsMapKeyB]; !ok {
					tsMap[tsMapKeyB] = make([]*model.Transfer, 0, 1)
				}
				tsMap[tsMapKeyB] = append(tsMap[tsMapKeyB], swapTransferB)
				tsSlice = append(tsSlice, swapTransferB)
			}
		}
	}

	return tsMap, tsSlice, nil
}

func parseUniswapV2(paramABI abi.Arguments, functionName string, argBytes []byte, msgValue *hexutil.Big) (common.Address, common.Address, common.Address, *hexutil.Big, error) {
	argMap := make(map[string]interface{}, 16)
	if err := paramABI.UnpackIntoMap(argMap, argBytes); err != nil {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV2} paramsABI.UnpackIntoMap() failed: %s", err.Error())
	}
	pathIntf, ok := argMap["path"]
	if !ok {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV2} 'path' not found in argMap")
	}
	toIntf, ok := argMap["to"]
	if !ok {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV2} 'to' not found in argMap")
	}
	path := pathIntf.([]common.Address)
	tokenA := path[0]
	tokenB := path[len(path)-1]
	return toIntf.(common.Address), tokenA, tokenB, (*hexutil.Big)(big.NewInt(0)), nil
}

func parseUniswapV3ISwap(paramABI abi.Arguments, functionName string, argBytes []byte, msgValue *hexutil.Big) (common.Address, common.Address, common.Address, *hexutil.Big, error) {
	argMap := make(map[string]interface{}, 8)
	if err := paramABI.UnpackIntoMap(argMap, argBytes); err != nil {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} paramsABI.UnpackIntoMap() failed: %s", err.Error())
	}
	paramStructIntf, ok := argMap["params"]
	if !ok {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} 'params' not found in argMap")
	}
	paramStructBytes, err := json.Marshal(paramStructIntf)
	if err != nil {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} json.Marshal() failed : %s", err.Error())
	}
	var to, tokenA, tokenB common.Address
	switch functionName {
	case UniswapV3Funcs[0]:
		paramsStruct := &UniswapV3ISwap_ExactInputParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = common.BytesToAddress(paramsStruct.Path[0:20])
		tokenB = common.BytesToAddress(paramsStruct.Path[len(paramsStruct.Path)-20:])
	case UniswapV3Funcs[1]:
		paramsStruct := &UniswapV3ISwap_ExactOutputParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = common.BytesToAddress(paramsStruct.Path[0:20])
		tokenB = common.BytesToAddress(paramsStruct.Path[len(paramsStruct.Path)-20:])
	case UniswapV3Funcs[2]:
		paramsStruct := &UniswapV3ISwap_ExactInputSingleParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = paramsStruct.TokenIn
		tokenB = paramsStruct.TokenOut
	case UniswapV3Funcs[3]:
		paramsStruct := &UniswapV3ISwap_ExactOutputSingleParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3ISwap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = paramsStruct.TokenIn
		tokenB = paramsStruct.TokenOut
	}
	return to, tokenA, tokenB, (*hexutil.Big)(big.NewInt(0)), nil
}

func parseUniswapV3IV3Swap(paramABI abi.Arguments, functionName string, argBytes []byte, msgValue *hexutil.Big) (common.Address, common.Address, common.Address, *hexutil.Big, error) {
	argMap := make(map[string]interface{}, 8)
	if err := paramABI.UnpackIntoMap(argMap, argBytes); err != nil {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} paramsABI.UnpackIntoMap() failed: %s", err.Error())
	}
	paramStructIntf, ok := argMap["params"]
	if !ok {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} 'params' not found in argMap")
	}
	paramStructBytes, err := json.Marshal(paramStructIntf)
	if err != nil {
		return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} json.Marshal() failed : %s", err.Error())
	}
	var to, tokenA, tokenB common.Address
	switch functionName {
	case UniswapV3Funcs[0]:
		paramsStruct := &UniswapV3IV3Swap_ExactInputParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3V3ISwap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = common.BytesToAddress(paramsStruct.Path[0:20])
		tokenB = common.BytesToAddress(paramsStruct.Path[len(paramsStruct.Path)-20:])
	case UniswapV3Funcs[1]:
		paramsStruct := &UniswapV3IV3Swap_ExactOutputParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = common.BytesToAddress(paramsStruct.Path[0:20])
		tokenB = common.BytesToAddress(paramsStruct.Path[len(paramsStruct.Path)-20:])
	case UniswapV3Funcs[2]:
		paramsStruct := &UniswapV3IV3Swap_ExactInputSingleParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = paramsStruct.TokenIn
		tokenB = paramsStruct.TokenOut
	case UniswapV3Funcs[3]:
		paramsStruct := &UniswapV3IV3Swap_ExactOutputSingleParams{}
		err = json.Unmarshal(paramStructBytes, paramsStruct)
		if err != nil {
			return model.EmptyAddress, model.EmptyAddress, model.EmptyAddress, nil, fmt.Errorf("{parseUniswapV3IV3Swap} json.Unmarshal() failed : %s", err.Error())
		}
		to = paramsStruct.Recipient
		tokenA = paramsStruct.TokenIn
		tokenB = paramsStruct.TokenOut
	}
	return to, tokenA, tokenB, (*hexutil.Big)(big.NewInt(0)), nil
}
*/

// utils.go
/*
func mergeTwoSubgraph(a, b *model.Subgraph) (*model.Subgraph, error) {
	subgraphs := make([]*model.Subgraph, 2)
	subgraphs[0] = a
	subgraphs[1] = b
	return model.MergeSubgraphs(subgraphs)
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
		ret.NodePtrs[i+1] = ret.NodePtrs[i] + uint32(len(row))
	}
	return ret
}
*/

func AddWithinTx_Legency(
	txMap map[string][]*model.Tx,
	tsMap map[string][]*model.Transfer,
	tsSlice []*model.Transfer,
	outDegreeAll, outDegreeToken map[string]int,
	tsMapByPos map[uint64][]*model.Transfer) (map[string][]*model.Transfer, []*model.Transfer) {
	//DEBUG := true
	superNodeOutDegreeLimit := model.SuperNodeOutDegreeLimitLevel1
	for txMapKey, txs := range txMap {
		toAddress := txMapKey[len(txMapKey)/2:]
		if outDegreeAll[toAddress] < superNodeOutDegreeLimit {
			continue
		}
		for _, tx := range txs {
			txPos := tx.Pos()
			tss, ok := tsMapByPos[txPos]
			if !ok {
				continue
			}
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
			superNodeID, ok := addressMap[toAddress]
			if !ok {
				continue
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
			fromAddress := getFromAddressOfTxMapKey(txMapKey)
			fromAddressString := txMapKey[:len(txMapKey)/2]
			/*
				if DEBUG {
					fmt.Println(" ")
					fmt.Println(fromAddress.Hex(), " ", common.BytesToAddress([]byte(toAddress)).Hex())
					fmt.Println(" ")
					for id := range closure {
						touchedAddress := common.BytesToAddress([]byte(rMap[id])).Hex()
						fmt.Println(touchedAddress)
					}
					fmt.Println(" ")
					for _, ts := range tss {
						fmt.Println(ts.From.Hex(), " ", ts.To.Hex())
					}
					fmt.Println(" ")
					DEBUG = false
				}
			*/
			for id := range closure {
				if outDegreeAll[rMap[id]] > superNodeOutDegreeLimit {
					continue
				}
				touchedAddress := common.BytesToAddress([]byte(rMap[id]))
				for token := range tokenSet {
					if degree, ok := outDegreeToken[token+toAddress]; ok && degree > superNodeOutDegreeLimit {
						transfer := &model.Transfer{
							Pos:   txPos,
							Type:  uint16(model.TransferVirtualTypeWithinTx),
							Token: common.BytesToAddress([]byte(token)),
							From:  fromAddress,
							To:    touchedAddress,
							Value: (*hexutil.Big)(big.NewInt(0)),
						}
						tsMapKey := token + fromAddressString + rMap[id]
						if _, ok := tsMap[tsMapKey]; !ok {
							tsMap[tsMapKey] = make([]*model.Transfer, 0, 1)
						}
						tsMap[tsMapKey] = append(tsMap[tsMapKey], transfer)
						tsSlice = append(tsSlice, transfer)
					}
				}
			}
		}
	}
	return tsMap, tsSlice
}
