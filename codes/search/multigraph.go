package search

import (
	"sort"
	"time"

	fgraph "github.com/yourbasic/graph"
)

type MEdge interface {
	From() string
	To() string
	Value() float64
	Pos() uint64
	After(MEdge) bool
	Before(MEdge) bool
	TimeDelta(MEdge) int64
}

type MEdges []MEdge

func (e MEdges) SortByValue() {
	sort.Slice(e, func(i, j int) bool {
		return e[i].Value() > e[j].Value()
	})
}

func (e MEdges) SortByTime() {
	sort.Slice(e, func(i, j int) bool {
		return e[i].TimeDelta(e[j]) < 0
	})
}

func (e MEdges) MinValue() float64 {
	if len(e) == 0 {
		return 0
	}
	min := e[0].Value()
	for _, edge := range e {
		if edge.Value() < min {
			min = edge.Value()
		}
	}
	return min
}

func (e MEdges) maxValue() float64 {
	if len(e) == 0 {
		return 0
	}
	max := e[0].Value()
	for _, edge := range e {
		if edge.Value() > max {
			max = edge.Value()
		}
	}
	return max
}

func (e MEdges) timeSpan() [2]uint64 {
	if len(e) == 0 {
		return [2]uint64{0, 0}
	}
	min := e[0].Pos()
	max := e[0].Pos()
	for _, edge := range e {
		if edge.Pos() < min {
			min = edge.Pos()
		}
		if edge.Pos() > max {
			max = edge.Pos()
		}
	}
	return [2]uint64{min, max}
}

func (e MEdges) filterByTimeSpan(span [2]uint64) MEdges {
	var ret MEdges
	for _, edge := range e {
		if edge.Pos() >= span[0] && edge.Pos() <= span[1] {
			ret = append(ret, edge)
		}
	}
	return ret
}

type MultiGraph map[string]map[string]MEdges

func NewMultiGraph(edges MEdges) MultiGraph {
	ret := make(MultiGraph)
	for _, edge := range edges {
		from := edge.From()
		to := edge.To()
		if _, ok := ret[from]; !ok {
			ret[from] = make(map[string]MEdges)
		}
		if _, ok := ret[from][to]; !ok {
			ret[from][to] = make(MEdges, 0, 1)
		}
		ret[from][to] = append(ret[from][to], edge)
	}
	return ret
}

func (mg MultiGraph) FindPathDFS(src, des string, maxDepth int, minValue float64, count int, timeLimit time.Duration) ([]MEdges, int) {
	timeS := time.Now()
	if maxDepth <= 0 {
		return nil, 0
	}
	if _, ok := mg[src]; !ok {
		return nil, 0
	}
	visited := make(map[string]struct{})
	var ret []MEdges
	var dfs func(MEdges, string)
	dfs = func(path MEdges, cur string) {
		if time.Since(timeS) > timeLimit {
			return
		}
		if len(path) > maxDepth || len(ret) >= count {
			return
		}
		if cur == des {
			ret = append(ret, path)
			return
		}
		visited[cur] = struct{}{}
		for next, edges := range mg[cur] {
			edges.SortByValue()
			for _, edge := range edges {
				if edge.Value() < minValue || len(path) > 0 && !edge.After(path[len(path)-1]) {
					continue
				}
				if _, ok := visited[next]; !ok {
					nextPath := make(MEdges, len(path), maxDepth)
					copy(nextPath, path)
					dfs(append(nextPath, edge), next)
				}
			}
		}
		delete(visited, cur)
	}
	dfs(nil, src)
	supValue := float64(0)
	supValueIndex := -1
	for i, path := range ret {
		pValue := path.MinValue()
		if pValue > supValue {
			supValue = pValue
			supValueIndex = i
		}
	}
	return ret, supValueIndex
}

func (mg MultiGraph) MaxFlow(src, des string) uint64 {
	var addressMapCounter uint64 = 0
	addressMap := make(map[string]uint64)
	edgeMap := make(map[uint64][][2]uint64)
	for from, tos := range mg {
		if _, ok := addressMap[from]; !ok {
			addressMap[from] = addressMapCounter
			edgeMap[addressMapCounter] = make([][2]uint64, 0, 1)
			addressMapCounter++
		}
		for to, edges := range tos {
			if _, ok := addressMap[to]; !ok {
				addressMap[to] = addressMapCounter
				addressMapCounter++
			}
			var totalValue uint64 = 0
			for _, edge := range edges {
				totalValue += uint64(edge.Value())
			}
			edgeMap[addressMap[from]] = append(edgeMap[addressMap[from]], [2]uint64{addressMap[to], totalValue})
		}
	}
	source, ok := addressMap[src]
	if !ok {
		return 0
	}
	destination, ok := addressMap[des]
	if !ok {
		return 0
	}
	fg := fgraph.New(len(addressMap))
	for from, edges := range edgeMap {
		for _, edge := range edges {
			fg.AddCost(int(from), int(edge[0]), fg.Cost(int(from), int(edge[0]))+int64(edge[1]))
		}
	}
	ifg := fgraph.Sort(fg)
	maxFlow, _ := fgraph.MaxFlow(ifg, int(source), int(destination))
	return uint64(maxFlow)
}

type TimeSpanPath struct {
	Addresses []string
	TimeSpans [][2]uint64
	Edges     []MEdges

	supMinTime uint64
}

func (tsp *TimeSpanPath) deepCopy() *TimeSpanPath {
	ret := &TimeSpanPath{
		Addresses:  make([]string, len(tsp.Addresses)),
		TimeSpans:  make([][2]uint64, len(tsp.TimeSpans)),
		supMinTime: tsp.supMinTime,
	}
	copy(ret.Addresses, tsp.Addresses)
	copy(ret.TimeSpans, tsp.TimeSpans)
	return ret
}

func (tsp *TimeSpanPath) addEdge(from, to string, timeSpan [2]uint64) {
	if len(tsp.Addresses) == 0 {
		tsp.Addresses = append(tsp.Addresses, from)
	} else if tsp.Addresses[len(tsp.Addresses)-1] != from {
		panic("invalid edge")
	}
	tsp.Addresses = append(tsp.Addresses, to)
	tsp.TimeSpans = append(tsp.TimeSpans, timeSpan)
	if timeSpan[0] > tsp.supMinTime {
		tsp.supMinTime = timeSpan[0]
	}
}

func (tsp *TimeSpanPath) computeTimeSpan() {
	if len(tsp.TimeSpans) == 0 {
		return
	}
	supMin := tsp.TimeSpans[0][0]
	infMax := tsp.TimeSpans[len(tsp.TimeSpans)-1][1]
	for i := 0; i < len(tsp.TimeSpans); i++ {
		if tsp.TimeSpans[i][0] > supMin {
			supMin = tsp.TimeSpans[i][0]
		} else {
			tsp.TimeSpans[i][0] = supMin
		}
	}
	for i := len(tsp.TimeSpans) - 1; i >= 0; i-- {
		if tsp.TimeSpans[i][1] < infMax {
			infMax = tsp.TimeSpans[i][1]
		} else {
			tsp.TimeSpans[i][1] = infMax
		}
	}
}

func (tsp *TimeSpanPath) definiteTimeSpan() {
	for i := range tsp.TimeSpans {
		tsp.Edges[i].SortByTime()
		tsp.TimeSpans[i] = [2]uint64{tsp.Edges[i][0].Pos(), tsp.Edges[i][len(tsp.Edges[i])-1].Pos()}
	}
}

func (mg MultiGraph) timeSpanGraph() map[string]map[string][2]uint64 {
	ret := make(map[string]map[string][2]uint64)
	for from, tos := range mg {
		if _, ok := ret[from]; !ok {
			ret[from] = make(map[string][2]uint64)
		}
		for to, edges := range tos {
			ret[from][to] = edges.timeSpan()
		}
	}
	return ret
}

func (mg MultiGraph) FindTimeSpanPath(src, des string, maxDepth int, minValue float64, count int, timeLimit time.Duration) []*TimeSpanPath {
	timeS := time.Now()
	timeSpanGraph := mg.timeSpanGraph()
	visited := make(map[string]struct{})
	var ret []*TimeSpanPath
	var dfs func(*TimeSpanPath, string)
	dfs = func(thisPath *TimeSpanPath, this string) {
		if time.Since(timeS) > timeLimit {
			return
		}
		if len(thisPath.TimeSpans) > maxDepth || len(ret) >= count {
			return
		}
		if this == des {
			ret = append(ret, thisPath)
			return
		}
		visited[this] = struct{}{}
		for next, timeSpan := range timeSpanGraph[this] {
			if timeSpan[1] < thisPath.supMinTime || mg[this][next].maxValue() < minValue {
				continue
			}
			if _, ok := visited[next]; !ok {
				nextPath := thisPath.deepCopy()
				nextPath.addEdge(this, next, timeSpan)
				dfs(nextPath, next)
			}
		}
		delete(visited, this)
	}
	dfs(&TimeSpanPath{
		Addresses: []string{src},
	}, src)
	fret := make([]*TimeSpanPath, 0, len(ret))
p:
	for _, path := range ret {
		path.computeTimeSpan()
		path.Edges = make([]MEdges, len(path.TimeSpans))
		for i, timeSpan := range path.TimeSpans {
			path.Edges[i] = mg[path.Addresses[i]][path.Addresses[i+1]].filterByTimeSpan(timeSpan)
			if len(path.Edges[i]) == 0 {
				continue p
			}
		}
		path.definiteTimeSpan()
		fret = append(fret, path)
	}
	return fret
}

func (mg MultiGraph) SubgraphBySrcDes(srcs, dess []string, maxDepth int, minValue float64, count int, timeLimit time.Duration) MultiGraph {
	ret := make(MultiGraph)
	timeSpan := make(map[string]map[string][2]uint64)
	for _, src := range srcs {
		for _, des := range dess {
			paths := mg.FindTimeSpanPath(src, des, maxDepth, minValue, count, timeLimit)
			for _, path := range paths {
				for i := 0; i < len(path.Edges); i++ {
					from := path.Addresses[i]
					to := path.Addresses[i+1]
					if _, ok := timeSpan[from]; !ok {
						timeSpan[from] = make(map[string][2]uint64)
					}
					if _, ok := timeSpan[from][to]; !ok {
						timeSpan[from][to] = path.TimeSpans[i]
					} else {
						if path.TimeSpans[i][0] < timeSpan[from][to][0] {
							timeSpan[from][to] = [2]uint64{path.TimeSpans[i][0], timeSpan[from][to][1]}
						}
						if path.TimeSpans[i][1] > timeSpan[from][to][1] {
							timeSpan[from][to] = [2]uint64{timeSpan[from][to][0], path.TimeSpans[i][1]}
						}
					}
				}
			}
		}
	}
	for from, toMap := range timeSpan {
		if _, ok := ret[from]; !ok {
			ret[from] = make(map[string]MEdges)
		}
		for to, span := range toMap {
			edges := mg[from][to].filterByTimeSpan(span)
			if len(edges) > 0 {
				edges.SortByTime()
				ret[from][to] = edges
			}
		}
	}
	return ret
}
