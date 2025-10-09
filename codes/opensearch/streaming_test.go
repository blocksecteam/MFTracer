package opensearch_test

import (
	"testing"
	"transfer-graph/opensearch"

	"github.com/stretchr/testify/require"
)

var path = "/root/transfer-graph/data_merged/2160001_2180000.json.zst"

func BenchmarkStreaming(b *testing.B) {
	for n := 0; n < b.N; n += 1 {
		_, err := opensearch.LoadQueryResult(path)
		require.NoError(b, err)
	}
}

func BenchmarkLegacy(b *testing.B) {
	for n := 0; n < b.N; n += 1 {
		_, err := opensearch.LoadQueryResultLegacy(path)
		require.NoError(b, err)
	}
}
