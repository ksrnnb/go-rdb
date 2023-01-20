package record_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableScan(t *testing.T) {
	db := server.NewSimpleDB("data", 400, 8)
	tx, err := db.NewTransaction()
	require.NoError(t, err)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout, err := record.NewLayout(schema)
	require.NoError(t, err)
	for _, fn := range layout.Schema().Fields() {
		ofs, err := layout.Offset(fn)
		require.NoError(t, err)
		fmt.Printf("%s has offset %d\n", fn, ofs)
	}

	ts, err := record.NewTableScan(tx, "T", layout)
	require.NoError(t, err)

	err = ts.BeforeFirst()
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		ts.Insert()
		n := rand.Intn(50)
		err := ts.SetInt("A", n)
		require.NoError(t, err)
		err = ts.SetString("B", fmt.Sprintf("rec%d", n))
		require.NoError(t, err)

		fmt.Printf("inserting into slot %s: {%d, rec%d}\n", ts.GetRid(), n, n)
	}

	count := 0
	err = ts.BeforeFirst()
	require.NoError(t, err)

	next, err := ts.Next()
	require.NoError(t, err)

	for next {
		a, err := ts.GetInt("A")
		require.NoError(t, err)
		b, err := ts.GetString("B")
		require.NoError(t, err)

		if a < 25 {
			count++
			fmt.Printf("[deleting] slot %s: {%d, %s}\n", ts.GetRid(), a, b)
			err = ts.Delete()
			require.NoError(t, err)
		}
		next, err = ts.Next()
		require.NoError(t, err)
	}

	fmt.Printf("%d values under 25 were deleted.\n", count)

	err = ts.BeforeFirst()
	require.NoError(t, err)

	next, err = ts.Next()
	require.NoError(t, err)
	for next {
		a, err := ts.GetInt("A")
		require.NoError(t, err)
		b, err := ts.GetString("B")
		require.NoError(t, err)
		fmt.Printf("[after delete] slot %s: {%d, %s}\n", ts.GetRid(), a, b)

		assert.GreaterOrEqual(t, a, 25)

		next, err = ts.Next()
		require.NoError(t, err)
	}

	err = ts.Close()
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
}
