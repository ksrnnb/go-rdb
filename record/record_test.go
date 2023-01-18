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

func TestRecordPage(t *testing.T) {
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

	blk, err := tx.Append("testfile")
	require.NoError(t, err)

	err = tx.Pin(blk)
	require.NoError(t, err)

	rp := record.NewRecordPage(tx, blk, layout)
	rp.Format()

	fmt.Println("Filling the page with random records.")

	slot, err := rp.InsertAfter(-1)
	require.NoError(t, err)

	for slot >= 0 {
		n := rand.Intn(50)
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n))
		fmt.Printf("inserting into slot %d: {%d, rec%d}\n", slot, n, n)
		slot, err = rp.InsertAfter(slot)
		require.NoError(t, err)
	}

	fmt.Println("Deleted these records with A-values < 25.")
	count := 0
	slot, err = rp.NextAfter(-1)
	require.NoError(t, err)

	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		require.NoError(t, err)
		b, err := rp.GetString(slot, "B")
		require.NoError(t, err)
		if a < 25 {
			count++
			fmt.Printf("deleting slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot, err = rp.NextAfter(slot)
		require.NoError(t, err)
	}

	fmt.Printf("%d values under 25 were deleted\n", count)
	fmt.Printf("Here are the remaining records\n")
	slot, err = rp.NextAfter(-1)
	require.NoError(t, err)

	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		require.NoError(t, err)
		b, err := rp.GetString(slot, "B")
		require.NoError(t, err)

		fmt.Printf("over 25 slot %d: {%d, %s}\n", slot, a, b)

		assert.GreaterOrEqual(t, a, 25)

		slot, err = rp.NextAfter(slot)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Unpin(blk))
	require.NoError(t, tx.Commit())
}
