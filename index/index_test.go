package index_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/planner"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initializeFiles(t *testing.T) {
	t.Helper()
	err := os.RemoveAll("../data")
	require.NoError(t, err)
}

func prepareIndexTestData(t *testing.T, db *server.SimpleDB) {
	t.Helper()

	tx, err := db.NewTransaction()
	require.NoError(t, err)

	// Create table
	pe := db.PlanExecuter()
	_, err = pe.ExecuteUpdate("create table student (sid int, sname varchar(16), gradyear int, major_id int)", tx)
	require.NoError(t, err)

	// Create index
	_, err = pe.ExecuteUpdate("create index student_major_id_index on student (major_id)", tx)
	require.NoError(t, err)

	// Insert User data
	// TODO: 50回くらいテストすると fail する。たぶん StatInfo の refresh 起因？
	for i := 0; i < 30; i++ {
		query := fmt.Sprintf("insert into student (sid, sname, gradyear, major_id) values (%d, 'user%d', 2020, %d)", i, i, i%5)
		_, err = pe.ExecuteUpdate(query, tx)
		require.NoError(t, err)
	}

	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func TestIndexRetrieval(t *testing.T) {
	initializeFiles(t)
	db := server.NewSimpleDBWithMetadata("data")
	prepareIndexTestData(t, db)

	tx, err := db.NewTransaction()
	require.NoError(t, err)
	mdm := db.MetadataManager()

	// Open an scan on the data table
	studentPlan, err := planner.NewTablePlan(tx, "student", mdm)
	require.NoError(t, err)
	studentScan, err := studentPlan.Open()
	require.NoError(t, err)
	sc, ok := studentScan.(query.UpdateScanner)
	require.True(t, ok)

	// Open the index on MajorId
	indexes, err := mdm.GetIndexInfo("student", tx)
	require.NoError(t, err)
	ii, ok := indexes["major_id"]
	require.True(t, ok)
	idx, err := ii.Open()
	require.NoError(t, err)

	// Retrieve all index records having a data_value of 3
	err = idx.BeforeFirst(query.NewConstant(3))
	require.NoError(t, err)

	hasNext, err := idx.Next()
	require.NoError(t, err)

	for hasNext {
		// Use the datarid to go to the coressponding STUDENT record.
		datarid, err := idx.GetDataRid()
		require.NoError(t, err)
		err = sc.MoveToRid(datarid)
		require.NoError(t, err)
		sname, err := sc.GetString("sname")
		require.NoError(t, err)
		fmt.Printf("sname: %s\n", sname)

		newHasNext, err := idx.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}

	err = idx.Close()
	require.NoError(t, err)
	err = studentScan.Close()
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func TestIndexUpdate(t *testing.T) {
	initializeFiles(t)

	db := server.NewSimpleDBWithMetadata("data")
	prepareIndexTestData(t, db)

	tx, err := db.NewTransaction()
	require.NoError(t, err)
	mdm := db.MetadataManager()

	sp, err := planner.NewTablePlan(tx, "student", mdm)
	require.NoError(t, err)
	studentScan, err := sp.Open()
	require.NoError(t, err)
	sc, ok := studentScan.(query.UpdateScanner)
	require.True(t, ok)

	// create a map containing all indexes for student
	indexes := make(map[string]index.Index)
	idxInfo, err := mdm.GetIndexInfo("student", tx)
	require.NoError(t, err)
	for fn, ii := range idxInfo {
		idx, err := ii.Open()
		require.NoError(t, err)
		indexes[fn] = idx
	}

	// Task1: insert a new student record for sam
	require.NoError(t, sc.Insert())
	require.NoError(t, sc.SetInt("sid", 10000))
	require.NoError(t, sc.SetString("sname", "sam"))
	require.NoError(t, sc.SetInt("gradyear", 2023))
	require.NoError(t, sc.SetInt("major_id", 3))
	datarid, err := sc.GetRid()
	require.NoError(t, err)
	for fn, idx := range indexes {
		dataval, err := sc.GetVal(fn)
		fmt.Printf("field: %s, dataval: %v\n", fn, dataval)
		require.NoError(t, err)
		require.NoError(t, idx.Insert(dataval, datarid))
	}

	// Task2: Find and delete sam's record
	require.NoError(t, sc.BeforeFirst())
	hasNext, err := sc.Next()
	require.NoError(t, err)
	for hasNext {
		sname, err := sc.GetString("sname")
		require.NoError(t, err)
		if sname != "sam" {
			newHasNext, err := sc.Next()
			require.NoError(t, err)
			hasNext = newHasNext
			continue
		}
		samRid, err := sc.GetRid()
		require.NoError(t, err)

		for fn, idx := range indexes {
			dataval, err := sc.GetVal(fn)
			require.NoError(t, err)
			require.NoError(t, idx.Delete(dataval, samRid))
		}
		require.NoError(t, sc.Delete())
		fmt.Printf("Deleted sam's record\n")
		break
	}

	// Print the records to verify updates
	require.NoError(t, sc.BeforeFirst())
	hasNext, err = sc.Next()
	require.NoError(t, err)
	for hasNext {
		sname, err := sc.GetString("sname")
		require.NoError(t, err)
		sid, err := sc.GetInt("sid")
		require.NoError(t, err)
		assert.NotEqual(t, sname, "sam")
		assert.NotEqual(t, sid, 10000)

		newHasNext, err := sc.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}

	require.NoError(t, sc.Close())
	for _, idx := range indexes {
		require.NoError(t, idx.Close())
	}
	require.NoError(t, tx.Commit())
}
