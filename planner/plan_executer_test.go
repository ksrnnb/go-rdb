package planner_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/require"
)

func initializeFiles(t *testing.T) {
	t.Helper()
	err := os.RemoveAll("../data")
	require.NoError(t, err)
}

func TestPlanExecuter(t *testing.T) {
	initializeFiles(t)

	db := server.NewSimpleDBWithMetadata("data")
	pe := db.PlanExecuter()
	tx, err := db.NewTransaction()
	require.NoError(t, err)

	// Part1: Create table
	cq := "create table student (sid int, sname varchar(16), gradyear int)"
	_, err = pe.ExecuteUpdate(cq, tx)
	require.NoError(t, err)

	// Part2: Insert Data
	iq1 := "insert into student (sid, sname, gradyear) values (1, 'user1', 2020)"
	_, err = pe.ExecuteUpdate(iq1, tx)
	require.NoError(t, err)

	iq2 := "insert into student (sid, sname, gradyear) values (2, 'user2', 2020)"
	_, err = pe.ExecuteUpdate(iq2, tx)
	require.NoError(t, err)

	// Part3: Select Data
	sq := "select sid, sname, gradyear from student"
	p, err := pe.CreateQueryPlan(sq, tx)
	require.NoError(t, err)
	s, err := p.Open()
	require.NoError(t, err)
	hasNext, err := s.Next()
	require.NoError(t, err)
	for hasNext {
		sid, err := s.GetInt("sid")
		require.NoError(t, err)
		sname, err := s.GetString("sname")
		require.NoError(t, err)
		gradYear, err := s.GetInt("gradyear")
		require.NoError(t, err)
		fmt.Printf("sid: %d, sname: %s, gradyear:%d\n", sid, sname, gradYear)
		newHasNext, err := s.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}
	err = s.Close()
	require.NoError(t, err)

	// Part4: Create Index
	iq := "create index student_sid_idx on student (sid)"
	_, err = pe.ExecuteUpdate(iq, tx)
	require.NoError(t, err)

	// Part4: Update Data
	uq := "update student set sname='user001' where sname='user1'"
	_, err = pe.ExecuteUpdate(uq, tx)
	require.NoError(t, err)

	// Part5: Delete Data
	dq := "delete from student where sname='user2'"
	_, err = pe.ExecuteUpdate(dq, tx)
	require.NoError(t, err)

	// Part6: Select Data
	sq = "select sname, gradyear from student"
	p, err = pe.CreateQueryPlan(sq, tx)
	require.NoError(t, err)
	s, err = p.Open()
	require.NoError(t, err)
	hasNext, err = s.Next()
	require.NoError(t, err)
	for hasNext {
		sname, err := s.GetString("sname")
		require.NoError(t, err)
		gradYear, err := s.GetInt("gradyear")
		require.NoError(t, err)
		fmt.Printf("%s %d\n", sname, gradYear)
		newHasNext, err := s.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}
	err = s.Close()
	require.NoError(t, err)
}
