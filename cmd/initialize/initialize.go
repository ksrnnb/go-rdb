package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/server"
)

func initializeFiles() {
	os.RemoveAll(filepath.Join(file.ProjectRootDir(), "simpledb"))
}

func main() {
	initializeFiles()
	db := server.NewSimpleDBWithMetadata("simpledb")
	pe := db.PlanExecuter()

	queries := []string{
		"create table users (uid int, uname varchar(16))",
		"create table pictures (pid int, user_id int, title varchar(16))",
		"create index pictures_user_id_idx on pictures (user_id)",
		"create index pictures_pid_idx on pictures (pid)",
		"create index users_uid_idx on users (uid)",
	}

	txn, err := db.NewTransaction()
	if err != nil {
		panic(err)
	}

	for _, q := range queries {
		_, err := pe.ExecuteUpdate(q, txn)
		if err != nil {
			panic(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	for i := 1; i < 100000; i++ {
		txn, err := db.NewTransaction()
		if err != nil {
			panic(err)
		}

		q1 := fmt.Sprintf("insert into users (uid, uname) values (%d, 'user%d')", i, i)
		q2 := fmt.Sprintf("insert into pictures (pid, user_id, title) values (%d, %d, 'title%d')", i, i, i)

		fmt.Printf("executing: %s\n", q1)
		_, err = pe.ExecuteUpdate(q1, txn)
		if err != nil {
			panic(err)
		}
		fmt.Printf("executing: %s\n", q2)
		_, err = pe.ExecuteUpdate(q2, txn)
		if err != nil {
			panic(err)
		}

		err = txn.Commit()
		if err != nil {
			panic(err)
		}

		// go func(q1 string, q2 string, tx *tx.Transaction) {
		// 	fmt.Printf("executing: %s\n", q1)
		// 	_, err = pe.ExecuteUpdate(q1, txn)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	fmt.Printf("executing: %s\n", q2)
		// 	_, err = pe.ExecuteUpdate(q2, txn)
		// 	if err != nil {
		// 		panic(err)
		// 	}

		// 	err = txn.Commit()
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }(q1, q2, txn)
	}
}
