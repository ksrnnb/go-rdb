package planner

import (
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type Planner interface {
	// Open は対応する scan を作成する
	Open() (query.Scanner, error)

	// BlocksAccessed はテーブルsで使われるブロック数 B(s) を計算する
	BlocksAccessed() int

	// RecordsOutput はテーブルsのレコード数 R(s) を計算する
	RecordsOutput() int

	// DistinctValues はテーブルsのフィールドFにおける distinct F-value V(s, F) を計算する
	DistinctValues(fieldName string) int

	// Schema は出力テーブルの schema を返す
	// query planner はこのスキーマを型の検証や最適な plan 選択に使用する
	Schema() *record.Schema
}

type QueryPlanner interface {
	CreatePlan(qd *parser.QueryData, tx *tx.Transaction) (Planner, error)
}

type UpdatePlanner interface {
	ExecuteDelete(dd *parser.DeleteData, tx *tx.Transaction) (int, error)
	ExecuteModify(md *parser.ModifyData, tx *tx.Transaction) (int, error)
	ExecuteInsert(id *parser.InsertData, tx *tx.Transaction) (int, error)
	ExecuteCreateTable(ctd *parser.CreateTableData, tx *tx.Transaction) (int, error)
	ExecuteCreateView(cvd *parser.CreateViewData, tx *tx.Transaction) (int, error)
	ExecuteCreateIndex(cid *parser.CreateIndexData, tx *tx.Transaction) (int, error)
}
