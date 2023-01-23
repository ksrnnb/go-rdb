package planner

import (
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
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
