package parser

import (
	"testing"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPred(t *testing.T, fieldName string, val interface{}) *query.Predicate {
	t.Helper()

	lhs := query.NewExpressionFromFieldName(fieldName)
	rhs := query.NewExpressionFromConstant(query.NewConstant(val))
	term := query.NewTerm(lhs, rhs)
	return query.NewPredicateFromTerm(term)
}

func TestParser_Query(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *QueryData
	}{
		{
			name:  "select simple query",
			query: "select a, b from users where id=3",
			wantFunc: func(t *testing.T) *QueryData {
				return NewQueryData(
					[]string{"a", "b"},
					[]string{"users"},
					newPred(t, "id", 3),
				)
			},
		},
		{
			name:  "select 2 tables query",
			query: "select a, b from users, pictures",
			wantFunc: func(t *testing.T) *QueryData {
				return NewQueryData(
					[]string{"a", "b"},
					[]string{"users", "pictures"},
					query.NewPredicate(),
				)
			},
		},
		{
			name:  "select multi predicates query",
			query: "select a, b from users where id=3 and name='hoge'",
			wantFunc: func(t *testing.T) *QueryData {
				p := newPred(t, "id", 3)
				p.ConJoinWith(newPred(t, "name", "hoge"))

				return NewQueryData(
					[]string{"a", "b"},
					[]string{"users", "pictures"},
					p,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			qd, err := p.Query()
			require.NoError(t, err)

			wantQD := tt.wantFunc(t)

			for i, f := range qd.Fields() {
				assert.Equal(t, f, wantQD.Fields()[i])
			}
			for i, f := range qd.Tables() {
				assert.Equal(t, f, wantQD.Tables()[i])
			}
			assert.Equal(t, qd.Predicate().String(), wantQD.Predicate().String())
		})
	}
}

func TestParser_Insert(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func() *InsertData
	}{
		{
			name:  "insert query",
			query: "insert into users (id, name) values (3, 'hoge')",
			wantFunc: func() *InsertData {
				return NewInsertData(
					"users",
					[]string{"id", "name"},
					[]query.Constant{query.NewConstant(3), query.NewConstant("hoge")},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyID, err := p.UpdateCommand()
			require.NoError(t, err)
			id, ok := anyID.(*InsertData)
			require.True(t, ok)
			wantID := tt.wantFunc()

			assert.Equal(t, id.TableName(), wantID.TableName())

			for i, f := range id.Fields() {
				assert.Equal(t, f, wantID.Fields()[i])
			}
			for i, v := range id.Values() {
				assert.True(t, v.Equals(wantID.Values()[i]))
			}
		})
	}
}

func TestParser_Delete(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *DeleteData
	}{
		{
			name:  "delete query",
			query: "delete from users where id=3 and name='hoge'",
			wantFunc: func(t *testing.T) *DeleteData {
				p := newPred(t, "id", 3)
				p.ConJoinWith(newPred(t, "name", "hoge"))

				return NewDeleteData(
					"users",
					p,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyDD, err := p.UpdateCommand()
			require.NoError(t, err)
			dd, ok := anyDD.(*DeleteData)
			require.True(t, ok)
			wantDD := tt.wantFunc(t)

			assert.Equal(t, dd.TableName(), wantDD.TableName())
			assert.Equal(t, dd.Predicate().String(), wantDD.Predicate().String())

		})
	}
}

func TestParser_Modify(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *ModifyData
	}{
		{
			name:  "update query",
			query: "update users set name='piyopiyo' where id=3",
			wantFunc: func(t *testing.T) *ModifyData {
				return NewModifyData(
					"users",
					"name",
					query.NewExpressionFromConstant(query.NewConstant("piyopiyo")),
					newPred(t, "id", 3),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyMD, err := p.UpdateCommand()
			require.NoError(t, err)
			md, ok := anyMD.(*ModifyData)
			require.True(t, ok)
			wantMD := tt.wantFunc(t)

			assert.Equal(t, md.TableName(), wantMD.TableName())
			assert.Equal(t, md.TargetField(), wantMD.TargetField())
			assert.Equal(t, md.NewValue().String(), wantMD.NewValue().String())
			assert.Equal(t, md.Predicate().String(), wantMD.Predicate().String())

		})
	}
}

func TestParser_createTable(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *CreateTableData
	}{
		{
			name:  "create table query",
			query: "create table users (id int, name varchar(16))",
			wantFunc: func(t *testing.T) *CreateTableData {
				schema := record.NewSchema()
				schema.AddIntField("id")
				schema.AddStringField("name", 16)
				return NewCreateTableData(
					"users",
					schema,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyCTD, err := p.UpdateCommand()
			require.NoError(t, err)
			ctd, ok := anyCTD.(*CreateTableData)
			require.True(t, ok)
			wantCTD := tt.wantFunc(t)

			assert.Equal(t, ctd.TableName(), wantCTD.TableName())
			for i, f := range ctd.Schema().Fields() {
				assert.Equal(t, f, wantCTD.Schema().Fields()[i])

				l, err := ctd.Schema().Length(f)
				require.NoError(t, err)
				wantl, err := wantCTD.Schema().Length(f)
				require.NoError(t, err)
				assert.Equal(t, l, wantl)
			}

		})
	}
}

func TestParser_createIndex(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *CreateIndexData
	}{
		{
			name:  "create index query",
			query: "create index picture_user_id_idx on pictures (user_id)",
			wantFunc: func(t *testing.T) *CreateIndexData {
				return NewCreateIndexData(
					"picture_user_id_idx",
					"pictures",
					"user_id",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyCID, err := p.UpdateCommand()
			require.NoError(t, err)
			cid, ok := anyCID.(*CreateIndexData)
			require.True(t, ok)
			wandCID := tt.wantFunc(t)

			assert.Equal(t, cid.IndexName(), wandCID.IndexName())
			assert.Equal(t, cid.TableName(), wandCID.TableName())
			assert.Equal(t, cid.FieldName(), wandCID.FieldName())
		})
	}
}

func TestParser_createView(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFunc func(t *testing.T) *CreateViewData
	}{
		{
			name:  "create view query",
			query: "create view users_id_3 as select a, b from users where id=3",
			wantFunc: func(t *testing.T) *CreateViewData {
				return NewCreateViewData(
					"users_id_3",
					NewQueryData(
						[]string{"a", "b"},
						[]string{"users"},
						newPred(t, "id", 3),
					),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.query)
			require.NoError(t, err)
			anyCVD, err := p.UpdateCommand()
			require.NoError(t, err)
			cvd, ok := anyCVD.(*CreateViewData)
			require.True(t, ok)
			wandCVD := tt.wantFunc(t)

			assert.Equal(t, cvd.ViewName(), wandCVD.ViewName())
			assert.Equal(t, cvd.ViewDefinition(), wandCVD.ViewDefinition())
		})
	}
}
