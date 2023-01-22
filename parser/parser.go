package parser

import (
	"errors"

	"github.com/ksrnnb/go-rdb/lexer"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
)

type Parser struct {
	lex *lexer.Lexer
}

func NewParser(query string) (*Parser, error) {
	lex, err := lexer.NewLexer(query)
	if err != nil {
		return nil, err
	}
	return &Parser{lex}, nil
}

func (p *Parser) Field() (string, error) {
	return p.lex.EatIdentifier()
}

func (p *Parser) Constant() (query.Constant, error) {
	if p.lex.MatchStringConstant() {
		sc, err := p.lex.EatStringConstant()
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(sc), nil
	} else if p.lex.MatchIntConstant() {
		ic, err := p.lex.EatIntConstant()
		if err != nil {
			return query.Constant{}, err
		}
		return query.NewConstant(ic), nil
	} else {
		return query.Constant{}, errors.New("invalid constant type")
	}

}

func (p *Parser) Expression() (query.Expression, error) {
	if p.lex.MatchIdentifier() {
		field, err := p.Field()
		if err != nil {
			return query.Expression{}, err
		}
		return query.NewExpressionFromFieldName(field), nil
	} else {
		c, err := p.Constant()
		if err != nil {
			return query.Expression{}, err
		}
		return query.NewExpressionFromConstant(c), nil
	}
}

func (p *Parser) Term() (query.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return query.Term{}, err
	}
	err = p.lex.EatDelimiter('=')
	if err != nil {
		return query.Term{}, err
	}
	rhs, err := p.Expression()
	if err != nil {
		return query.Term{}, err
	}
	return query.NewTerm(lhs, rhs), nil
}

func (p *Parser) Predicate() (*query.Predicate, error) {
	t, err := p.Term()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicateFromTerm(t)
	if p.lex.MatchKeyword("and") {
		err = p.lex.EatKeyword("and")
		if err != nil {
			return nil, err
		}
		// 再帰的に Predicate を呼び出す
		nextPred, err := p.Predicate()
		if err != nil {
			return nil, err
		}
		pred.ConJoinWith(nextPred)
	}
	return pred, nil
}

func (p *Parser) Query() (*QueryData, error) {
	err := p.lex.EatKeyword("select")
	if err != nil {
		return nil, err
	}

	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}

	err = p.lex.EatKeyword("from")
	if err != nil {
		return nil, err
	}

	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		err := p.lex.EatKeyword("where")
		if err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewQueryData(fields, tables, pred), nil
}

func (p *Parser) UpdateCommand() (interface{}, error) {
	if p.lex.MatchKeyword("insert") {
		return p.Insert()
	}
	if p.lex.MatchKeyword("delete") {
		return p.Delete()
	}
	if p.lex.MatchKeyword("update") {
		return p.Modify()
	}
	return p.Create()
}

func (p *Parser) Insert() (*InsertData, error) {
	// TODO: err のハンドリング多すぎ。 Parser に err を持たせた方が良さそう
	err := p.lex.EatKeyword("insert")
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("into")
	if err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter('(')
	if err != nil {
		return nil, err
	}
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter(')')
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("values")
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter('(')
	if err != nil {
		return nil, err
	}
	values, err := p.constList()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter(')')
	if err != nil {
		return nil, err
	}
	return NewInsertData(tableName, fields, values), nil
}

func (p *Parser) Delete() (*DeleteData, error) {
	err := p.lex.EatKeyword("delete")
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("from")
	if err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		err := p.lex.EatKeyword("where")
		if err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(tableName, pred), nil
}

func (p *Parser) Modify() (*ModifyData, error) {
	err := p.lex.EatKeyword("update")
	if err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("set")
	if err != nil {
		return nil, err
	}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter('=')
	if err != nil {
		return nil, err
	}
	newVal, err := p.Expression()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		err := p.lex.EatKeyword("where")
		if err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewModifyData(tableName, field, newVal, pred), nil
}

func (p *Parser) fieldList() ([]string, error) {
	f, err := p.Field()
	if err != nil {
		return nil, err
	}
	flist := []string{f}
	if p.lex.MatchDelimiter(',') {
		err := p.lex.EatDelimiter(',')
		if err != nil {
			return nil, err
		}
		remainFList, err := p.fieldList()
		if err != nil {
			return nil, err
		}
		flist = append(flist, remainFList...)
	}
	return flist, nil
}

func (p *Parser) constList() ([]query.Constant, error) {
	c, err := p.Constant()
	if err != nil {
		return nil, err
	}
	list := []query.Constant{c}
	if p.lex.MatchDelimiter(',') {
		err := p.lex.EatDelimiter(',')
		if err != nil {
			return nil, err
		}
		remainList, err := p.constList()
		if err != nil {
			return nil, err
		}
		list = append(list, remainList...)
	}
	return list, nil
}

func (p *Parser) Create() (interface{}, error) {
	err := p.lex.EatKeyword("create")
	if err != nil {
		return nil, err
	}

	if p.lex.MatchKeyword("table") {
		return p.createTable()
	} else if p.lex.MatchKeyword("view") {
		return p.createView()
	} else if p.lex.MatchKeyword("index") {
		return p.createIndex()
	}

	return nil, errors.New("invalid create keyword")
}

func (p *Parser) createTable() (*CreateTableData, error) {
	err := p.lex.EatKeyword("table")
	if err != nil {
		return nil, err
	}
	tn, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter('(')
	if err != nil {
		return nil, err
	}
	schema, err := p.fieldDefinitions()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter(')')
	if err != nil {
		return nil, err
	}
	return NewCreateTableData(tn, schema), nil
}

func (p *Parser) createView() (*CreateViewData, error) {
	err := p.lex.EatKeyword("view")
	if err != nil {
		return nil, err
	}
	viewName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("as")
	if err != nil {
		return nil, err
	}
	qd, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewName, qd), nil
}

func (p *Parser) createIndex() (*CreateIndexData, error) {
	err := p.lex.EatKeyword("index")
	if err != nil {
		return nil, err
	}
	indexName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatKeyword("on")
	if err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter('(')
	if err != nil {
		return nil, err
	}
	fieldName, err := p.Field()
	if err != nil {
		return nil, err
	}
	err = p.lex.EatDelimiter(')')
	if err != nil {
		return nil, err
	}
	return NewCreateIndexData(indexName, tableName, fieldName), nil
}

func (p *Parser) fieldDefinitions() (*record.Schema, error) {
	fd, err := p.fieldDefinition()
	if err != nil {
		return nil, err
	}

	if p.lex.MatchDelimiter(',') {
		err := p.lex.EatDelimiter(',')
		if err != nil {
			return nil, err
		}
		remainFDs, err := p.fieldDefinitions()
		if err != nil {
			return nil, err
		}
		fd.AddAll(remainFDs)
	}
	return fd, nil
}

func (p *Parser) fieldDefinition() (*record.Schema, error) {
	fn, err := p.Field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(fn)
}

func (p *Parser) fieldType(fieldName string) (*record.Schema, error) {
	schema := record.NewSchema()
	if p.lex.MatchKeyword("int") {
		err := p.lex.EatKeyword("int")
		if err != nil {
			return nil, err
		}
		schema.AddIntField(fieldName)
	} else if p.lex.MatchKeyword("varchar") {
		err := p.lex.EatKeyword("varchar")
		if err != nil {
			return nil, err
		}
		err = p.lex.EatDelimiter('(')
		if err != nil {
			return nil, err
		}
		length, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}
		err = p.lex.EatDelimiter(')')
		if err != nil {
			return nil, err
		}
		schema.AddStringField(fieldName, length)
	} else {
		return nil, errors.New("invalid field type")
	}

	return schema, nil
}

func (p *Parser) selectList() ([]string, error) {
	f, err := p.Field()
	if err != nil {
		return nil, err
	}
	list := []string{f}
	if p.lex.MatchDelimiter(',') {
		err := p.lex.EatDelimiter(',')
		if err != nil {
			return nil, err
		}
		remainList, err := p.selectList()
		if err != nil {
			return nil, err
		}
		list = append(list, remainList...)
	}
	return list, nil
}

func (p *Parser) tableList() ([]string, error) {
	table, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}
	tables := []string{table}

	if p.lex.MatchDelimiter(',') {
		err := p.lex.EatDelimiter(',')
		if err != nil {
			return nil, err
		}
		remainTables, err := p.tableList()
		if err != nil {
			return nil, err
		}
		tables = append(tables, remainTables...)
	}

	return tables, nil
}
