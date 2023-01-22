package lexer

type TokenType uint8

const (
	Integer TokenType = iota + 1
	String
	Delimiter
	Keyword
	Identifier
)

type Token struct {
	ttype TokenType
	val   interface{}
}

func NewToken(ttype TokenType, val interface{}) Token {
	return Token{ttype, val}
}
