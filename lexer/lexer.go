package lexer

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Lexer struct {
	reader io.RuneScanner
	pos    int
	tokens []Token
}

var ErrEatToken = errors.New("eat token error")

var keywords = []string{
	"select",
	"from",
	"where",
	"and",
	"insert",
	"into",
	"values",
	"delete",
	"update",
	"set",
	"create",
	"table",
	"varchar",
	"int",
	"view",
	"as",
	"index",
	"on",
}

func NewLexer(query string) (*Lexer, error) {
	l := &Lexer{
		reader: bufio.NewReader(strings.NewReader(strings.ToLower(query))),
		pos:    0,
	}
	if err := l.Tokenize(); err != nil {
		return nil, err
	}
	return l, nil
}

// TODO: Match*** や Eat*** は Lexer のやること？？
func (l *Lexer) MatchDelimiter(d rune) bool {
	v, ok := l.currentToken().val.(rune)
	if !ok {
		return false
	}
	return v == d
}

func (l *Lexer) MatchIntConstant() bool {
	return l.currentToken().ttype == Integer
}

func (l *Lexer) MatchStringConstant() bool {
	return l.currentToken().ttype == String
}

func (l *Lexer) MatchKeyword(k string) bool {
	tok := l.currentToken()
	return tok.ttype == Keyword && tok.val == k
}

func (l *Lexer) MatchIdentifier() bool {
	return l.currentToken().ttype == Identifier
}

func (l *Lexer) EatDelimiter(d rune) error {
	if !l.MatchDelimiter(d) {
		return ErrEatToken
	}
	l.nextToken()
	return nil
}

func (l *Lexer) EatIntConstant() (int, error) {
	if !l.MatchIntConstant() {
		return 0, ErrEatToken
	}
	i := l.currentToken().val.(int)
	l.nextToken()
	return i, nil
}

func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", ErrEatToken
	}
	s := l.currentToken().val.(string)
	l.nextToken()
	return s, nil
}

func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return ErrEatToken
	}
	l.nextToken()
	return nil
}

func (l *Lexer) EatIdentifier() (string, error) {
	if !l.MatchIdentifier() {
		return "", ErrEatToken
	}
	s := l.currentToken().val.(string)
	l.nextToken()
	return s, nil
}

func (l *Lexer) Tokenize() error {
	for {
		err := l.tokenize()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	return nil
}

func (l *Lexer) CurrentTokenValue() interface{} {
	return l.currentToken().val
}

func (l *Lexer) tokenize() error {
	err := l.skipWhiteSpace()
	if err != nil {
		return err
	}

	r, _, err := l.readRune()
	if err != nil {
		return err
	}

	if isDelimiter(r) {
		l.tokens = append(l.tokens, NewToken(Delimiter, r))
		return nil
	}

	err = l.unreadRune()
	if err != nil {
		return err
	}

	// 先頭文字が - の場合は負の数値
	if isNumeric(r) || r == '-' {
		num, err := l.readInteger()
		if err != nil {
			return err
		}
		l.tokens = append(l.tokens, NewToken(Integer, num))
		return nil
	}

	if isAlphabet(r) {
		idt, err := l.readIdentifier()
		if err != nil {
			return err
		}

		if isKeyword(idt) {
			l.tokens = append(l.tokens, NewToken(Keyword, idt))
			return nil
		}

		l.tokens = append(l.tokens, NewToken(Identifier, idt))
		return nil
	}

	if r == '\'' {
		s, err := l.readString()
		if err != nil {
			return err
		}
		l.tokens = append(l.tokens, NewToken(String, s))
		return nil
	}

	return fmt.Errorf("rune %s cannot tokenize", string(r))
}

// readInteger は rune 配列に数値を読み込んで、最後に数値に変換する
func (l *Lexer) readInteger() (int, error) {
	rs := make([]rune, 0)
	r, _, err := l.readRune()
	if err != nil {
		return 0, err
	}

	if r == '-' {
		rs = append(rs, r)
	} else {
		if err := l.unreadRune(); err != nil {
			return 0, err
		}
	}

	for {
		r, _, err := l.readRune()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}

		if isDelimiter(r) || isWhiteSpace(r) {
			err := l.unreadRune()
			if err != nil {
				return 0, err
			}
			break
		}

		if !isNumeric(r) {
			return 0, fmt.Errorf("number is required, but got %s", string(r))
		}
		rs = append(rs, r)
	}

	val := string(rs)
	return strconv.Atoi(val)
}

// readIdentifier は識別子を読み込む
func (l *Lexer) readIdentifier() (string, error) {
	rs := make([]rune, 0)
	for {
		r, _, err := l.readRune()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return "", err
		}

		if !isAlphaNumeric(r) {
			err := l.unreadRune()
			if err != nil {
				return "", err
			}
			break
		}

		rs = append(rs, r)
	}
	return string(rs), nil
}

// readString は文字列を読み込む
func (l *Lexer) readString() (string, error) {
	r, _, err := l.readRune()
	if err != nil {
		return "", err
	}
	if r != '\'' {
		return "", fmt.Errorf("start rune must be \"'\", but got %s", string(r))
	}

	rs := make([]rune, 0)
	for {
		r, _, err := l.readRune()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", fmt.Errorf("string is not terminated")
			}
			return "", err
		}

		if r == '\'' {
			break
		}
		rs = append(rs, r)
	}
	return string(rs), nil
}

func (l *Lexer) skipWhiteSpace() error {
	for {
		r, _, err := l.readRune()
		if err != nil {
			return err
		}

		if isWhiteSpace(r) {
			continue
		}

		// whitespace の次の次の文字まで進めてしまっているので1文字分戻す
		err = l.unreadRune()
		if err != nil {
			return err
		}
		break
	}
	return nil
}

// readRune は現在の位置から1文字読み取って、読み込み位置を1文字進める
func (l *Lexer) readRune() (rune, int, error) {
	return l.reader.ReadRune()
}

// unreadRune は読み込み位置を1文字戻す
func (l *Lexer) unreadRune() error {
	return l.reader.UnreadRune()
}

func (l *Lexer) currentToken() Token {
	return l.tokens[l.pos]
}

func (l *Lexer) nextToken() {
	if l.pos == len(l.tokens)-1 {
		return
	}
	l.pos++
}

func isKeyword(k string) bool {
	for _, kw := range keywords {
		if k == kw {
			return true
		}
	}
	return false
}

func isDelimiter(r rune) bool {
	switch r {
	case '=', ',', '(', ')', '*':
		return true
	}
	return false
}

func isNumeric(r rune) bool {
	return r >= '0' && r <= '9'
}

func isAlphabet(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
}

func isAlphaNumeric(r rune) bool {
	return isAlphabet(r) || isNumeric(r)
}

func isWhiteSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\r', '\n':
		return true
	}
	return false
}
