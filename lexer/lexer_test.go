package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		want     []interface{}
		wantType []TokenType
	}{
		{
			name:     "select statement",
			query:    "select a, b from users where id=3",
			want:     []interface{}{"select", "a", ',', "b", "from", "users", "where", "id", '=', 3},
			wantType: []TokenType{Keyword, Identifier, Delimiter, Identifier, Keyword, Identifier, Keyword, Identifier, Delimiter, Integer},
		},
		{
			name:     "update statement",
			query:    "update users set a='hoge' where id=3",
			want:     []interface{}{"update", "users", "set", "a", '=', "hoge", "where", "id", '=', 3},
			wantType: []TokenType{Keyword, Identifier, Keyword, Identifier, Delimiter, String, Keyword, Identifier, Delimiter, Integer},
		},
		{
			name:     "select statement with uppercase",
			query:    "SELECT a, b FroM users wheRE id=3",
			want:     []interface{}{"select", "a", ',', "b", "from", "users", "where", "id", '=', 3},
			wantType: []TokenType{Keyword, Identifier, Delimiter, Identifier, Keyword, Identifier, Keyword, Identifier, Delimiter, Integer},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lex, err := NewLexer(tt.query)
			require.NoError(t, err)

			for i, token := range lex.tokens {
				assert.Equal(t, token.val, tt.want[i])
				assert.Equal(t, token.ttype, tt.wantType[i])
			}
		})
	}
}
