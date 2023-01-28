package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/planner"
	q "github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/server"
)

var db *server.SimpleDB

func main() {
	db = server.NewSimpleDBWithMetadata("simpledb")

	http.HandleFunc("/", HandleQuery)

	fmt.Println("DB is running at localhost:8888")
	log.Fatal(http.ListenAndServe(":8888", nil))
}

type QueryRequest struct {
	Query string `json:"query"`
}

func (qr QueryRequest) IsSelect() bool {
	q := strings.ToLower(qr.Query)
	return strings.HasPrefix(q, "select")
}

type MessageResponse struct {
	Message string `json:"message"`
}

func MakeMessageResponse(msg string) string {
	mr := MessageResponse{Message: msg}
	b, _ := json.Marshal(mr)
	return string(b)
}

func HandleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	tx, err := db.NewTransaction()
	if err != nil {
		handleError(w, r, err)
		return
	}

	var req QueryRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pe := db.PlanExecuter()
	if req.IsSelect() {
		p, err := pe.CreateQueryPlan(req.Query, tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		values, err := selectValues(p, req.Query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, _ := json.Marshal(values)
		fmt.Fprint(w, string(res))
		return
	}
	num, err := pe.ExecuteUpdate(req.Query, tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = tx.Commit()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, MakeMessageResponse("%d records has changed"), num)
}

func handleError(w http.ResponseWriter, r *http.Request, err error) {
	fmt.Fprint(w, MakeMessageResponse(err.Error()))
}

func selectValues(pl planner.Planner, query string) ([]map[string]interface{}, error) {
	pas, err := parser.NewParser(query)
	if err != nil {
		return nil, err
	}
	qd, err := pas.Query()
	if err != nil {
		return nil, err
	}
	s, err := pl.Open()
	if err != nil {
		return nil, err
	}
	hasNext, err := s.Next()
	if err != nil {
		return nil, err
	}
	fields := qd.Fields()
	values := make([]map[string]interface{}, 0)
	for hasNext {
		rec := make(map[string]interface{})
		for _, fn := range fields {
			val, err := s.GetVal(fn)
			if err != nil {
				return nil, err
			}
			switch val.ConstantType() {
			case q.IntConstant:
				rec[fn] = val.AsInt()
			case q.StringConstant:
				rec[fn] = val.AsString()
			}
		}
		values = append(values, rec)
		newHasNext, err := s.Next()
		if err != nil {
			return nil, err
		}
		hasNext = newHasNext
	}
	err = s.Close()
	if err != nil {
		return nil, err
	}
	return values, err
}
