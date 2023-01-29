package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/ksrnnb/go-rdb/parser"
	"github.com/ksrnnb/go-rdb/planner"
	q "github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/ksrnnb/go-rdb/tx"
)

var db *server.SimpleDB
var txns = make(map[string]*tx.Transaction)

func main() {
	db = server.NewSimpleDBWithMetadata("simpledb")

	http.HandleFunc("/", HandleQuery)

	fmt.Println("DB is running at localhost:8888")
	log.Fatal(http.ListenAndServe(":8888", nil))
}

type QueryRequest struct {
	Query         string `json:"query"`
	TransactionID string `json:"transaction_id"`
}

func (qr QueryRequest) IsSelect() bool {
	q := strings.ToLower(qr.Query)
	return strings.HasPrefix(q, "select")
}

func (qr QueryRequest) IsStartTransaction() bool {
	q := strings.ToLower(qr.Query)
	return q == "start transaction" || q == "begin"
}

func (qr QueryRequest) IsInTransaction() bool {
	_, ok := txns[qr.TransactionID]
	return ok
}

func (qr QueryRequest) IsCommit() bool {
	q := strings.ToLower(qr.Query)
	return q == "commit"
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

	var req QueryRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.IsCommit() {
		tx := txns[req.TransactionID]
		if err := tx.Commit(); err != nil {
			handleError(w, r, err)
			return
		}

		delete(txns, req.TransactionID)
		fmt.Fprint(w, MakeMessageResponse("Commit!!"))
		return
	}

	if req.IsStartTransaction() {
		tx, err := db.NewTransaction()
		if err != nil {
			handleError(w, r, err)
			return
		}
		tid := generateTransactionID()
		txns[tid] = tx
		res := make(map[string]string)
		res["transaction_id"] = tid
		res["message"] = "start transaction"
		b, _ := json.Marshal(res)
		fmt.Fprint(w, string(b))
		return
	}

	var tx *tx.Transaction
	if req.IsInTransaction() {
		tx = txns[req.TransactionID]
	} else {
		tx, err = db.NewTransaction()
		if err != nil {
			handleError(w, r, err)
			return
		}
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
		if !req.IsInTransaction() {
			if err := tx.Commit(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
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
	if !req.IsInTransaction() {
		err = tx.Commit()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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

func generateTransactionID() string {
	return uuid.New().String()
}
