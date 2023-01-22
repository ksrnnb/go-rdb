package query

import (
	"errors"
	"fmt"

	"github.com/ksrnnb/go-rdb/record"
)

var ErrNoSubPredicate = errors.New("predicate are not found")

type Predicate struct {
	terms []Term
}

func NewPredicate() *Predicate {
	return &Predicate{terms: make([]Term, 0)}
}

func NewPredicateFromTerm(t Term) *Predicate {
	return &Predicate{terms: []Term{t}}
}

func (p *Predicate) ConJoinWith(pp *Predicate) {
	p.terms = append(p.terms, pp.terms...)
}

func (p *Predicate) IsSatisfied(s Scanner) (bool, error) {
	for _, t := range p.terms {
		isSatisfied, err := t.IsSatisfied(s)
		if err != nil {
			return false, err
		}
		if !isSatisfied {
			return false, nil
		}
	}
	return true, nil
}

func (p *Predicate) ReductionFactor(planner Planner) int {
	factor := 1
	for _, t := range p.terms {
		factor *= t.ReductionFactor(planner)
	}
	return factor
}

func (p *Predicate) SelectSubPredicate(schema *record.Schema) (*Predicate, error) {
	newP := NewPredicate()
	for _, t := range p.terms {
		if t.AppliesTo(schema) {
			newP.terms = append(newP.terms, t)
		}
	}
	if len(newP.terms) == 0 {
		return nil, ErrNoSubPredicate
	}
	return newP, nil
}

func (p *Predicate) JoinSubPredicate(schema1 *record.Schema, schema2 *record.Schema) (*Predicate, error) {
	newP := NewPredicate()
	newSchema := record.NewSchema()
	err := newSchema.AddAll(schema1)
	if err != nil {
		return nil, err
	}
	err = newSchema.AddAll(schema2)
	if err != nil {
		return nil, err
	}

	for _, t := range p.terms {
		if !t.AppliesTo(schema1) && !t.AppliesTo(schema2) && t.AppliesTo(newSchema) {
			newP.terms = append(newP.terms, t)
		}
	}
	if len(newP.terms) == 0 {
		return nil, ErrNoSubPredicate
	}
	return newP, nil
}

func (p *Predicate) EquatesWithConstant(fieldName string) Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fieldName)
		if c.ctype != UnknownConstant {
			return c
		}
	}
	return Constant{}
}

func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, t := range p.terms {
		s := t.EquatesWithFieldName(fieldName)
		if s != "" {
			return s
		}
	}
	return ""
}

func (p *Predicate) String() string {
	var s string
	for i, t := range p.terms {
		if i == 0 {
			s = t.String()
			continue
		}
		s = fmt.Sprintf("%s and %s", s, t.String())
	}
	return s
}
