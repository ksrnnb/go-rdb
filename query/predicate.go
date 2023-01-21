package query

type Predicate struct {
	terms []Term
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
