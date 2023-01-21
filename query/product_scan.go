package query

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1, scan2 Scan) (*ProductScan, error) {
	_, err := scan1.Next()
	if err != nil {
		return nil, err
	}
	return &ProductScan{scan1, scan2}, nil
}

func (ps *ProductScan) BeforeFirst() error {
	err := ps.scan1.BeforeFirst()
	if err != nil {
		return err
	}
	_, err = ps.scan1.Next()
	if err != nil {
		return err
	}
	return ps.scan2.BeforeFirst()
}

func (ps *ProductScan) Next() (bool, error) {
	hasNext, err := ps.scan2.Next()
	if err != nil {
		return false, err
	}

	if hasNext {
		return true, nil
	}

	err = ps.scan2.BeforeFirst()
	if err != nil {
		return false, err
	}

	hasNext, err = ps.scan2.Next()
	if err != nil {
		return false, err
	}
	if !hasNext {
		return false, nil
	}

	hasNext, err = ps.scan1.Next()
	if err != nil {
		return false, err
	}
	return hasNext, nil
}

func (ps *ProductScan) GetInt(fieldName string) (int, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetInt(fieldName)
	}
	return ps.scan2.GetInt(fieldName)
}

func (ps *ProductScan) GetString(fieldName string) (string, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetString(fieldName)
	}
	return ps.scan2.GetString(fieldName)
}

func (ps *ProductScan) GetVal(fieldName string) (Constant, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetVal(fieldName)
	}
	return ps.scan2.GetVal(fieldName)
}

func (ps *ProductScan) HasField(fieldName string) bool {
	return ps.scan1.HasField(fieldName) || ps.scan2.HasField(fieldName)
}

func (ps *ProductScan) Close() error {
	err := ps.scan1.Close()
	if err != nil {
		return err
	}
	return ps.scan2.Close()
}
