package parser

type CreateViewData struct {
	viewName string
	qd       *QueryData
}

func NewCreateViewData(viewName string, qd *QueryData) *CreateViewData {
	return &CreateViewData{viewName, qd}
}

func (c *CreateViewData) ViewName() string {
	return c.viewName
}

func (c *CreateViewData) ViewDefinition() string {
	return c.qd.String()
}
