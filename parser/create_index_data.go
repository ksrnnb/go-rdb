package parser

type CreateIndexData struct {
	indexName string
	tableName string
	fieldName string
}

func NewCreateIndexData(indexName, tableName, fieldName string) *CreateIndexData {
	return &CreateIndexData{indexName, tableName, fieldName}
}

func (c *CreateIndexData) IndexName() string {
	return c.indexName
}

func (c *CreateIndexData) TableName() string {
	return c.tableName
}

func (c *CreateIndexData) FieldName() string {
	return c.fieldName
}
