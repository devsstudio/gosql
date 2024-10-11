package types

type Columns map[string]string

type Order map[string]string

type Row map[string]interface{}

type ListParams struct {
	Columns      Columns
	Table        string
	Where        *string
	Group        *string
	Placeholders map[string]any
}
