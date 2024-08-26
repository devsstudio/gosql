package request

import "github.com/devsstudio/gosql/types"

type FilterRequest struct {
	Type  string   `json:"limit" validate:"omitempty,oneof=SIMPLE COLUMN SUB BETWEEN NOT_BETWEEN IN NOT_IN NULL NOT_NULL DATE DATE_BETWEEN NUMERIC TERM"`
	Attr  string   `json:"attr" validate:"omitempty"`
	Attrs []string `json:"attrs" validate:"omitempty"`
	Val   string   `json:"val" validate:"omitempty"`
	Vals  []string `json:"vals" validate:"omitempty"`
	Opr   string   `json:"opr" validate:"omitempty,oneof== <> > >= < <= LIKE ILIKE"`
	Conn  string   `json:"conn" validate:"omitempty,oneof=AND OR"`
}

type PaginationRequest struct {
	Count bool        `json:"count" validate:"omitempty,boolean"`
	Page  int         `json:"page" validate:"omitempty,gte=1"`
	Limit int         `json:"limit" validate:"omitempty,gte=1,lte=50"`
	Order types.Order `json:"order" validate:"omitempty"`
}

type PaginationOffsetRequest struct {
	Offset int         `json:"offset" validate:"omitempty,min=0"`
	Limit  int         `json:"limit" validate:"omitempty,min=1,max=50"`
	Order  types.Order `json:"order" validate:"omitempty"`
}

type FindRequest struct {
	Limit int         `json:"limit" validate:"gte=1,lte=50,omitempty"`
	Order types.Order `json:"order,omitempty"`
}

type InfiniteScrollRequest struct {
	Page  int         `json:"page" validate:"gte=1,omitempty"`
	Limit int         `json:"limit" validate:"gte=1,lte=50,omitempty"`
	Order types.Order `json:"order,omitempty"`
}
