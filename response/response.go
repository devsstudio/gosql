package response

type PaginationResponse struct {
	Page       int                      `json:"page"`
	Limit      int                      `json:"limit"`
	TotalPages int                      `json:"totalPages"`
	TotalItems interface{}              `json:"totalItems"` // Ajusta el tipo según tus necesidades
	Items      []map[string]interface{} `json:"items"`
}

type Select2Response struct {
	Items []map[string]interface{} `json:"items"`
}

type PaginationOffsetResponse struct {
	Offset        int                      `json:"offset"`
	Limit         int                      `json:"limit"`
	TotalItems    int                      `json:"totalItems"`
	FilteredItems int                      `json:"filteredItems"`
	Items         []map[string]interface{} `json:"items"`
}
