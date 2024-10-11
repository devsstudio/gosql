package services_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/devsstudio/gosql/request"
	"github.com/devsstudio/gosql/services"
	"github.com/devsstudio/gosql/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func setupMockDB() (*gorm.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})

	return gormDB, mock, err
}

func TestPaginationService_FindAll(t *testing.T) {
	db, mock, err := setupMockDB()
	assert.NoError(t, err)

	defer mock.ExpectClose()

	where := "name = :name"
	columns := types.Columns{"id": "ID", "name": "Name"}
	baseParams := types.ListParams{
		Table:        "users",
		Columns:      columns,
		Where:        &where,
		Placeholders: map[string]any{"name": "John"},
	}

	paginationService := services.PaginationService(db, baseParams)

	mock.ExpectQuery("SELECT ID as id, Name as name FROM users WHERE name = \\?").
		WithArgs("John").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John Doe"))

	filters := []request.FilterRequest{}
	findRequest := request.FindRequest{}
	items, err := paginationService.FindAll(filters, findRequest, nil)

	assert.NoError(t, err)
	assert.NotNil(t, items)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, "John Doe", items[0]["name"])
}

func TestPaginationService_FindSelect2(t *testing.T) {
	db, mock, err := setupMockDB()
	assert.NoError(t, err)

	defer mock.ExpectClose()

	where := "name LIKE :name"
	columns := types.Columns{"id": "ID", "name": "Name"}
	baseParams := types.ListParams{
		Table:        "users",
		Columns:      columns,
		Where:        &where,
		Placeholders: map[string]any{"name": "%John%"},
	}

	paginationService := services.PaginationService(db, baseParams)

	mock.ExpectQuery("SELECT ID as value, Name as label FROM users WHERE name LIKE \\?").
		WithArgs("%John%").
		WillReturnRows(sqlmock.NewRows([]string{"value", "label"}).
			AddRow(1, "John Doe"))

	filters := []request.FilterRequest{}
	infiniteScroll := request.InfiniteScrollRequest{}
	select2Resp, err := paginationService.FindSelect2(filters, infiniteScroll, "id", "name")

	assert.NoError(t, err)
	assert.NotNil(t, select2Resp)
	assert.Equal(t, 1, len(select2Resp.Items))
	assert.Equal(t, "John Doe", select2Resp.Items[0]["label"])
}

func TestPaginationService_FindPaginated(t *testing.T) {
	db, mock, err := setupMockDB()
	assert.NoError(t, err)

	defer mock.ExpectClose()

	where := "name = :name"
	columns := types.Columns{"id": "ID", "name": "Name"}
	baseParams := types.ListParams{
		Table:        "users",
		Columns:      columns,
		Where:        &where,
		Placeholders: map[string]any{"name": "John"},
	}

	paginationService := services.PaginationService(db, baseParams)

	mock.ExpectQuery("SELECT ID as id, Name as name FROM users WHERE name = \\?").
		WithArgs("John").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John Doe"))

	filters := []request.FilterRequest{}
	pagination := request.PaginationRequest{Page: 1, Limit: 10, Count: false}
	paginatedResp, err := paginationService.FindPaginated(filters, pagination, nil)

	assert.NoError(t, err)
	assert.NotNil(t, paginatedResp)
	assert.Equal(t, 1, len(paginatedResp.Items))
	assert.Equal(t, "John Doe", paginatedResp.Items[0]["name"])
	assert.Equal(t, 1, paginatedResp.TotalPages)
	assert.Equal(t, 0, paginatedResp.TotalItems)
}
