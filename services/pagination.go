package services

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/devsstudio/gosql/helpers"
	"github.com/devsstudio/gosql/request"
	"github.com/devsstudio/gosql/response"
	"github.com/devsstudio/gosql/types"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"

	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"
)

type (
	Pagination struct {
		db                   *gorm.DB
		columns              types.Columns
		table                string
		originalWhere        string
		where                string
		group                string
		order                string
		offsetLimit          string
		originalPlaceholders []any
	}
)

func PaginationService(db *gorm.DB, baseParams types.ListParams) *Pagination {

	originalWhere := ""
	where := ""
	group := ""
	placeholders := []any{}
	// Adding WHERE
	if baseParams.Where != nil && len(strings.TrimSpace(baseParams.Table)) > 0 {
		originalWhere = *baseParams.Where
		where = *baseParams.Where
	} else {
		originalWhere = "1 = 1"
		where = "1 = 1"
	}

	// Adding GROUP
	if baseParams.Group != nil {
		group = "GROUP BY " + *baseParams.Group
	} else {
		group = ""
	}

	service := &Pagination{
		db:                   db,
		columns:              baseParams.Columns,
		table:                baseParams.Table,
		originalWhere:        originalWhere,
		where:                where,
		group:                group,
		originalPlaceholders: placeholders,
		order:                "",
		offsetLimit:          "",
	}
	service.table = service.replaceOriginalPlaceholders(baseParams.Table, baseParams.Placeholders, &service.originalPlaceholders)
	service.originalWhere = service.replaceOriginalPlaceholders(service.originalWhere, baseParams.Placeholders, &service.originalPlaceholders)
	return service
}

func (service *Pagination) FindAll(filters []request.FilterRequest, findRequest request.FindRequest, exclusions *[]string) ([]map[string]any, error) {
	placeholders := make([]any, len(service.originalPlaceholders))
	copy(placeholders, service.originalPlaceholders)

	var err error = nil
	service.where, err = service.getFilters(filters, service.originalWhere, &placeholders)
	if err != nil {
		return nil, err
	}

	service.offsetLimit = getLimit(findRequest)
	service.order = service.getOrder(findRequest.Order)

	cols, selectPairs := service.getSelectCols(exclusions)
	sql := service.getSql(selectPairs)

	// Ejecutar la consulta
	return service.getItems(sql, cols, placeholders)
}

func (service *Pagination) FindSelect2(filters []request.FilterRequest, infiniteScroll request.InfiniteScrollRequest, valueAttribute, textAttribute string) (*response.Select2Response, error) {
	placeholders := make([]any, len(service.originalPlaceholders))
	copy(placeholders, service.originalPlaceholders)

	var err error = nil
	service.where, err = service.getFilters(filters, service.originalWhere, &placeholders)
	if err != nil {
		return nil, err
	}

	service.offsetLimit = getInfiniteScroll(infiniteScroll)
	service.order = service.getOrder(infiniteScroll.Order)

	selectPairs := service.getSelect2Pairs(valueAttribute, textAttribute)
	sql := service.getSql(selectPairs)

	// Ejecutar la consulta
	items, err := service.getItems(sql, []string{"value", "label"}, placeholders)
	if err != nil {
		return nil, err
	}

	return &response.Select2Response{Items: items}, nil
}

func (service *Pagination) FindPaginated(filters []request.FilterRequest, pagination request.PaginationRequest, exclusions *[]string) (*response.PaginationResponse, error) {
	placeholders := make([]any, len(service.originalPlaceholders))
	copy(placeholders, service.originalPlaceholders)

	var err error = nil
	service.where, err = service.getFilters(filters, service.originalWhere, &placeholders)
	if err != nil {
		return nil, err
	}

	service.offsetLimit = getPagination(pagination)
	service.order = service.getOrder(pagination.Order)

	cols, selectPairs := service.getSelectCols(exclusions)
	sql := service.getSql(selectPairs)

	// Ejecutar la consulta
	items, err := service.getItems(sql, cols, placeholders)
	if err != nil {
		return nil, err
	}

	totalItems := 0
	if pagination.Count {
		totalItems = service.internalCount(placeholders)
	}

	totalPages := 1
	if pagination.Limit > 0 && totalItems > 0 {
		totalPages = int(math.Ceil(float64(totalItems) / float64(pagination.Limit)))
	}

	response := &response.PaginationResponse{
		Page:       pagination.Page,
		Limit:      pagination.Limit,
		TotalPages: totalPages,
		TotalItems: totalItems,
		Items:      items,
	}

	return response, nil
}

func (service *Pagination) FindPaginatedOffset(filters []request.FilterRequest, pagination request.PaginationOffsetRequest, exclusions *[]string) (*response.PaginationOffsetResponse, error) {
	placeholders := make([]any, len(service.originalPlaceholders))
	copy(placeholders, service.originalPlaceholders)

	var err error = nil
	// Contar sin filtros
	totalItems := 0
	if len(filters) > 0 {
		count, err := service.Count(nil) // Se pasa nil porque los filtros aún no están establecidos
		if err != nil {
			return nil, err
		}
		totalItems = count
	}

	// Preparar filtros y cláusulas
	service.where, err = service.getFilters(filters, "", &placeholders)
	if err != nil {
		return nil, err
	}

	service.offsetLimit = getPaginationOffset(pagination)
	service.order = service.getOrder(pagination.Order)

	cols, selectPairs := service.getSelectCols(exclusions)
	sql := service.getSql(selectPairs)

	// Ejecutar la consulta
	items, err := service.getItems(sql, cols, placeholders)
	if err != nil {
		return nil, err
	}

	// Contar los ítems filtrados
	filteredItems := service.internalCount(placeholders)
	if len(filters) == 0 {
		totalItems = filteredItems
	}

	return &response.PaginationOffsetResponse{
		Offset:        pagination.Offset,
		Limit:         pagination.Limit,
		TotalItems:    totalItems,
		FilteredItems: filteredItems,
		Items:         items,
	}, nil
}

func (service *Pagination) Count(filters []request.FilterRequest) (int, error) {
	placeholders := make([]any, len(service.originalPlaceholders))
	copy(placeholders, service.originalPlaceholders)

	var err error = nil
	service.where, err = service.getFilters(filters, service.originalWhere, &placeholders)
	if err != nil {
		return 0, err
	}

	return service.internalCount(placeholders), nil
}

func (service *Pagination) replaceOriginalPlaceholders(str string, originalPlaceholders map[string]any, placeholders *[]any) string {

	var keys []string
	// Extraemos las claves del mapa
	for key := range originalPlaceholders {
		keys = append(keys, key)
	}

	// Ordenamos las claves por longitud en orden descendente para evitar colisiones
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i]) > len(keys[j])
	})

	// Recorremos cada key-value del map
	for _, key := range keys {
		// Si encontramos el key en el string
		for strings.Contains(str, ":"+key) {
			// Reemplazamos la primera ocurrencia del key por el valor de reemplazo
			str = strings.Replace(str, ":"+key, service.setPlaceholder(placeholders, originalPlaceholders[key]), 1)
		}
	}
	return str
}

func (service *Pagination) internalCount(placeholders []any) int {
	sql := service.getCountSql()

	// Ejecuta la consulta y obtiene el resultado.
	var count int
	err := service.db.Raw(sql, placeholders...).Scan(&count).Error
	if err != nil {
		return 0
	}
	return count
}

func (service *Pagination) getSelectCols(exclusions *[]string) ([]string, []string) {
	exclusionSet := make(map[string]struct{})
	if exclusions != nil {
		for _, excl := range *exclusions {
			exclusionSet[excl] = struct{}{}
		}
	}

	var cols []string
	var selectPairs []string
	for colName, colValue := range service.columns {
		if _, excluded := exclusionSet[colName]; !excluded {
			cols = append(cols, colName)
			selectPairs = append(selectPairs, colValue+" as "+colName)
		}
	}

	// Si no hay pares seleccionados, incluimos todas las columnas.
	if len(selectPairs) == 0 {
		for colName, colValue := range service.columns {
			cols = append(cols, colName)
			selectPairs = append(selectPairs, colValue+" as "+colName)
		}
	}

	return cols, selectPairs
}

func (service *Pagination) getSelect2Pairs(valueAttribute, textAttribute string) []string {
	var selectPairs []string

	// Agrega el atributo de valor y la etiqueta al slice de selectPairs.
	if val, ok := service.columns[valueAttribute]; ok {
		selectPairs = append(selectPairs, fmt.Sprintf("%s as value", val))
	}
	if lbl, ok := service.columns[textAttribute]; ok {
		selectPairs = append(selectPairs, fmt.Sprintf("%s as label", lbl))
	}

	return selectPairs
}

func (service *Pagination) getSql(selectPairs []string) string {
	sql := "SELECT " +
		strings.Join(selectPairs, ", ") +
		" FROM " + service.table +
		" WHERE " + service.where +
		" " + service.group +
		" " + service.order +
		" " + service.offsetLimit

	return sql
}

func (service *Pagination) getCountSql() string {
	sql := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		service.getColumnCount(),
		service.table,
		service.where,
	)
	return sql
}

// getColumnCount construye la consulta para contar las columnas.
func (service *Pagination) getColumnCount() string {
	groupTrimmed := strings.TrimSpace(service.group)

	if len(groupTrimmed) > 0 {
		var distinct []string
		parts := strings.Split(strings.ReplaceAll(groupTrimmed, "GROUP BY", ""), ",")

		for _, part := range parts {
			cleanPart := strings.TrimSpace(part)
			cleanPart = strings.ReplaceAll(cleanPart, " ASC", "")
			cleanPart = strings.ReplaceAll(cleanPart, " DESC", "")
			cleanPart = strings.ReplaceAll(cleanPart, " asc", "")
			cleanPart = strings.ReplaceAll(cleanPart, " desc", "")
			distinct = append(distinct, cleanPart)
		}

		return fmt.Sprintf("COUNT(DISTINCT %s)", strings.Join(distinct, ", "))
	}

	return "COUNT(*)"
}

func (service *Pagination) getFilters(filters []request.FilterRequest, condition string, placeholders *[]any) (string, error) {
	if filters == nil {
		return "", errors.New("filters should be an array")
	}

	for _, filter := range filters {

		if err := service.verifyFilterRequest(&filter); err != nil {
			return "", err
		}

		// Procesamos filtro
		condition += service.processFilter(filter, condition, placeholders)
	}

	return condition, nil
}

func (service *Pagination) verifyFilterRequest(filter *request.FilterRequest) error {

	// Si no existe el tipo, lo seteamos por defecto
	if filter.Type != "" {
		filter.Type = strings.ToUpper(filter.Type)
	} else {
		filter.Type = "SIMPLE"
	}

	// Si no existe el conector, lo seteamos por defecto
	if filter.Conn != "" {
		filter.Conn = strings.ToUpper(filter.Conn)
	} else {
		filter.Conn = "AND"
	}

	// Si no existe el operador, lo seteamos por defecto
	if filter.Opr != "" {
		filter.Opr = strings.ToUpper(filter.Opr)
	} else {
		filter.Opr = "="
	}

	//Validamos
	validate := validator.New()
	err := validate.Struct(filter)
	if err != nil {
		return err
	}

	//Validaciones especificas para operador
	switch filter.Type {
	case "SIMPLE", "COLUMN":
		validOperators := []string{
			"=",
			"<>",
			">",
			">=",
			"<",
			"<=",
			"LIKE",
			"ILIKE",
		}

		validateOperator(validOperators, filter.Opr)

	case "NUMERIC":
		validOperators := []string{
			"=",
			"<>",
			">",
			">=",
			"<",
			"<=",
		}

		validateOperator(validOperators, filter.Opr)

	case "TERM":
		validOperators := []string{
			"LIKE",
			"ILIKE",
		}

		validateOperator(validOperators, filter.Opr)
	}

	//Validaciones especificas para atributo
	if filter.Type == "TERM" {
		if len(filter.Attrs) == 0 {
			return errors.New("attributes cannot be empty")
		}
		// Verificamos si es un valor válido
		for _, attr := range filter.Attrs {
			column := service.getColumn(attr)
			if column == nil {
				return errors.New("attribute filter '" + attr + "' is not allowed")
			}
		}
	} else {
		if len(filter.Attr) == 0 {
			return errors.New("attributes cannot be empty")
		}
		// Verificamos si es un valor válido
		column := service.getColumn(filter.Attr)
		if column == nil {
			return errors.New("attribute filter '" + filter.Attr + "' is not allowed")
		}
	}

	//Validaciones especificas para el valor
	switch filter.Type {
	case "BETWEEN", "DATE_BETWEEN":
		if len(filter.Vals) != 2 {
			return errors.New("vals should have two elements")
		}
	case "IN":
		if len(filter.Vals) > 0 {
			return errors.New("vals cannot be empty")
		}
	case "NUMERIC":
		if !isNumber(filter.Val) {
			return errors.New("val should be numeric")
		}
	case "COLUMN":
		if service.getColumn(filter.Val) == nil {
			return errors.New("unknown column '" + filter.Val)
		}
	default:
		if len(filter.Val) == 0 {
			return errors.New("val cannot be empty")
		}
	}

	return nil
}

func (service *Pagination) processFilter(filter request.FilterRequest, condition string, placeholders *[]any) string {
	switch filter.Type {
	case "SIMPLE":
		return service.processSimpleOrNumericFilter(filter, condition, placeholders)
	case "COLUMN":
		return service.processColumnFilter(filter, condition)
	case "BETWEEN":
		return service.processBetweenFilter(filter, false, condition, placeholders)
	case "NOT_BETWEEN":
		return service.processBetweenFilter(filter, true, condition, placeholders)
	case "IN":
		return service.processInFilter(filter, false, condition, placeholders)
	case "NOT_IN":
		return service.processInFilter(filter, true, condition, placeholders)
	case "NULL":
		return service.processNullFilter(filter, false, condition)
	case "NOT_NULL":
		return service.processNullFilter(filter, true, condition)
	case "TERM":
		return service.processTermFilter(filter, condition, placeholders)
	case "DATE":
		return service.processDateFilter(filter, condition, placeholders)
	case "NUMERIC":
		return service.processSimpleOrNumericFilter(filter, condition, placeholders)
	case "DATE_BETWEEN":
		return service.processDateBetweenFilter(filter, condition, placeholders)
	default:
		return "Unknown Filter Type"
	}
}

func (service *Pagination) getColumn(column string) *string {
	if val, exists := service.columns[column]; exists {
		return &val
	}
	return nil
}

func (service *Pagination) getOrder(order types.Order) string {
	var orderSQL []string
	for key, value := range order {
		if col, ok := service.columns[key]; ok {
			orderSQL = append(orderSQL, col+" "+value)
		}
	}
	if len(orderSQL) > 0 {
		return "ORDER BY " + strings.Join(orderSQL, ", ")
	}
	return ""
}

func (service *Pagination) processSimpleOrNumericFilter(filter request.FilterRequest, condition string, placeholders *[]any) string {
	column := *service.getColumn(filter.Attr)

	return fmt.Sprintf(
		" %s (%s %s %s)",
		getConn(filter.Conn, condition),
		column,
		filter.Opr,
		service.setPlaceholder(placeholders, filter.Val),
	)
}

func (service *Pagination) processColumnFilter(filter request.FilterRequest, condition string) string {
	// Verificamos que sea una columna válida
	column := *service.getColumn(filter.Attr)
	column2 := *service.getColumn(filter.Val)

	return fmt.Sprintf(
		" %s (%s %s %s)",
		getConn(filter.Conn, condition),
		column,
		filter.Opr,
		column2,
	)
}

func (service *Pagination) processBetweenFilter(filter request.FilterRequest, not bool, condition string, placeholders *[]any) string {

	// Creamos la columna
	column := *service.getColumn(filter.Attr)

	conn := getConn(filter.Conn, condition)
	if not {
		return fmt.Sprintf(" %s (%s NOT BETWEEN %s AND %s)",
			conn,
			column,
			service.setPlaceholder(placeholders, filter.Vals[0]),
			service.setPlaceholder(placeholders, filter.Vals[1]),
		)
	} else {
		return fmt.Sprintf(" %s (%s BETWEEN %s AND %s)",
			conn,
			column,
			service.setPlaceholder(placeholders, filter.Vals[0]),
			service.setPlaceholder(placeholders, filter.Vals[1]),
		)
	}
}

func (service *Pagination) processInFilter(filter request.FilterRequest, not bool, condition string, placeholders *[]any) string {

	currentPlaceholders := []string{}
	for _, val := range filter.Vals {
		currentPlaceholders = append(currentPlaceholders, service.setPlaceholder(placeholders, val))
	}

	// Creamos la columna
	column := *service.getColumn(filter.Attr)

	conn := getConn(filter.Conn, condition)
	if not {
		return fmt.Sprintf(" %s (%s NOT IN (%s))",
			conn,
			column,
			currentPlaceholders,
		)
	} else {
		return fmt.Sprintf(" %s (%s IN (%s))",
			conn,
			column,
			currentPlaceholders,
		)
	}
}

func (service *Pagination) processNullFilter(filter request.FilterRequest, not bool, condition string) string {
	// Creamos la columna
	column := *service.getColumn(filter.Attr)

	conn := getConn(filter.Conn, condition)
	if not {
		return fmt.Sprintf(" %s (%s IS NOT NULL)", conn, column)
	} else {
		return fmt.Sprintf(" %s (%s IS NULL)", conn, column)
	}
}

func (service *Pagination) processTermFilter(filter request.FilterRequest, condition string, placeholders *[]any) string {

	// Recorremos las columnas para filtros OR
	var ors []string
	for _, attr := range filter.Attrs {
		column := *service.getColumn(attr)
		ors = append(ors, fmt.Sprintf("%s %s %s", column, filter.Opr, service.setPlaceholder(placeholders, filter.Val)))
	}

	return fmt.Sprintf(" %s (%s)", getConn(filter.Conn, condition), strings.Join(ors, " OR "))
}

// Creamos la columna
func (service *Pagination) processDateFilter(filter request.FilterRequest, condition string, placeholders *[]any) string {
	column := *service.getColumn(filter.Attr)

	conn := getConn(filter.Conn, condition)
	valPlaceholder := service.setPlaceholder(placeholders, filter.Val)

	switch getDatabaseType(service.db) {
	case "mysql":
		return fmt.Sprintf(" %s (%s BETWEEN %s AND DATE_ADD(%s, INTERVAL 1 DAY))",
			conn,
			column,
			valPlaceholder,
			valPlaceholder,
		)
	case "postgres":
		fallthrough
	default:
		return fmt.Sprintf(" %s (%s BETWEEN (%s)::TIMESTAMP AND (%s)::TIMESTAMP + interval '1 days')",
			conn,
			column,
			valPlaceholder,
			valPlaceholder,
		)
	}
}

func (service *Pagination) processDateBetweenFilter(filter request.FilterRequest, condition string, placeholders *[]any) string {

	column := *service.getColumn(filter.Attr)

	switch getDatabaseType(service.db) {
	case "mysql":
		return fmt.Sprintf(" %s (%s BETWEEN %s AND DATE_ADD(%s, INTERVAL 1 DAY))",
			getConn(filter.Conn, condition),
			column,
			service.setPlaceholder(placeholders, filter.Vals[0]),
			service.setPlaceholder(placeholders, filter.Vals[1]),
		)
	case "postgres":
		fallthrough
	default:
		return fmt.Sprintf(" %s (%s BETWEEN (%s)::TIMESTAMP AND (%s)::TIMESTAMP + interval '1 days')",
			getConn(filter.Conn, condition),
			column,
			service.setPlaceholder(placeholders, filter.Vals[0]),
			service.setPlaceholder(placeholders, filter.Vals[1]),
		)
	}
}

func (service *Pagination) setPlaceholder(placeholders *[]any, value any) string {

	*placeholders = append(*placeholders, value)

	switch getDatabaseType(service.db) {
	case "mysql":
		return "?"
	case "postgres":
		fallthrough
	default:
		return "$" + strconv.Itoa(len(*placeholders))
	}
}

// func (service *Pagination) setPlaceholder(placeholders *[]any, value string) string {
// 	if *placeholders == nil {
// 		*placeholders = make(map[string]any)
// 	}

// 	key := "p" + strconv.Itoa(len(*placeholders))

// 	(*placeholders)[key] = value

// 	return "@" + key
// }

func (service *Pagination) getItems(sql string, cols []string, placeholders []any) ([]map[string]any, error) {

	rows, err := service.db.Raw(sql, placeholders...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []map[string]any{}
	for rows.Next() {
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			row[colName] = *val
		}

		items = append(items, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// Otras funciones
func validateOperator(validOperators []string, opr string) error {
	if !helpers.ArrayContains(validOperators, opr) {
		return errors.New("operator filter '" + opr + "' not allowed")
	}
	return nil
}

func getConn(conn string, condition string) string {
	if strings.TrimSpace(condition) != "" {
		return conn
	}
	return ""
}

func getLimit(findRequest request.FindRequest) string {
	if findRequest.Limit > 0 {
		return "LIMIT " + strconv.Itoa(findRequest.Limit)
	}
	return ""
}

func getInfiniteScroll(infiniteScroll request.InfiniteScrollRequest) string {
	// Asignar valores predeterminados si son cero o negativos
	if infiniteScroll.Limit <= 0 {
		infiniteScroll.Limit = 10
	}
	if infiniteScroll.Page <= 0 {
		infiniteScroll.Page = 1
	}

	// Calcular el OFFSET
	offset := (infiniteScroll.Page - 1) * infiniteScroll.Limit

	// Construir la cláusula LIMIT y OFFSET
	if infiniteScroll.Limit > 0 {
		return fmt.Sprintf("LIMIT %d OFFSET %d", infiniteScroll.Limit, offset)
	}
	return ""
}

func getPagination(pagination request.PaginationRequest) string {
	// Aplicar valores predeterminados si no están definidos
	if pagination.Limit <= 0 {
		pagination.Limit = 10
	}
	if pagination.Page <= 0 {
		pagination.Page = 1
	}

	// Calcular el offset
	offset := (pagination.Page - 1) * pagination.Limit

	// Construir la cláusula LIMIT y OFFSET
	if pagination.Limit > 0 {
		return fmt.Sprintf("LIMIT %d OFFSET %d", pagination.Limit, offset)
	}
	return ""
}

func getPaginationOffset(pagination request.PaginationOffsetRequest) string {
	// Establece valores predeterminados si no se proporcionan
	if pagination.Limit == 0 {
		pagination.Limit = 10
	}

	if pagination.Offset < 0 {
		pagination.Offset = 0
	}

	var parts []string
	if pagination.Limit > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", pagination.Limit))
	}

	if pagination.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", pagination.Offset))
	}

	return strings.Join(parts, " ")
}

func isNumber(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func getDatabaseType(db *gorm.DB) string {
	switch db.Dialector.(type) {
	case *mysql.Dialector:
		return "mysql"
	case *postgres.Dialector:
		return "postgres"
	default:
		return "unknown"
	}
}
