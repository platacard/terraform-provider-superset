package client

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// uploadHint explains the 404 of the upload endpoint on older versions.
const uploadHint = "The file upload endpoint needs Superset 4.1 or later; the connection must allow file uploads and " +
	"list the schema in schemas_allowed_for_file_upload of its extra."

// Upload is a file loaded into a table of a database connection: the file, where it goes and
// how it is read. Pointers and empty values are left out of the request, so Superset applies
// its own defaults.
type Upload struct {
	DatabaseID       int64
	TableName        string
	Schema           string
	Type             string // csv, excel or columnar
	FileName         string
	Content          []byte
	AlreadyExists    string // fail, replace or append
	Delimiter        string
	HeaderRow        *int64
	SkipRows         *int64
	RowsToRead       *int64
	ColumnDates      []string
	ColumnDataTypes  string
	NullValues       []string
	DecimalCharacter string
	DayFirst         *bool
	SkipBlankLines   *bool
	SkipInitialSpace *bool
	ColumnsRead      []string
	IndexColumn      string
	DataframeIndex   *bool
	IndexLabel       string
	SheetName        string
}

// fields renders the form fields of the upload request.
func (u Upload) fields() map[string]string {
	fields := map[string]string{"table_name": u.TableName, "type": u.Type, "already_exists": u.AlreadyExists}
	set := func(key, value string) {
		if value != "" {
			fields[key] = value
		}
	}
	setInt := func(key string, value *int64) {
		if value != nil {
			fields[key] = strconv.FormatInt(*value, 10)
		}
	}
	setBool := func(key string, value *bool) {
		if value != nil {
			fields[key] = strconv.FormatBool(*value)
		}
	}
	set("schema", u.Schema)
	set("delimiter", u.Delimiter)
	setInt("header_row", u.HeaderRow)
	setInt("skip_rows", u.SkipRows)
	setInt("rows_to_read", u.RowsToRead)
	set("column_dates", strings.Join(u.ColumnDates, ","))
	set("column_data_types", u.ColumnDataTypes)
	set("null_values", strings.Join(u.NullValues, ","))
	set("decimal_character", u.DecimalCharacter)
	setBool("day_first", u.DayFirst)
	setBool("skip_blank_lines", u.SkipBlankLines)
	setBool("skip_initial_space", u.SkipInitialSpace)
	set("columns_read", strings.Join(u.ColumnsRead, ","))
	set("index_column", u.IndexColumn)
	setBool("dataframe_index", u.DataframeIndex)
	set("index_label", u.IndexLabel)
	set("sheet_name", u.SheetName)
	return fields
}

// UploadFile loads the file into the table, creating or replacing the table as the upload
// says, and makes Superset create a dataset for it. The 404 of the endpoint on older
// versions gets a hint.
func (c *Client) UploadFile(upload Upload) error {
	endpoint := fmt.Sprintf("%s%d/upload/", databasesPath, upload.DatabaseID)
	err := c.uploadFile(endpoint, "file", upload.FileName, upload.fields(), upload.Content, fmt.Sprintf("upload %s into table %s", upload.FileName, upload.TableName))
	if err != nil && (errors.Is(err, ErrNotFound) || strings.Contains(err.Error(), "status code: 404")) {
		return fmt.Errorf("%w\n%s", err, uploadHint)
	}
	return err
}
