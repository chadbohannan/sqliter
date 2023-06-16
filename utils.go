package sqliter

import (
	"fmt"
	"reflect"
	"strings"
)

type StructField struct {
	Key   string
	Value interface{}
	Type  reflect.Type
	Attr  string
}

func resolveDbFieldName(t reflect.StructTag) string {
	if len(t.Get("db")) > 0 {
		return t.Get("db")
	}
	return "-"
}

func decomposeStruct(data interface{}) (string, []StructField, error) {
	t := reflect.TypeOf(data)
	r := reflect.ValueOf(data)
	switch t.Kind() {
	case reflect.Slice:
		return decomposeStruct(reflect.New(t.Elem().Elem()).Interface())
	case reflect.Ptr:
		return decomposeStruct(r.Elem().Interface())
	case reflect.Struct:
		arry := []StructField{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Tag != "" {
				v := reflect.Indirect(r).FieldByName(f.Name)
				if name := resolveDbFieldName(f.Tag); name != "-" {
					arry = append(arry, StructField{name, v.Interface(), f.Type, f.Tag.Get("attr")})
				}
			}
		}
		return strings.ToLower(t.Name()), arry, nil
	default:
		return "", nil, fmt.Errorf("type %v not supported", t.Kind())
	}
}

func mapType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return mapType(t.Elem())
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.String:
		return "TEXT"
	default:
		return "-"
	}
}

func fieldsListToCreateTable(table string, fields []StructField) (string, error) {
	cols := make([]string, len(fields))
	for i, field := range fields {
		if len(field.Attr) > 0 {
			cols[i] = fmt.Sprintf("%s %s %s", field.Key, mapType(field.Type), field.Attr)
		} else {
			cols[i] = fmt.Sprintf("%s %s", field.Key, mapType(field.Type))
		}
	}
	q := "CREATE TABLE IF NOT EXISTS %s (%s);"
	return fmt.Sprintf(q, table, strings.Join(cols, ", ")), nil
}

func fieldsListToInsertRecord(table string, fields []StructField) (string, []interface{}, error) {
	cols := []string{}
	hodl := []string{}
	vals := []interface{}{}
	for _, field := range fields {
		if strings.Contains(field.Attr, "PRIMARY KEY") {
			continue
		}
		cols = append(cols, field.Key)
		hodl = append(hodl, "?")
		vals = append(vals, field.Value)
	}
	q := "INSERT INTO %s (%s) VALUES (%s);"
	return fmt.Sprintf(q, table, strings.Join(cols, ", "), strings.Join(hodl, ", ")), vals, nil
}

func fieldsListToUpdateRecord(table string, fields []StructField) (string, []interface{}, error) {
	var cols []string
	var vals []interface{}
	var whereKey string
	var whereValue interface{}
	for _, field := range fields {
		isPrimaryKey := field.Type.Name() == "id" || strings.Contains(field.Attr, "PRIMARY KEY") || strings.Contains(field.Attr, "UNIQUE")
		if isPrimaryKey {
			whereKey = field.Key
			whereValue = field.Value
		} else {
			cols = append(cols, fmt.Sprintf("%s = ?", field.Key))
			vals = append(vals, field.Value)
		}
	}
	q := ""
	if _, ok := whereValue.(string); ok {
		q = "UPDATE %s SET %s WHERE %s = '%s';"
	} else{
		q = "UPDATE %s SET %s WHERE %s = %v;"
	}
	return fmt.Sprintf(q, table, strings.Join(cols, ", "), whereKey, whereValue), vals, nil
}

func fieldListToReadRecord(table string, fields []StructField, where string) (string, error) {
	q := "SELECT %s FROM %s WHERE %s;"
	cols := []string{}
	for _, field := range fields {
		cols = append(cols, field.Key)
	}
	return fmt.Sprintf(q, strings.Join(cols, ","), table, where), nil
}
