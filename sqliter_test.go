package sqliter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	A int    `json:"json_a" db:"db_a" attr:"PRIMARY KEY"`
	B string `json:"json_b" db:"db_b"`
}

func TestGenerateCreateTable(t *testing.T) {
	name, fields, _ := decomposeStruct(TestStruct{})
	q, err := fieldsListToCreateTable(name, fields)
	assert.Nil(t, err)
	assert.Equal(t, "CREATE TABLE IF NOT EXISTS teststruct (db_a INTEGER PRIMARY KEY, db_b TEXT);", q)
}

func TestGenerateInsertRecord(t *testing.T) {
	name, fields, _ := decomposeStruct(TestStruct{1, "b"})
	q, vals, err := fieldsListToInsertRecord(name, fields)
	assert.Nil(t, err)
	assert.Equal(t, "INSERT INTO teststruct (db_b) VALUES (?);", q)
	assert.Equal(t, "b", vals[0].(string))
}

func TestGenerateUpdateRecord(t *testing.T) {
	name, fields, _ := decomposeStruct(TestStruct{1, "b"})
	q, vals, err := fieldsListToUpdateRecord(name, fields)
	assert.Nil(t, err)
	assert.Equal(t, "UPDATE teststruct SET db_b = ? WHERE db_a = 1;", q)
	assert.Equal(t, "b", vals[0].(string))
}

func TestGenerateSelectRecord(t *testing.T) {
	name, fields, _ := decomposeStruct(TestStruct{1, "b"})
	where := "db_a > 0"
	q, err := fieldListToReadRecord(name, fields, where)
	assert.Nil(t, err)
	assert.Equal(t, "SELECT db_a,db_b FROM teststruct WHERE db_a > 0;", q)
}

func TestCRUD(t *testing.T) {
	store, err := Open(InMemory)
	defer store.Close()
	assert.Nil(t, err)
	assert.NotNil(t, store)

	assert.Nil(t, store.CreateTable(TestStruct{}))
	id, err := store.Insert(TestStruct{B: "b"})
	assert.Nil(t, err)

	testStruct := TestStruct{}
	// assert.Nil(t, store.ReadOne(&testStruct, fmt.Sprintf("db_a = %d", id)))
	assert.Nil(t, store.ReadOne(&testStruct, "db_a = ?", id))
	assert.Equal(t, 1, testStruct.A)
	assert.Equal(t, "b", testStruct.B)

	testStruct.B = "banana"
	assert.Nil(t, store.Update(testStruct, "db_a = ?", id))

	testStruct2 := TestStruct{}
	assert.Nil(t, store.ReadOne(&testStruct2, "db_a = ?", id))
	assert.Equal(t, 1, testStruct2.A)
	assert.Equal(t, "banana", testStruct2.B)

	// insert a second record
	_, err = store.Insert(TestStruct{B: "c"})
	assert.Nil(t, err)

	// expect 2 records
	allStructs := []*TestStruct{}
	assert.Nil(t, store.ReadMany(&allStructs, "db_a > 0 ORDER BY db_a"))
	assert.Len(t, allStructs, 2)

	allStructs = []*TestStruct{}
	assert.Nil(t, store.ReadMany(&allStructs, ""))
	assert.Len(t, allStructs, 2)

	oneStruct := []*TestStruct{}
	assert.Nil(t, store.ReadMany(&oneStruct, "db_a > 1 ORDER BY db_a"))
	assert.Len(t, oneStruct, 1)
	assert.Equal(t, 2, oneStruct[0].A)
}

func TestUpsert(t *testing.T) {
	type KeyValue struct {
		Key       string `json:"key" db:"_key" attr:"UNIQUE"`
		Value     string `json:"value" db:"value"`
		UpdatedAt int64  `json:"updated_at" db:"updated_at"`
	}

	store, err := Open(InMemory)
	defer store.Close()
	assert.Nil(t, err)
	assert.NotNil(t, store)

	assert.Nil(t, store.CreateTable(KeyValue{}))
	testKV := KeyValue{Key: "foo", Value: "bar"}

	id, err := store.Upsert(&testKV, "_key = ?", "foo")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), id)
	assert.Nil(t, store.ReadOne(&testKV, "_key = ?", "foo"))
	assert.Equal(t, "foo", testKV.Key)
	assert.Equal(t, "bar", testKV.Value)

	testKV.Value = "baz"
	_, err = store.Upsert(&testKV, "_key = ?", "foo")
	assert.Nil(t, err)
	assert.Nil(t, store.ReadOne(&testKV, "_key = ?", "foo"))
	assert.Equal(t, "foo", testKV.Key)
	assert.Equal(t, "baz", testKV.Value)
}
