package sqliter

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const InMemory = ":memory:"

type Sqliter struct {
	db    *sqlx.DB
	mutex sync.RWMutex
}

// Open a file on disk or 'sqliter.InMemory'
func Open(filename string) (*Sqliter, error) {
	if db, err := sqlx.Connect("sqlite3", filename); err == nil {
		return &Sqliter{db: db}, nil
	} else {
		return nil, err
	}
}

// Close cleans up db resources
func (s *Sqliter) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.db.Close()
}

// Exec SQL on the db
func (s *Sqliter) Exec(q string, args ...any) (sql.Result, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Exec(q, args...)
}

// CreateTable takes an example object (or nill pointer to its type)
// and runs CREATE TABLE IF NOT EXISTS with columns read from typej.
func (s *Sqliter) CreateTable(sample interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	name, fields, err := decomposeStruct(sample)
	if err != nil {
		return err
	}
	q, err := fieldsListToCreateTable(name, fields)
	if err != nil {
		return err
	}
	if _, err = s.db.Exec(q); err != nil {
		return err
	}

	createIndexList, err := fieldsListToCreateIndexList(name, fields)
	if err != nil {
		return err
	}

	for _, createIndexStr := range createIndexList {
		if _, err = s.db.Exec(createIndexStr); err != nil {
			return err
		}
	}
	return nil
}

// Insert a record. You must first call CreateTable with the type.
func (s *Sqliter) Insert(obj interface{}) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	name, fields, err := decomposeStruct(obj)
	if err != nil {
		return 0, err
	}
	q, vals, err := fieldsListToInsertRecord(name, fields)
	if err != nil {
		return 0, err
	}
	r, err := s.db.Exec(q, vals...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// Read a single record. Use where clause to specify which one.
func (s *Sqliter) ReadOne(outPtr interface{}, where string, args ...interface{}) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	name, fields, err := decomposeStruct(outPtr)
	if err != nil {
		return err
	}
	q, err := fieldListToReadRecord(name, fields, where)
	if err != nil {
		return err
	}
	if err = s.db.Get(outPtr, q, args...); err != nil {
		return fmt.Errorf("get %s not found where %s, %s, %v", name, where, args, err.Error())
	}
	return nil
}

// Read several records. Use where clause to specify which ones and also
// to inject OFFSET and LIMIT clauses.
func (s *Sqliter) ReadMany(outPtr interface{}, where string, args ...interface{}) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	name, fields, err := decomposeStruct(outPtr)
	if err != nil {
		return err
	}
	q, err := fieldListToReadRecord(name, fields, where)
	if err != nil {
		return err
	}
	if err = s.db.Select(outPtr, q, args...); err != nil {
		return fmt.Errorf("select %s err where %s, %s", name, where, err.Error())
	}
	return nil
}

// Update modifies a record. The obj must have an attr:"PRIMARY KEY" set on a field.
func (s *Sqliter) Update(obj interface{}, where string, args ...interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	name, fields, err := decomposeStruct(obj)
	if err != nil {
		return err
	}
	q, vals, err := fieldsListToUpdateRecord(name, fields)
	if err != nil {
		return err
	}
	vals = append(vals, args...)
	_, err = s.db.Exec(q, vals...)
	return err
}

// Upsert attempts to modify an existing record before inserting. The obj string must
// have attr:"PRIMARY KEY" set on a field
func (s *Sqliter) Upsert(obj interface{}, where string, args ...interface{}) (int64, error) {
	name, _, err := decomposeStruct(obj)
	if err != nil {
		return 0, err
	}
	count := 0
	if err = s.db.Get(&count, "SELECT COUNT(1) FROM "+name+" WHERE "+where, args...); err != nil {
		return 0, fmt.Errorf("count %s where %s, %s", name, where, err.Error())
	}
	switch count {
	case 0:
		return s.Insert(obj)
	case 1:
		return 0, s.Update(obj, where, args...) // TODO return the updated record id
	default:
		return 0, fmt.Errorf("upsert err: %d existing records", count)
	}
}

// Delete from the obj table all records matching the where clause
func (s *Sqliter) Delete(sample interface{}, where string, args ...interface{}) error {
	name, _, err := decomposeStruct(sample)
	if err != nil {
		return fmt.Errorf("Delete decompose struct err:%w", err)
	}
	return s.DeleteFrom(name, where, args...)
}

// Delete from the specified table all records matching the where clause
func (s *Sqliter) DeleteFrom(table, where string, args ...interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	q := fmt.Sprintf("DELETE FROM %s WHERE %s", table, where)
	if _, err := s.db.Exec(q, args...); err != nil {
		return fmt.Errorf("DeleteFrom('%s'):%w", q, err)
	}
	return nil
}
