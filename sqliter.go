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

func Open(filename string) (*Sqliter, error) {
	if db, err := sqlx.Connect("sqlite3", filename); err == nil {
		return &Sqliter{db: db}, nil
	} else {
		return nil, err
	}
}

func (s *Sqliter) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.db.Close()
}

func (s *Sqliter) Exec(q string, args ...interface{}) (sql.Result, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Exec(q, args)
}

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
	_, err = s.db.Exec(q)
	return err
}

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
		return fmt.Errorf("get record not found where %s, %s", where, err.Error())
	}
	return nil
}

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
		return fmt.Errorf("select records err where %s, %s", where, err.Error())
	}
	return nil
}

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

func (s *Sqliter) Delete(sample interface{}, where string, args ...interface{}) error {
	name, _, err := decomposeStruct(sample)
	if err != nil {
		return err
	}
	return s.DeleteFrom(name, where, args)
}

func (s *Sqliter) DeleteFrom(table, where string, args ...interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	q := fmt.Sprintf("DELETE FROM %s WHERE %s", table, where)
	_, err := s.db.Exec(q, args...)
	return err
}
