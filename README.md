## Summary

SQLiter provides a simplified interface for storing flat structs to disk. It is meant to be easy to use and quick to get started but not to be particularly powerful.

The utility is in adding structored data persistence to a golang project in just a few lines of code.

## Usage

### Init
```
type MyStruct struct {
    ID    int64  `db:"id" attr:"PRIMARY KEY"`
    Value string `db:"value"`
}

db, _ := sqliter.Open(dbName)
db.CreateTable(MyStruct{})
```

### Insert
```
db.Insert(MyStruct{Value:"foo"})
```

### Read
```
myStruct := MyStruct{}
db.ReadOne(&myStruct, "id = ?", 1)
```

### ReadMany
```
myList := []*Mystruct{}
db.ReadMany(&mylist, "value > ?")
db.ReadMany(&mylist, "")

### Update
```
myStruct.Value = "bar"
db.Update(myStruct, "id = ?", 1)
```

### Delete
```
db.Delete(MyStruct{}, "id = ?", 1)
```
