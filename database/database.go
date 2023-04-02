package database

import (
	"github.com/IfanTsai/metis/datastruct"
)

type Databse struct {
	ID     int
	Dict   *datastruct.Dict
	Expire *datastruct.Dict // key: string, value: int64
}

func NewDatabase(id int) *Databse {
	return &Databse{
		ID:     id,
		Dict:   datastruct.NewDict(&DictType{}),
		Expire: datastruct.NewDict(&DictType{}),
	}
}
