package database

import (
	"github.com/IfanTsai/metis/datastruct"
)

type Databse struct {
	Dict   *datastruct.Dict
	Expire *datastruct.Dict
}

func NewDatabase() *Databse {
	return &Databse{
		Dict:   datastruct.NewDict(&DictType{}),
		Expire: datastruct.NewDict(&DictType{}),
	}
}
