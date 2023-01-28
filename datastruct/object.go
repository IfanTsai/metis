package datastruct

type ObjectType int

const (
	ObjectTypeString ObjectType = iota
	ObjectTypeList
	ObjectTypeSet
	ObjectTypeZSet
	ObjectTypeHash
)

type Object struct {
	Type  ObjectType
	Value any
}

func NewObject(typ ObjectType, value any) *Object {
	return &Object{
		Type:  typ,
		Value: value,
	}
}
