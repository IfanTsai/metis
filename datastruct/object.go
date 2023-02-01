package datastruct

import "strconv"

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

func (o *Object) IntValue() (int64, error) {
	if o.Type != ObjectTypeString {
		return 0, nil
	}

	value, err := strconv.ParseInt(o.Value.(string), 10, 64)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (o *Object) StrValue() string {
	if o.Type != ObjectTypeString {
		return ""
	}

	value, ok := o.Value.(string)
	if !ok {
		return ""
	}

	return value
}
