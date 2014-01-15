package pike

import (
	"encoding/json"
)

const (
	NullType    = "null"
	BooleanType = "boolean"
	IntType     = "int"
	LongType    = "long"
	FloatType   = "float"
	DoubleType  = "double"
	BytesType   = "bytes"
	StringType  = "string"
	ArrayType   = "array"
	UnionType   = "union"
	RecordType  = "record"
	MapType     = "map"
)

type MetaData map[string][]byte

type Schema struct {
	SchemaType SchemaType `json:"type"` //TODO: Implement json.Unmarshaller
	Name       string     `json:"name,omitempty"`
	Namespace  string     `json:"namespace,omitempty"`
	Doc        string     `json:"doc,omitempty"`
	Fields     []Schema   `json:"fields,omitempty"`
	Items      *Schema    `json:"items,omitempty"`
	Values     *Schema    `json:"values,omitempty"`
}

type SchemaType struct {
	Type              string
	ComplexTypeSchema *Schema
	UnionSchemaTypes  []SchemaType
}

func (st *SchemaType) Primitive() bool {
	return st.Type != RecordType &&
		st.ComplexTypeSchema == nil && len(st.UnionSchemaTypes) == 0
}

func (st *SchemaType) UnmarshalJSON(b []byte) (err error) {
	switch b[0] {
	case '[':
		st.Type = UnionType
		err = json.Unmarshal(b, &st.UnionSchemaTypes)
	case '{':
		var s Schema
		if err = json.Unmarshal(b, &s); err == nil {
			st.ComplexTypeSchema = &s
			st.Type = s.SchemaType.Type
		}
	default:
		err = json.Unmarshal(b, &st.Type)
	}
	return
}

func (st *SchemaType) MarshalJSON() ([]byte, error) {
	switch {
	case st.Type == UnionType:
		return json.Marshal(&st.UnionSchemaTypes)
	case st.ComplexTypeSchema != nil:
		return json.Marshal(st.ComplexTypeSchema)
	default:
		return json.Marshal(&st.Type)
	}
}

type Boolean bool
type Int int32
type Long int64
type Float float32
type Double float64
type Bytes []byte
type String string

type BooleanArray []Boolean
type IntArray []Int
type LongArray []Long
type FloatArray []Float
type DoubleArray []Double
type BytesArray []Bytes
type StringArray []String
type UnionArray []Union

type Union interface{}
type Map map[string]interface{}
type Record map[string]interface{}
type RecordArray []Record
