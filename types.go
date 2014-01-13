package pike

import "fmt"

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
)

type MetaData map[string][]byte

type Schema struct {
	SchemaType interface{} `json:"type"` //TODO: Implement json.Unmarshaller
	Name       string      `json:"name,omitempty"`
	Namespace  string      `json:"namespace,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Fields     []Schema    `json:"fields,omitempty"`
}

func (s *Schema) Type() string {
	switch s.SchemaType.(type) {
	case string:
		return s.SchemaType.(string)
	case []interface{}:
		return UnionType
	default:
		fmt.Printf("%T\n", s.SchemaType)
		return "unknown"
	}
}

type Boolean bool
type Int int32
type Long int64
type Float float32
type Double float64
type Bytes []byte
type String string

type Union interface{}

type BooleanArray []Boolean
type IntArray []Int
type LongArray []Long
type FloatArray []Float
type DoubleArray []Double
type BytesArray []Bytes
type StringArray []String
type UnionArray []Union

type Record map[string]interface{}
