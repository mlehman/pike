package pike

type MetaData map[string][]byte

type Schema struct {
	Type      string   `json:"type"`
	Name      string   `json:"name,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
	Fields    []Schema `json:"fields,omitempty"`
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
