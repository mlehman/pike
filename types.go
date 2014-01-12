package pike

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
