package pike

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

type testCase struct {
	in  []byte
	out interface{}
	err error
}

var avroPrimitiveCases = []testCase{
	{
		in:  []byte{0x0},
		out: Boolean(false)},
	{
		in:  []byte{0x1},
		out: Boolean(true)},
	{
		in:  []byte{0x2},
		out: Boolean(false),
		err: ErrorInvalidBoolean},
	{
		in:  []byte{0x0},
		out: Int(0)},
	{
		in:  []byte{0x1},
		out: Int(-1)},
	{
		in:  []byte{0x2},
		out: Int(1)},
	{
		in:  []byte{0x4},
		out: Int(2)},
	{
		in:  []byte{0x0},
		out: Long(0)},
	{
		in:  []byte{0x1},
		out: Long(-1)},
	{
		in:  []byte{0x2},
		out: Long(1)},
	{
		in:  []byte{0x0, 0x0, 0x80, 0x7f},
		out: Float(math.Inf(1))},
	{
		in:  []byte{0x0, 0x0, 0x80, 0xff},
		out: Float(math.Inf(-1))},
	{
		in:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x7f},
		out: Double(math.Inf(1))},
	{
		in:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0xff},
		out: Double(math.Inf(-1))},
	{
		in:  []byte{0xa, 'H', 'e', 'l', 'l', 'o'},
		out: String("Hello")},
	{
		in:  []byte{0xa, 0x1, 0x2, 0x3, 0x4, 0x5},
		out: Bytes([]byte{0x1, 0x2, 0x3, 0x4, 0x5})},
}

var avroArrayCases = []testCase{
	{
		in:  []byte{0x4, 0x1, 0x0, 0x0},
		out: BooleanArray([]Boolean{true, false})},
	{
		in:  []byte{0x4, 0x6, 0x36, 0x0},
		out: IntArray([]Int{3, 27})},
	{
		in:  []byte{0x4, 0x6, 0x36, 0x0},
		out: LongArray([]Long{3, 27})},
	{
		in:  []byte{0x4, 0x0, 0x0, 0x80, 0x7f, 0x0, 0x0, 0x80, 0xff, 0x0},
		out: FloatArray([]Float{Float(math.Inf(1)), Float(math.Inf(-1))})},
	{
		in: []byte{0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x7f,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0xff, 0x0},
		out: DoubleArray([]Double{Double(math.Inf(1)), Double(math.Inf(-1))})},
	{
		in: []byte{0x4, 0xa, 'H', 'e', 'l', 'l', 'o',
			0xa, 'W', 'o', 'r', 'l', 'd', 0x0},
		out: StringArray([]String{"Hello", "World"})},
}

func checkEqual(t *testing.T, i int, c testCase, a interface{}, err error) {
	if c.err != nil && c.err != err {
		t.Errorf("%d. read(%q) => %q, want %q", i, c.in, err, c.err)
	} else if !reflect.DeepEqual(a, c.out) {
		if err != nil {
			t.Errorf(err.Error())
		}
		t.Errorf("%d. read(%q) => %q, want %q", i, c.in, a, c.out)
	}
}

func TestReadPrimitives(t *testing.T) {
	for i, c := range avroPrimitiveCases {
		reader := bytes.NewReader(c.in)
		var tr TypeReader
		switch c.out.(type) {
		case Boolean:
			tr = new(Boolean)
		case Int:
			tr = new(Int)
		case Long:
			tr = new(Long)
		case Float:
			tr = new(Float)
		case Double:
			tr = new(Double)
		case Bytes:
			tr = new(Bytes)
		case String:
			tr = new(String)
		}
		err := tr.read(reader)
		checkEqual(t, i, c, tr.value(), err)
	}
}

func TestReadArrays(t *testing.T) {
	for i, c := range avroArrayCases {
		reader := bytes.NewReader(c.in)
		var tr TypeReader
		switch c.out.(type) {
		case BooleanArray:
			tr = new(BooleanArray)
		case IntArray:
			tr = new(IntArray)
		case LongArray:
			tr = new(LongArray)
		case FloatArray:
			tr = new(FloatArray)
		case DoubleArray:
			tr = new(DoubleArray)
		case BytesArray:
			tr = new(BytesArray)
		case StringArray:
			tr = new(StringArray)
		}
		err := tr.read(reader)
		checkEqual(t, i, c, tr.value(), err)
	}
}
