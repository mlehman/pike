package pike

import (
	"encoding/binary"
	"errors"
	//"fmt"
	"io"
	"math"
)

var (
	ErrorInvalidBoolean = errors.New("invalid value for boolean")
)

type BinaryReader interface {
	io.ByteReader
	io.Reader
}

type TypeReader interface {
	read(binaryReader BinaryReader) error
	value() interface{}
}

func (b *Boolean) read(r BinaryReader) (err error) {
	var v byte
	if v, err = r.ReadByte(); err == nil {
		switch v {
		case 0:
			*b = false
		case 1:
			*b = true
		default:
			err = ErrorInvalidBoolean
		}
	}
	return
}

func (b *Boolean) value() interface{} { return *b }

func (i *Int) read(r BinaryReader) (err error) {
	var v int64
	if v, err = binary.ReadVarint(r); err == nil {
		*i = Int(v)
	}
	return
}

func (i *Int) value() interface{} { return *i }

func (l *Long) read(r BinaryReader) (err error) {
	var v int64
	if v, err = binary.ReadVarint(r); err == nil {
		*l = Long(v)
	}
	return
}

func (l *Long) value() interface{} { return *l }

func (f *Float) read(reader BinaryReader) (err error) {
	buff := make([]byte, 4)
	if _, err = io.ReadFull(reader, buff); err == nil {
		i := binary.LittleEndian.Uint32(buff)
		*f = Float(math.Float32frombits(i))
	}
	return err
}

func (f *Float) value() interface{} { return *f }

func (d *Double) read(reader BinaryReader) (err error) {
	buff := make([]byte, 8)
	if _, err = io.ReadFull(reader, buff); err == nil {
		i := binary.LittleEndian.Uint64(buff)
		*d = Double(math.Float64frombits(i))
	}
	return err
}

func (d *Double) value() interface{} { return *d }

func (b *Bytes) read(reader BinaryReader) (err error) {
	var count Long
	if err = count.read(reader); err == nil {
		*b = make([]byte, count)
		if _, err = io.ReadFull(reader, *b); err != nil {
			return err
		}
	}
	return err
}

func (b *Bytes) value() interface{} { return *b }

func (s *String) read(reader BinaryReader) (err error) {
	var b Bytes
	if err := b.read(reader); err == nil {
		*s = String(b)
	}
	return err
}

func (s *String) value() interface{} { return *s }

func readArray(r BinaryReader, tr TypeReader, add func(interface{})) (err error) {
	var count Long
	for err = count.read(r); err == nil && count > 0; err = count.read(r) {
		for ; count > 0; count-- {
			if err = tr.read(r); err != nil {
				return err
			}
			add(tr)
		}
	}
	return err
}

func (array *BooleanArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Boolean), func(tr interface{}) { *array = append(*array, *tr.(*Boolean)) })
	return err
}

func (a *BooleanArray) value() interface{} { return *a }

func (array *IntArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Int), func(tr interface{}) { *array = append(*array, *tr.(*Int)) })
	return err
}

func (a *IntArray) value() interface{} { return *a }

func (array *LongArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Long), func(tr interface{}) { *array = append(*array, *tr.(*Long)) })
	return err
}

func (a *LongArray) value() interface{} { return *a }

func (array *FloatArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Float), func(tr interface{}) { *array = append(*array, *tr.(*Float)) })
	return err
}

func (a *FloatArray) value() interface{} { return *a }

func (array *DoubleArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Double), func(tr interface{}) { *array = append(*array, *tr.(*Double)) })
	return err
}

func (a *DoubleArray) value() interface{} { return *a }

func (array *BytesArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Bytes), func(tr interface{}) { *array = append(*array, *tr.(*Bytes)) })
	return err
}

func (a *BytesArray) value() interface{} { return *a }

func (array *StringArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(String), func(tr interface{}) { *array = append(*array, *tr.(*String)) })
	return err
}

func (a *StringArray) value() interface{} { return *a }
