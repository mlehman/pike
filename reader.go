package pike

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
)

var (
	Magic                  = []byte{'O', 'b', 'j', 0x1}
	MetaDataCodec          = "avro.codec"
	MetaDataSchema         = "avro.schema"
	ErrorNotDataFile       = errors.New("not a data file")
	ErrorInvalidBoolean    = errors.New("invalid value for boolean")
	ErrorInvalidSyncMarker = errors.New("invalid sync marker")
)

type AvroReader struct {
	binaryReader   BinaryReader
	MetaData       MetaData
	Schema         Schema
	Codec          string
	SyncMarker     [16]byte
	blockReader    BinaryReader
	blockSize      int64
	blockRemaining int64
}

func NewReader(br BinaryReader) (ar *AvroReader, err error) {
	ar = &AvroReader{binaryReader: br}
	err = ar.initialize()
	return
}

func (metaData MetaData) read(r BinaryReader) (err error) {
	err = readMap(r, new(String), new(Bytes),
		func(key TypeReader, val TypeReader) {
			metaData[string(key.value().(String))] = val.value().(Bytes)
		})
	return err
}

func (r *AvroReader) initialize() (err error) {
	magic := make([]byte, len(Magic), len(Magic))
	if _, err = io.ReadFull(r.binaryReader, magic); err != nil {
		return ErrorNotDataFile
	}
	if !eql(magic, Magic) {
		return ErrorNotDataFile
	}

	r.MetaData = make(MetaData)

	if err = r.MetaData.read(r.binaryReader); err != nil {
		return err
	}

	if err = r.readSyncMarker(r.SyncMarker[:]); err != nil {
		return err
	}

	r.Codec = string(r.MetaData[MetaDataCodec])
	if err = json.Unmarshal(r.MetaData[MetaDataSchema], &r.Schema); err != nil {
		return err
	}

	return
}

func eql(a, b []byte) bool {
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (r *AvroReader) readSyncMarker(out []byte) (err error) {
	syncMarker := make([]byte, len(r.SyncMarker), len(r.SyncMarker))
	if _, err = io.ReadFull(r.binaryReader, syncMarker); err == nil {
		copy(out, syncMarker)
	}
	return
}

func (r *AvroReader) readBlock() (err error) {

	if r.blockSize > 0 {
		syncMarker := make([]byte, len(r.SyncMarker), len(r.SyncMarker))
		if err = r.readSyncMarker(syncMarker); err != nil {
			return err
		}
		if !eql(r.SyncMarker[:], syncMarker) {
			return ErrorInvalidSyncMarker
		}
	}

	var count Long
	if err = count.read(r.binaryReader); err != nil {
		return err
	}

	r.blockRemaining = int64(count)

	if err = count.read(r.binaryReader); err != nil {
		return err
	}
	r.blockSize = int64(count)

	r.blockReader = r.binaryReader
	return
}

func (r *AvroReader) ReadRecord() (record Record, err error) {
	if r.blockRemaining == 0 {
		if err = r.readBlock(); err != nil {
			return nil, err
		}

	}
	r.blockRemaining--
	return r.Schema.readRecord(r.blockReader)
}

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

func readArray(r BinaryReader, tr TypeReader, add func(TypeReader)) (err error) {
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
	err = readArray(r, new(Boolean), func(tr TypeReader) { *array = append(*array, tr.value().(Boolean)) })
	return err
}

func (a *BooleanArray) value() interface{} { return *a }

func (array *IntArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Int), func(tr TypeReader) { *array = append(*array, tr.value().(Int)) })
	return err
}

func (a *IntArray) value() interface{} { return *a }

func (array *LongArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Long), func(tr TypeReader) { *array = append(*array, tr.value().(Long)) })
	return err
}

func (a *LongArray) value() interface{} { return *a }

func (array *FloatArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Float), func(tr TypeReader) { *array = append(*array, tr.value().(Float)) })
	return err
}

func (a *FloatArray) value() interface{} { return *a }

func (array *DoubleArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Double), func(tr TypeReader) { *array = append(*array, tr.value().(Double)) })
	return err
}

func (a *DoubleArray) value() interface{} { return *a }

func (array *BytesArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(Bytes), func(tr TypeReader) { *array = append(*array, tr.value().(Bytes)) })
	return err
}

func (a *BytesArray) value() interface{} { return *a }

func (array *StringArray) read(r BinaryReader) (err error) {
	err = readArray(r, new(String), func(tr TypeReader) { *array = append(*array, tr.value().(String)) })
	return err
}

func (a *StringArray) value() interface{} { return *a }

func readMap(r BinaryReader, key TypeReader, val TypeReader, add func(TypeReader, TypeReader)) (err error) {
	var count Long
	for err = count.read(r); err == nil && count > 0; err = count.read(r) {
		for ; count > 0; count-- {
			if err = key.read(r); err != nil {
				return err
			}
			if err = val.read(r); err != nil {
				return err
			}
			add(key, val)
		}
	}
	return err
}

func (s *Schema) read(r BinaryReader) (val interface{}, err error) {
	var tr TypeReader
	switch s.Type {
	case IntType:
		tr = new(Int)
	case LongType:
		tr = new(Long)
	case StringType:
		tr = new(String)
	default:
		return val, errors.New("unknown type:" + s.Type)
	}
	if err = tr.read(r); err == nil {
		val = tr.value()
	}
	return val, err
}

func (s *Schema) readRecord(r BinaryReader) (record Record, err error) {
	record = make(Record)
	for _, field := range s.Fields {
		var val interface{}
		if val, err = field.read(r); err != nil {
			return record, err
		}
		record[field.Name] = val
	}
	return
}
