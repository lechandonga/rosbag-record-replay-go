package bag

import (
	"encoding/binary"
	"fmt"
	"math"
)

// 字段线格式（TLV，按字段名而不是位置识别）：
//   u8   type tag
//   u16  name 长度（大端）
//   []byte name
//   value，长度由外层字段数 / 容器长度或定长类型决定
//
// 值编码：
//   bool    1 字节
//   int64   8 字节大端
//   float64 8 字节 IEEE754
//   string  u32 长度 + UTF-8
//   bytes   u32 长度 + 原始字节
//   array   u32 元素数 + 逐元素（元素名留空）
//   message u32 字段数 + 逐字段

const (
	tagBool    = 1
	tagInt64   = 2
	tagFloat   = 3
	tagString  = 4
	tagBytes   = 5
	tagArray   = 6
	tagMessage = 7
)

func typeTag(t FieldType) (byte, error) {
	switch t {
	case TypeBool:
		return tagBool, nil
	case TypeInt64:
		return tagInt64, nil
	case TypeFloat:
		return tagFloat, nil
	case TypeString:
		return tagString, nil
	case TypeBytes:
		return tagBytes, nil
	case TypeArray:
		return tagArray, nil
	case TypeMessage:
		return tagMessage, nil
	default:
		return 0, fmt.Errorf("bag: unknown field type %q", t)
	}
}

func fieldByTag(tag byte) (FieldType, bool) {
	switch tag {
	case tagBool:
		return TypeBool, true
	case tagInt64:
		return TypeInt64, true
	case tagFloat:
		return TypeFloat, true
	case tagString:
		return TypeString, true
	case tagBytes:
		return TypeBytes, true
	case tagArray:
		return TypeArray, true
	case tagMessage:
		return TypeMessage, true
	default:
		return "", false
	}
}

type encWriter struct{ b []byte }

func (w *encWriter) u8(v byte)    { w.b = append(w.b, v) }
func (w *encWriter) u16(v uint16) { w.b = binary.BigEndian.AppendUint16(w.b, v) }
func (w *encWriter) u32(v uint32) { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *encWriter) u64(v uint64) { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *encWriter) bytes(v []byte) {
	w.u32(uint32(len(v)))
	w.b = append(w.b, v...)
}

type decReader struct {
	b   []byte
	pos int
}

func (r *decReader) u8() (byte, error) {
	if r.pos+1 > len(r.b) {
		return 0, ErrCorrupt
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *decReader) u16() (uint16, error) {
	if r.pos+2 > len(r.b) {
		return 0, ErrCorrupt
	}
	v := binary.BigEndian.Uint16(r.b[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *decReader) u32() (uint32, error) {
	if r.pos+4 > len(r.b) {
		return 0, ErrCorrupt
	}
	v := binary.BigEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *decReader) u64() (uint64, error) {
	if r.pos+8 > len(r.b) {
		return 0, ErrCorrupt
	}
	v := binary.BigEndian.Uint64(r.b[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *decReader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.b) {
		return nil, ErrCorrupt
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}

// encodeFields 按定义把 values 编码成 TLV 字节流。
func encodeFields(fields []Field, values map[string]any) ([]byte, error) {
	w := &encWriter{}
	w.u32(uint32(len(fields)))
	for i := range fields {
		f := fields[i]
		v, ok := values[f.Name]
		if !ok {
			return nil, fmt.Errorf("bag: missing value for field %q", f.Name)
		}
		if err := writeField(w, f, f.Name, v); err != nil {
			return nil, err
		}
	}
	return w.b, nil
}

func writeField(w *encWriter, f Field, name string, v any) error {
	tag, err := typeTag(f.Type)
	if err != nil {
		return err
	}
	w.u8(tag)
	nb := []byte(name)
	if len(nb) > math.MaxUint16 {
		return fmt.Errorf("bag: field name %q too long", name)
	}
	w.u16(uint16(len(nb)))
	w.b = append(w.b, nb...)
	return writeValue(w, f, v)
}

func writeValue(w *encWriter, f Field, v any) error {
	switch f.Type {
	case TypeBool:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("bag: field %q expects bool, got %T", f.Name, v)
		}
		if x {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case TypeInt64:
		switch x := v.(type) {
		case int64:
			w.u64(uint64(x))
		case int:
			w.u64(uint64(x))
		default:
			return fmt.Errorf("bag: field %q expects int64, got %T", f.Name, v)
		}
	case TypeFloat:
		x, ok := v.(float64)
		if !ok {
			return fmt.Errorf("bag: field %q expects float64, got %T", f.Name, v)
		}
		w.u64(math.Float64bits(x))
	case TypeString:
		x, ok := v.(string)
		if !ok {
			return fmt.Errorf("bag: field %q expects string, got %T", f.Name, v)
		}
		w.bytes([]byte(x))
	case TypeBytes:
		x, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("bag: field %q expects []byte, got %T", f.Name, v)
		}
		w.bytes(x)
	case TypeArray:
		x, ok := v.([]any)
		if !ok {
			return fmt.Errorf("bag: field %q expects []any, got %T", f.Name, v)
		}
		w.u32(uint32(len(x)))
		for _, item := range x {
			// 数组元素同样自描述类型标签（名为空），元素类型演进可被识别而非错位解析。
			if err := writeField(w, *f.ArrayElem, "", item); err != nil {
				return err
			}
		}
	case TypeMessage:
		x, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("bag: field %q expects map[string]any, got %T", f.Name, v)
		}
		b, err := encodeFields(f.Fields, x)
		if err != nil {
			return err
		}
		w.b = append(w.b, b...)
	}
	return nil
}
