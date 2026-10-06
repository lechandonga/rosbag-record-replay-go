package bag

import (
	"errors"
	"fmt"
	"math"
)

// IncompatibleError 表示数据所用的旧定义与回放时要求的新定义不兼容。
// 它与“数据损坏”（ErrCorrupt）是两类不同的错误：不兼容是定义层面可解释的
// 拒绝，损坏是字节层面 CRC/长度校验失败。
type IncompatibleError struct {
	Topic   string
	Reasons []string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("bag: topic %q message definition incompatible: %v", e.Topic, e.Reasons)
}

var (
	// ErrCorrupt 表示录制文件中存在半截消息或 CRC 校验失败。
	ErrCorrupt = errors.New("bag: corrupt data (truncated frame or crc mismatch)")
	// ErrNoData 表示请求的话题/时间区间内本来就没有完整消息。
	ErrNoData = errors.New("bag: no data in the requested range")
)

// DecodeResult 是按目标定义解码一条旧消息的结果。
type DecodeResult struct {
	Values    map[string]any
	Defaulted []string // 因目标定义新增而被填入固定默认值的字段路径
}

// decodeFields 按 target 定义解码 data。
// 规则：
//   - 数据中有、目标定义中也有且类型一致：使用原值；
//   - 数据中有、目标定义中没有（字段被删除/改名）：拒绝；
//   - 类型不一致（含数组元素类型、嵌套字段类型）：拒绝；
//   - 数据中没有、目标定义新增：写入该类型的固定零值并记录在 Defaulted 中。
func decodeFields(data []byte, target []Field, topic string) (DecodeResult, error) {
	r := &decReader{b: data}
	out := map[string]any{}
	var defaulted []string
	var reasons []string
	if err := readMessageInto(r, target, "", out, &defaulted, &reasons); err != nil {
		return DecodeResult{}, err
	}
	if r.pos != len(data) {
		return DecodeResult{}, ErrCorrupt
	}
	if len(reasons) > 0 {
		return DecodeResult{}, &IncompatibleError{Topic: topic, Reasons: reasons}
	}
	return DecodeResult{Values: out, Defaulted: defaulted}, nil
}

func readMessageInto(r *decReader, target []Field, prefix string, out map[string]any, defaulted *[]string, reasons *[]string) error {
	n, err := r.u32()
	if err != nil {
		return err
	}
	targets := map[string]Field{}
	for _, f := range target {
		targets[f.Name] = f
	}
	consumed := map[string]bool{}
	for i := uint32(0); i < n; i++ {
		tag, err := r.u8()
		if err != nil {
			return err
		}
		nameLen, err := r.u16()
		if err != nil {
			return err
		}
		nameBytes, err := r.take(int(nameLen))
		if err != nil {
			return err
		}
		name := string(nameBytes)
		wireType, ok := fieldByTag(tag)
		if !ok {
			return ErrCorrupt
		}
		path := prefix + name
		tf, exists := targets[name]
		if !exists {
			// 字段在目标定义中已不存在（删除或改名）：不能猜，必须拒绝。
			if err := skipValue(r, Field{Name: name, Type: wireType}); err != nil {
				return err
			}
			*reasons = append(*reasons, fmt.Sprintf("field %q exists in recorded data but is removed/renamed in current definition", path))
			continue
		}
		consumed[name] = true
		if tf.Type != wireType {
			if err := skipValue(r, Field{Name: name, Type: wireType}); err != nil {
				return err
			}
			*reasons = append(*reasons, fmt.Sprintf("field %q type mismatch: recorded as %s, current definition requires %s", path, wireType, tf.Type))
			continue
		}
		v, subReasons, err := readValue(r, tf, path, defaulted)
		if err != nil {
			return err
		}
		*reasons = append(*reasons, subReasons...)
		out[name] = v
	}
	// 目标定义新增的字段：填固定默认值。
	for _, f := range target {
		if consumed[f.Name] {
			continue
		}
		path := prefix + f.Name
		out[f.Name] = zeroValue(f)
		*defaulted = append(*defaulted, path)
	}
	return nil
}

func readValue(r *decReader, f Field, path string, defaulted *[]string) (any, []string, error) {
	var reasons []string
	switch f.Type {
	case TypeBool:
		v, err := r.u8()
		if err != nil {
			return nil, nil, err
		}
		return v != 0, nil, nil
	case TypeInt64:
		v, err := r.u64()
		if err != nil {
			return nil, nil, err
		}
		return int64(v), nil, nil
	case TypeFloat:
		v, err := r.u64()
		if err != nil {
			return nil, nil, err
		}
		return math.Float64frombits(v), nil, nil
	case TypeString:
		n, err := r.u32()
		if err != nil {
			return nil, nil, err
		}
		b, err := r.take(int(n))
		if err != nil {
			return nil, nil, err
		}
		return string(b), nil, nil
	case TypeBytes:
		n, err := r.u32()
		if err != nil {
			return nil, nil, err
		}
		b, err := r.take(int(n))
		if err != nil {
			return nil, nil, err
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		return cp, nil, nil
	case TypeArray:
		n, err := r.u32()
		if err != nil {
			return nil, nil, err
		}
		arr := make([]any, n)
		for i := uint32(0); i < n; i++ {
			tag, err := r.u8()
			if err != nil {
				return nil, nil, err
			}
			nameLen, err := r.u16()
			if err != nil {
				return nil, nil, err
			}
			if _, err := r.take(int(nameLen)); err != nil {
				return nil, nil, err
			}
			wireType, ok := fieldByTag(tag)
			if !ok {
				return nil, nil, ErrCorrupt
			}
			epath := fmt.Sprintf("%s[%d]", path, i)
			if wireType != f.ArrayElem.Type {
				if err := skipValue(r, Field{Type: wireType}); err != nil {
					return nil, nil, err
				}
				reasons = append(reasons, fmt.Sprintf("array field %q element type mismatch: recorded as %s, current definition requires %s", path, wireType, f.ArrayElem.Type))
				continue
			}
			v, sub, err := readValue(r, *f.ArrayElem, epath, defaulted)
			if err != nil {
				return nil, nil, err
			}
			reasons = append(reasons, sub...)
			arr[i] = v
		}
		return arr, reasons, nil
	case TypeMessage:
		sub := map[string]any{}
		if err := readMessageInto(r, f.Fields, path+".", sub, defaulted, &reasons); err != nil {
			return nil, nil, err
		}
		return sub, reasons, nil
	default:
		return nil, nil, fmt.Errorf("bag: field %q has unsupported type %q", f.Name, f.Type)
	}
}

// skipValue 跳过一个值（用于目标定义中已删除或类型不匹配的字段）。
func skipValue(r *decReader, f Field) error {
	switch f.Type {
	case TypeBool:
		_, err := r.u8()
		return err
	case TypeInt64, TypeFloat:
		_, err := r.u64()
		return err
	case TypeString, TypeBytes:
		n, err := r.u32()
		if err != nil {
			return err
		}
		_, err = r.take(int(n))
		return err
	case TypeArray:
		n, err := r.u32()
		if err != nil {
			return err
		}
		for i := uint32(0); i < n; i++ {
			tag, err := r.u8()
			if err != nil {
				return err
			}
			nameLen, err := r.u16()
			if err != nil {
				return err
			}
			if _, err := r.take(int(nameLen)); err != nil {
				return err
			}
			wt, ok := fieldByTag(tag)
			if !ok {
				return ErrCorrupt
			}
			if err := skipValue(r, Field{Type: wt}); err != nil {
				return err
			}
		}
		return nil
	case TypeMessage:
		n, err := r.u32()
		if err != nil {
			return err
		}
		for i := uint32(0); i < n; i++ {
			tag, err := r.u8()
			if err != nil {
				return err
			}
			nameLen, err := r.u16()
			if err != nil {
				return err
			}
			if _, err := r.take(int(nameLen)); err != nil {
				return err
			}
			wt, ok := fieldByTag(tag)
			if !ok {
				return ErrCorrupt
			}
			if err := skipValue(r, Field{Type: wt}); err != nil {
				return err
			}
		}
		return nil
	default:
		return ErrCorrupt
	}
}

// zeroValue 返回每个类型固定且可解释的默认值：
// bool=false, int64=0, float64=0, string="", bytes=空切片,
// array=空切片, message=所有嵌套字段递归取默认值。
func zeroValue(f Field) any {
	switch f.Type {
	case TypeBool:
		return false
	case TypeInt64:
		return int64(0)
	case TypeFloat:
		return float64(0)
	case TypeString:
		return ""
	case TypeBytes:
		return []byte{}
	case TypeArray:
		return []any{}
	case TypeMessage:
		m := map[string]any{}
		for _, nf := range f.Fields {
			m[nf.Name] = zeroValue(nf)
		}
		return m
	default:
		return nil
	}
}
