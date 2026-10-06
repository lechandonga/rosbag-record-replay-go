package msgdef

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
)

// FieldNote 记录一次“旧数据按新定义回放”时的默认值填充。
type FieldNote struct {
	Field string
	// Source 为 "default"（schema 显式默认值）或 "zero"（类型固定零值）。
	Source string
	Value  string
}

// IncompatibleError 表示写入时定义与读取时定义不兼容，必须明确拒绝。
type IncompatibleError struct {
	TypeName string
	Reason   string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("message type %s is incompatible with recorded definition: %s", e.TypeName, e.Reason)
}

// Encode 按写入方 schema 校验并编码消息。字段按名字编码，不依赖位置。
func Encode(ws Schema, msg map[string]any) ([]byte, error) {
	if err := ws.Validate(); err != nil {
		return nil, err
	}
	known := map[string]Field{}
	for _, f := range ws.Fields {
		known[f.Name] = f
	}
	clean := map[string]any{}
	for name, val := range msg {
		f, ok := known[name]
		if !ok {
			return nil, fmt.Errorf("schema %s: message contains unknown field %q", ws.Name, name)
		}
		converted, err := coerce(f, val)
		if err != nil {
			return nil, fmt.Errorf("schema %s field %q: %w", ws.Name, name, err)
		}
		clean[name] = converted
	}
	for _, f := range ws.Fields {
		if _, ok := clean[f.Name]; !ok {
			return nil, fmt.Errorf("schema %s: message missing required field %q", ws.Name, f.Name)
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(clean); err != nil {
		return nil, fmt.Errorf("schema %s: encode failed: %w", ws.Name, err)
	}
	return buf.Bytes(), nil
}

// CheckCompatible 判定“用 reader 定义回放 writer 定义的数据”是否允许。
// 唯一允许的演进：新定义纯新增字段，旧数据用显式默认值或固定零值补齐。
func CheckCompatible(typeName string, ws, rs Schema) error {
	if ws.Name != rs.Name {
		return &IncompatibleError{TypeName: typeName,
			Reason: fmt.Sprintf("type name changed from %q to %q", ws.Name, rs.Name)}
	}
	reader := map[string]Field{}
	for _, f := range rs.Fields {
		reader[f.Name] = f
	}
	for _, wf := range ws.Fields {
		rf, ok := reader[wf.Name]
		if !ok {
			return &IncompatibleError{TypeName: typeName, Reason: fmt.Sprintf(
				"field %q existed when data was recorded but is missing in the current definition (deleted or renamed)", wf.Name)}
		}
		if wf.Type != rf.Type {
			return &IncompatibleError{TypeName: typeName, Reason: fmt.Sprintf(
				"field %q type changed from %s to %s", wf.Name, wf.Type, rf.Type)}
		}
	}
	return nil
}

// Decode 用读取方 schema 解码按写入方 schema 编码的数据，
// 返回回放消息以及新增字段的默认值填充说明。
func Decode(ws, rs Schema, data []byte) (map[string]any, []FieldNote, error) {
	if err := CheckCompatible(ws.Name, ws, rs); err != nil {
		return nil, nil, err
	}
	raw := map[string]any{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&raw); err != nil {
		return nil, nil, fmt.Errorf("schema %s: payload decode failed: %w", ws.Name, err)
	}
	out := map[string]any{}
	var notes []FieldNote
	writer := map[string]Field{}
	for _, f := range ws.Fields {
		writer[f.Name] = f
	}
	for _, rf := range rs.Fields {
		if wf, ok := writer[rf.Name]; ok {
			val, present := raw[rf.Name]
			if !present {
				return nil, nil, fmt.Errorf("schema %s: recorded payload missing field %q (data corruption suspected)", ws.Name, rf.Name)
			}
			converted, err := coerce(wf, val)
			if err != nil {
				return nil, nil, &IncompatibleError{TypeName: ws.Name, Reason: fmt.Sprintf(
					"field %q recorded value does not match declared type %s: %v", rf.Name, wf.Type, err)}
			}
			out[rf.Name] = converted
		} else {
			val, source, err := defaultFor(rf)
			if err != nil {
				return nil, nil, &IncompatibleError{TypeName: rs.Name, Reason: fmt.Sprintf(
					"new field %q has invalid default %q: %v", rf.Name, rf.Default, err)}
			}
			out[rf.Name] = val
			notes = append(notes, FieldNote{Field: rf.Name, Source: source, Value: rf.Default})
		}
	}
	return out, notes, nil
}

func defaultFor(f Field) (any, string, error) {
	if f.Default == "" {
		v, err := parseJSONish(f.Type, ZeroValue(f.Type))
		return v, "zero", err
	}
	v, err := parseJSONish(f.Type, f.Default)
	return v, "default", err
}

func coerce(f Field, val any) (any, error) {
	switch f.Type {
	case String:
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", val)
		}
		return s, nil
	case Int32:
		switch n := val.(type) {
		case int:
			return int32(n), nil
		case int32:
			return n, nil
		case int64:
			return int32(n), nil
		default:
			return nil, fmt.Errorf("expected int32, got %T", val)
		}
	case Int64:
		switch n := val.(type) {
		case int:
			return int64(n), nil
		case int32:
			return int64(n), nil
		case int64:
			return n, nil
		default:
			return nil, fmt.Errorf("expected int64, got %T", val)
		}
	case Float64:
		switch n := val.(type) {
		case float64:
			return n, nil
		case float32:
			return float64(n), nil
		default:
			return nil, fmt.Errorf("expected float64, got %T", val)
		}
	case Bool:
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", val)
		}
		return b, nil
	}
	return nil, fmt.Errorf("unsupported type %q", f.Type)
}

// parseJSONish 解析默认值。为零依赖起见这里做最小的字面量解析。
func parseJSONish(t FieldType, lit string) (any, error) {
	switch t {
	case String:
		if len(lit) >= 2 && lit[0] == '"' && lit[len(lit)-1] == '"' {
			return lit[1 : len(lit)-1], nil
		}
		return nil, fmt.Errorf("string default must be JSON-quoted")
	case Int32:
		n, err := strconv.ParseInt(lit, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(n), nil
	case Int64:
		return strconv.ParseInt(lit, 10, 64)
	case Float64:
		return strconv.ParseFloat(lit, 64)
	case Bool:
		switch lit {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("bool default must be true or false")
	}
	return nil, fmt.Errorf("unsupported type %q", t)
}
