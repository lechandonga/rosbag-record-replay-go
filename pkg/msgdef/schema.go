package msgdef

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// FieldType 是消息字段支持的标量类型。
type FieldType string

const (
	String  FieldType = "string"
	Int32   FieldType = "int32"
	Int64   FieldType = "int64"
	Float64 FieldType = "float64"
	Bool    FieldType = "bool"
)

// Field 描述消息中的一个字段。字段按名字编解码，绝不按位置解析。
type Field struct {
	Name string    `json:"name"`
	Type FieldType `json:"type"`
	// Default 是“新增字段读取旧数据”时使用的默认值（JSON 表达）。
	// 为空表示使用该类型可解释的固定零值。
	Default string `json:"default,omitempty"`
}

// Schema 是一个具名消息类型在某一版本下的定义。
type Schema struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// Fingerprint 基于字段名与类型计算；默认值不影响线上数据布局，因此不进入指纹。
func (s Schema) Fingerprint() string {
	h := sha1.New()
	fmt.Fprintf(h, "type=%s\n", s.Name)
	for _, f := range s.Fields {
		fmt.Fprintf(h, "field=%s:%s\n", f.Name, f.Type)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ZeroValue 返回类型的固定、可解释零值（JSON 表达）。
func ZeroValue(t FieldType) string {
	switch t {
	case String:
		return `""`
	case Int32, Int64:
		return "0"
	case Float64:
		return "0.0"
	case Bool:
		return "false"
	}
	return "<null>"
}

// Validate 检查 schema 自身是否合法。
func (s Schema) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("schema name is empty")
	}
	seen := map[string]bool{}
	for _, f := range s.Fields {
		if f.Name == "" {
			return fmt.Errorf("schema %s has a field with empty name", s.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("schema %s has duplicate field %q", s.Name, f.Name)
		}
		seen[f.Name] = true
		if !validType(f.Type) {
			return fmt.Errorf("schema %s field %q has unsupported type %q", s.Name, f.Name, f.Type)
		}
	}
	return nil
}

func validType(t FieldType) bool {
	switch t {
	case String, Int32, Int64, Float64, Bool:
		return true
	}
	return false
}
