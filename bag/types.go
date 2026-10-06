package bag

import (
	"errors"
	"fmt"
	"time"
)

// FieldType 是消息定义中支持的字段类型。编码按"字段名 + 类型标签"进行，
// 不依赖字段在结构体中的位置，因此新增字段天然兼容，删除/改名/类型变更
// 可以被明确识别，而不是静默按位置错位解析。
type FieldType string

const (
	TypeBool    FieldType = "bool"
	TypeInt64   FieldType = "int64"
	TypeFloat   FieldType = "float64"
	TypeString  FieldType = "string"
	TypeBytes   FieldType = "bytes"
	TypeArray   FieldType = "array"   // 配合 ArrayElem 使用
	TypeMessage FieldType = "message" // 配合 Fields 使用
)

// Field 描述消息定义中的一个字段。
type Field struct {
	Name      string    `json:"name"`
	Type      FieldType `json:"type"`
	ArrayElem *Field    `json:"array_elem,omitempty"` // 仅 Type == TypeArray
	Fields    []Field   `json:"fields,omitempty"`     // 仅 Type == TypeMessage
}

// MessageDef 是一个具名消息类型的定义（schema）。
type MessageDef struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// Validate 检查消息定义本身是否合法（类型标签、字段名、嵌套等）。
func (d MessageDef) Validate() error {
	if d.Name == "" {
		return errors.New("bag: message definition name is empty")
	}
	seen := map[string]bool{}
	for i := range d.Fields {
		if err := d.Fields[i].validate(seen); err != nil {
			return fmt.Errorf("bag: invalid definition %q: %w", d.Name, err)
		}
	}
	return nil
}

func (f Field) validate(seen map[string]bool) error {
	if f.Name == "" {
		return errors.New("field name is empty")
	}
	if seen[f.Name] {
		return fmt.Errorf("duplicate field name %q", f.Name)
	}
	seen[f.Name] = true
	switch f.Type {
	case TypeBool, TypeInt64, TypeFloat, TypeString, TypeBytes:
		return nil
	case TypeArray:
		if f.ArrayElem == nil {
			return fmt.Errorf("array field %q missing element type", f.Name)
		}
		return f.ArrayElem.validate(map[string]bool{})
	case TypeMessage:
		if len(f.Fields) == 0 {
			return fmt.Errorf("message field %q has no nested fields", f.Name)
		}
		nested := map[string]bool{}
		for i := range f.Fields {
			if err := f.Fields[i].validate(nested); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("field %q has unknown type %q", f.Name, f.Type)
	}
}

// Message 是一条业务消息。Values 的 key 必须与定义中的字段名一一对应；
// 嵌套消息用 map[string]any 表示，数组用 []any 表示。
type Message struct {
	Topic     string
	Timestamp time.Time
	Values    map[string]any
}
