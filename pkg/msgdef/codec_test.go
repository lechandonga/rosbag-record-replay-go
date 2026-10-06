package msgdef

import (
	"errors"
	"testing"
)

func logCase(t *testing.T, business, format string, args ...any) {
	t.Helper()
	t.Logf("[业务:%s] "+format, append([]any{business}, args...)...)
}

func v1() Schema {
	return Schema{Name: "Pose", Fields: []Field{
		{Name: "frame", Type: String},
		{Name: "x", Type: Float64},
	}}
}

func TestSchemaEvolveAddField(t *testing.T) {
	business := "定义兼容-新增字段用默认值回放"
	old := v1()
	tests := []struct {
		name       string
		next       Schema
		wantField  string
		wantVal    any
		wantSource string
	}{
		{"新增字段带显式默认值", Schema{Name: "Pose", Fields: []Field{
			{Name: "frame", Type: String}, {Name: "x", Type: Float64},
			{Name: "conf", Type: Float64, Default: "0.95"}}}, "conf", 0.95, "default"},
		{"新增字段无默认值用固定零值", Schema{Name: "Pose", Fields: []Field{
			{Name: "frame", Type: String}, {Name: "x", Type: Float64},
			{Name: "flag", Type: Bool}}}, "flag", false, "zero"},
	}
	data, err := Encode(old, map[string]any{"frame": "map", "x": 1.5})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, notes, err := Decode(old, tc.next, data)
			if err != nil {
				t.Fatalf("期望兼容，实际被拒绝: %v", err)
			}
			if msg[tc.wantField] != tc.wantVal {
				t.Fatalf("新增字段默认值 = %v, 期望 %v", msg[tc.wantField], tc.wantVal)
			}
			if len(notes) != 1 || notes[0].Field != tc.wantField || notes[0].Source != tc.wantSource {
				t.Fatalf("默认值来源说明不符: %+v", notes)
			}
			logCase(t, business, "用例[%s] 通过：旧数据回放，字段 %s 以 %s 规则填充=%v",
				tc.name, tc.wantField, tc.wantSource, tc.wantVal)
		})
	}
}

func TestSchemaEvolveIncompatible(t *testing.T) {
	business := "定义不兼容-删除/改名/改类型必须拒绝"
	old := v1()
	data, err := Encode(old, map[string]any{"frame": "map", "x": 1.5})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		next Schema
		want string
	}{
		{"删除字段", Schema{Name: "Pose", Fields: []Field{{Name: "frame", Type: String}}}, "missing"},
		{"字段改名", Schema{Name: "Pose", Fields: []Field{{Name: "frame", Type: String}, {Name: "xx", Type: Float64}}}, "missing"},
		{"字段类型改变", Schema{Name: "Pose", Fields: []Field{{Name: "frame", Type: String}, {Name: "x", Type: Int64}}}, "type changed"},
		{"类型名改变", Schema{Name: "Pose2", Fields: []Field{{Name: "frame", Type: String}, {Name: "x", Type: Float64}}}, "type name changed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Decode(old, tc.next, data)
			var inc *IncompatibleError
			if !errors.As(err, &inc) {
				t.Fatalf("期望 IncompatibleError，实际 %v", err)
			}
			if !contains(inc.Reason, tc.want) {
				t.Fatalf("拒绝原因 %q 未包含 %q", inc.Reason, tc.want)
			}
			logCase(t, business, "用例[%s] 通过：明确拒绝，原因=%s", tc.name, inc.Reason)
		})
	}
}

func TestEncodeRejectsUnknownAndPositional(t *testing.T) {
	business := "编码安全-拒绝未知字段与缺字段，不靠位置猜测"
	if _, err := Encode(v1(), map[string]any{"frame": "map", "x": 1.0, "y": 2.0}); err == nil {
		t.Fatal("未知字段应被拒绝")
	}
	logCase(t, business, "用例[未知字段] 通过：拒绝写入")
	if _, err := Encode(v1(), map[string]any{"frame": "map"}); err == nil {
		t.Fatal("缺失必填字段应被拒绝")
	}
	logCase(t, business, "用例[缺字段] 通过：拒绝写入")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
