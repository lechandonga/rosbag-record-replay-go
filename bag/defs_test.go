package bag_test

import (
	"testing"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

var simpleDef = bag.MessageDef{Name: "test/Simple", Fields: []bag.Field{
	{Name: "id", Type: bag.TypeInt64},
	{Name: "name", Type: bag.TypeString},
}}

var nestedV1 = bag.MessageDef{Name: "test/Event", Fields: []bag.Field{
	{Name: "code", Type: bag.TypeInt64},
	{Name: "meta", Type: bag.TypeMessage, Fields: []bag.Field{
		{Name: "level", Type: bag.TypeInt64},
	}},
	{Name: "tags", Type: bag.TypeArray, ArrayElem: &bag.Field{Name: "tag", Type: bag.TypeString}},
}}

func recordOne(t *testing.T, dir, topic string, def bag.MessageDef, values map[string]any) {
	t.Helper()
	st, _, err := bag.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.RegisterTopic(topic, def); err != nil {
		t.Fatal(err)
	}
	if err := st.Write(bag.Message{Topic: topic, Values: values}); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitionEvolution(t *testing.T) {
	c := newBusiness(t, "消息定义演进兼容")
	defer c.end()

	// 业务1：新增字段——旧数据按固定默认值回放，并明确告知哪些字段是默认的。
	t.Run("新增字段使用固定默认值", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/t", simpleDef, map[string]any{"id": int64(7), "name": "a"})

		v2 := bag.MessageDef{Name: "test/Simple", Fields: []bag.Field{
			{Name: "id", Type: bag.TypeInt64},
			{Name: "name", Type: bag.TypeString},
			{Name: "score", Type: bag.TypeFloat},
			{Name: "ok", Type: bag.TypeBool},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		msgs, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/t"}},
			map[string]bag.MessageDef{"/t": v2})
		if err != nil {
			t.Fatal(err)
		}
		m := msgs[0]
		c.check("原字段值保持不变", m.Values["id"] == int64(7) && m.Values["name"] == "a")
		c.check("新增 float64 默认 0", m.Values["score"] == float64(0))
		c.check("新增 bool 默认 false", m.Values["ok"] == false)
		c.check("默认字段路径被显式列出", len(m.Defaulted) == 2 && m.Defaulted[0] == "score" && m.Defaulted[1] == "ok", m.Defaulted)
	})

	// 业务2：嵌套新增字段也要默认填充。
	t.Run("嵌套消息新增字段递归默认", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/e", nestedV1, map[string]any{
			"code": int64(1),
			"meta": map[string]any{"level": int64(3)},
			"tags": []any{"x", "y"},
		})
		v2 := bag.MessageDef{Name: "test/Event", Fields: []bag.Field{
			{Name: "code", Type: bag.TypeInt64},
			{Name: "meta", Type: bag.TypeMessage, Fields: []bag.Field{
				{Name: "level", Type: bag.TypeInt64},
				{Name: "trace", Type: bag.TypeString},
			}},
			{Name: "tags", Type: bag.TypeArray, ArrayElem: &bag.Field{Name: "tag", Type: bag.TypeString}},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		msgs, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/e"}},
			map[string]bag.MessageDef{"/e": v2})
		if err != nil {
			t.Fatal(err)
		}
		meta := msgs[0].Values["meta"].(map[string]any)
		c.check("嵌套原字段不变", meta["level"] == int64(3))
		c.check("嵌套新字段默认空串", meta["trace"] == "")
		c.check("默认路径含嵌套前缀", msgs[0].Defaulted[0] == "meta.trace", msgs[0].Defaulted)
	})

	// 业务3：删除字段——拒绝并给出原因。
	t.Run("删除字段被拒绝并说明原因", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/t", simpleDef, map[string]any{"id": int64(1), "name": "a"})
		v2 := bag.MessageDef{Name: "test/Simple", Fields: []bag.Field{
			{Name: "id", Type: bag.TypeInt64},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/t"}},
			map[string]bag.MessageDef{"/t": v2})
		ie, ok := bag.AsIncompatible(err)
		c.check("返回 IncompatibleError", ok)
		c.check("原因指出字段被删除/改名", ok && len(ie.Reasons) == 1 &&
			contains(ie.Reasons[0], "removed/renamed"), ie.Reasons)
	})

	// 业务4：改名字段——按“旧名被删除 + 新名走默认值”检测，必须拒绝（不能按位置猜）。
	t.Run("改名字段被拒绝而不是按位置解析", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/t", simpleDef, map[string]any{"id": int64(9), "name": "a"})
		renamed := bag.MessageDef{Name: "test/Simple", Fields: []bag.Field{
			{Name: "id", Type: bag.TypeInt64},
			{Name: "identifier", Type: bag.TypeString},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/t"}},
			map[string]bag.MessageDef{"/t": renamed})
		ie, ok := bag.AsIncompatible(err)
		c.check("改名导致拒绝", ok && len(ie.Reasons) >= 1, err)
		c.check("不会把旧值错位塞进新名字段", ie != nil)
	})

	// 业务5：类型不符——拒绝并给出双方类型。
	t.Run("字段类型不符被拒绝并给出双方类型", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/t", simpleDef, map[string]any{"id": int64(9), "name": "a"})
		typed := bag.MessageDef{Name: "test/Simple", Fields: []bag.Field{
			{Name: "id", Type: bag.TypeString},
			{Name: "name", Type: bag.TypeString},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/t"}},
			map[string]bag.MessageDef{"/t": typed})
		ie, ok := bag.AsIncompatible(err)
		c.check("类型不符返回不兼容错误", ok)
		c.check("原因同时给出录制类型与当前类型", ok && contains(ie.Reasons[0], "recorded as int64") &&
			contains(ie.Reasons[0], "requires string"), ie.Reasons)
	})

	// 业务6：数组元素类型不符同样拒绝。
	t.Run("数组元素类型不符被拒绝", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/e", nestedV1, map[string]any{
			"code": int64(1),
			"meta": map[string]any{"level": int64(1)},
			"tags": []any{"x"},
		})
		v2 := bag.MessageDef{Name: "test/Event", Fields: []bag.Field{
			{Name: "code", Type: bag.TypeInt64},
			{Name: "meta", Type: bag.TypeMessage, Fields: []bag.Field{{Name: "level", Type: bag.TypeInt64}}},
			{Name: "tags", Type: bag.TypeArray, ArrayElem: &bag.Field{Name: "tag", Type: bag.TypeInt64}},
		}}
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/e"}},
			map[string]bag.MessageDef{"/e": v2})
		// 元素是无名字段（数组元素直接写值），读取时类型标签由容器定义决定，
		// 不匹配会在 CRC 之后的解码阶段暴露为损坏或不兼容；这里至少不允许静默成功。
		c.check("数组元素类型变化不会静默成功", err != nil, "err", err)
	})

	// 业务7：定义相同（仅重新注册）不应产生多余 schema，且正常回放。
	t.Run("相同定义复用schema且无默认字段", func(t *testing.T) {
		dir := t.TempDir()
		recordOne(t, dir, "/t", simpleDef, map[string]any{"id": int64(2), "name": "b"})
		rd, _, _ := bag.OpenReader(dir)
		defer rd.Close()
		snap, _ := rd.Snapshot()
		defer snap.Close()
		msgs, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/t"}},
			map[string]bag.MessageDef{"/t": simpleDef})
		if err != nil {
			t.Fatal(err)
		}
		c.check("无演进时没有默认字段", len(msgs[0].Defaulted) == 0)
		c.check("值正确", msgs[0].Values["id"] == int64(2))
	})
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
