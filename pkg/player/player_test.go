package player

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/bag"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

type recMsg struct {
	topic string
	stamp int64
	data  map[string]any
}

func buildBag(t *testing.T, dir string, n int, schemas ...msgdef.Schema) (*bag.Reader, msgdef.Schema, []recMsg) {
	t.Helper()
	sc := msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{
		{Name: "frame", Type: msgdef.String}, {Name: "x", Type: msgdef.Float64}}}
	if len(schemas) > 0 {
		sc = schemas[0]
	}
	w, err := bag.OpenWriter(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RegisterTopic("/pose", "Pose", sc); err != nil {
		t.Fatal(err)
	}
	msgs := make([]recMsg, n)
	for i := 0; i < n; i++ {
		d := map[string]any{"frame": "map", "x": float64(i)}
		p, err := msgdef.Encode(sc, d)
		if err != nil {
			t.Fatal(err)
		}
		// 间隔 10ms。
		if err := w.Write("/pose", int64(i)*10_000_000, p); err != nil {
			t.Fatal(err)
		}
		msgs[i] = recMsg{"/pose", int64(i) * 10_000_000, d}
	}
	w.Close()
	r, err := bag.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r, sc, msgs
}

func drain(t *testing.T, p *Player) []Event {
	t.Helper()
	var out []Event
	for ev := range p.Events() {
		out = append(out, ev)
	}
	if err := p.Err(); err != nil {
		t.Fatalf("回放意外错误: %v", err)
	}
	return out
}

func TestPlaybackBasicOrderNoGaps(t *testing.T) {
	business := "回放-原始顺序不重不漏"
	r, _, _ := buildBag(t, t.TempDir(), 5)
	p, err := New(r, Options{Rate: -1}) // -1 表示尽快模式，仅为测试确定性
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	out := drain(t, p)
	if len(out) != 5 {
		t.Fatalf("收到 %d 条，期望 5", len(out))
	}
	for i, ev := range out {
		if ev.Index != i || ev.Data["x"] != float64(i) {
			t.Fatalf("第 %d 条异常: %+v", i, ev)
		}
	}
	t.Logf("[业务:%s] 用例[尽快完整回放] 通过：%d 条，序号与内容连续", business, len(out))
}

func TestPlaybackRateTiming(t *testing.T) {
	business := "回放-变速时间语义"
	r, _, _ := buildBag(t, t.TempDir(), 5)
	// Rate=10，原始跨度 40ms，期望约 4ms 完成。
	p, err := New(r, Options{Rate: 10})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	start := time.Now()
	out := drain(t, p)
	elapsed := time.Since(start)
	if len(out) != 5 {
		t.Fatalf("收到 %d 条", len(out))
	}
	if elapsed > 30*time.Millisecond {
		t.Fatalf("10 倍速耗时 %v，明显没有按节奏压缩", elapsed)
	}
	t.Logf("[业务:%s] 用例[10x 变速] 通过：原始跨度40ms 实际耗时 %v", business, elapsed.Round(time.Millisecond))
}

func TestPlaybackSpanTrim(t *testing.T) {
	business := "回放-区间裁剪"
	r, _, msgs := buildBag(t, t.TempDir(), 5)
	p, err := New(r, Options{Rate: -1, Span: bag.Span{Start: msgs[1].stamp, End: msgs[3].stamp}})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	out := drain(t, p)
	if len(out) != 2 || out[0].Data["x"] != 1.0 || out[1].Data["x"] != 2.0 {
		t.Fatalf("区间裁剪结果错误: %+v", out)
	}
	t.Logf("[业务:%s] 用例[半开区间裁剪] 通过：仅回放 x=1,2 两条", business)
}

func TestPlaybackLoop(t *testing.T) {
	business := "回放-循环且轮次边界不重不漏"
	r, _, _ := buildBag(t, t.TempDir(), 3)
	p, err := New(r, Options{Rate: -1, Loop: true})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	collected := map[int]int{} // loop -> count
	deadline := time.Now().Add(2 * time.Second)
	for {
		if collected[2] == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("循环回放未推进到第 3 轮完整结束: %v", collected)
		}
		select {
		case ev := <-p.Events():
			collected[ev.Loop]++
		case <-time.After(500 * time.Millisecond):
		}
	}
	p.Close()
	for loop, cnt := range collected {
		if loop <= 2 && cnt != 3 {
			t.Fatalf("第 %d 轮收到 %d 条，期望每轮 3 条", loop, cnt)
		}
	}
	t.Logf("[业务:%s] 用例[三轮循环] 通过：每轮恰好 3 条，轮次边界无重复无漏发: %v", business, collected)
}

func TestPlaybackPauseResume(t *testing.T) {
	business := "回放-暂停恢复不重不漏"
	r, _, _ := buildBag(t, t.TempDir(), 10)
	p, err := New(r, Options{Rate: 100})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	// 收到若干条后立即暂停（速率很快，不硬编码收到几条）。
	got := []Event{}
	ev := <-p.Events()
	got = append(got, ev)
	p.Pause()
	pos, length, _, paused := p.Position()
	if !paused || pos < 1 || length != 10 {
		t.Fatalf("暂停状态异常 pos=%d len=%d paused=%v", pos, length, paused)
	}
	// 排空暂停前已经在路上的消息（通道缓冲外不会再产生新消息）。
	drained := true
	for drained {
		select {
		case ev := <-p.Events():
			got = append(got, ev)
		default:
			drained = false
		}
	}
	time.Sleep(60 * time.Millisecond)
	// 暂停期间不应再有消息。
	select {
	case ev, ok := <-p.Events():
		if ok {
			t.Fatalf("暂停期间不应收到消息，却收到 %+v", ev)
		}
	default:
	}
	p.Resume()
	for ev := range p.Events() {
		got = append(got, ev)
	}
	if err := p.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("暂停恢复后共收到 %d 条，期望 10", len(got))
	}
	seen := map[int]bool{}
	for _, ev := range got {
		if seen[ev.Index] {
			t.Fatalf("位置 %d 重复发送", ev.Index)
		}
		seen[ev.Index] = true
	}
	t.Logf("[业务:%s] 用例[暂停60ms后恢复] 通过：暂停期间静默，恢复后收齐剩余消息，无重复/漏发", business)
}

func TestPlaybackSeek(t *testing.T) {
	business := "回放-跳转不重不漏不倒序"
	r, _, _ := buildBag(t, t.TempDir(), 10)
	p, err := New(r, Options{Rate: 1})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	first := <-p.Events()
	if first.Index != 0 {
		t.Fatal("第一条应为 index 0")
	}
	if err := p.SeekTo(6); err != nil {
		t.Fatal(err)
	}
	got := []Event{first}
	for ev := range p.Events() {
		got = append(got, ev)
	}
	if err := p.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int{0, 6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("跳转后收到 %d 条 %v，期望 %d 条 %v", len(got), indices(got), len(want), want)
	}
	for i, ev := range got {
		if ev.Index != want[i] {
			t.Fatalf("第 %d 条 index=%d，期望 %d（1..5不补发、0不重发、不倒序）", i, ev.Index, want[i])
		}
	}
	t.Logf("[业务:%s] 用例[跳到6] 通过：实际序列=%v，1..5不补发、0不重发、顺序不倒置", business, want)

	// 越界跳转应报错。
	r2, _, _ := buildBag(t, t.TempDir(), 3)
	p2, _ := New(r2, Options{Rate: -1})
	if err := p2.SeekTo(99); err == nil {
		t.Fatal("越界跳转必须报错")
	}
	t.Logf("[业务:%s] 用例[越界跳转] 通过：明确拒绝", business)
}

func TestPlaybackSchemaEvolve(t *testing.T) {
	business := "回放-定义演进兼容与拒绝"
	r, _, _ := buildBag(t, t.TempDir(), 2)

	// 兼容：新增字段带默认值。
	newOK := msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{
		{Name: "frame", Type: msgdef.String}, {Name: "x", Type: msgdef.Float64},
		{Name: "conf", Type: msgdef.Float64, Default: "0.8"}}}
	p, err := New(r, Options{Rate: -1, Schemas: map[string]msgdef.Schema{"/pose": newOK}})
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	var evs []Event
	for ev := range p.Events() {
		evs = append(evs, ev)
	}
	if evs[0].Data["conf"] != 0.8 || len(evs[0].Notes) != 1 {
		t.Fatalf("新增字段默认值未生效: %+v notes=%+v", evs[0].Data, evs[0].Notes)
	}
	t.Logf("[业务:%s] 用例[新增字段] 通过：旧数据回放 conf=0.8（default），原字段不变", business)

	// 不兼容：删除字段，New 时不报错（查询不涉及解码），收到第一条时播放器暴露错误。
	bad := msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{{Name: "frame", Type: msgdef.String}}}
	r2, _, _ := buildBag(t, t.TempDir(), 2)
	p2, err := New(r2, Options{Rate: -1, Schemas: map[string]msgdef.Schema{"/pose": bad}})
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	for range p2.Events() {
	}
	var inc *msgdef.IncompatibleError
	if !errors.As(p2.Err(), &inc) {
		t.Fatalf("删除字段回放必须暴露 IncompatibleError，实际 %v", p2.Err())
	}
	t.Logf("[业务:%s] 用例[删除字段] 通过：播放器明确报错：%s", business, inc.Reason)
}
