package bag

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

func schemaV1() msgdef.Schema {
	return msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{
		{Name: "frame", Type: msgdef.String},
		{Name: "x", Type: msgdef.Float64},
	}}
}

func writeN(t *testing.T, w *Writer, sc msgdef.Schema, n int) {
	t.Helper()
	if err := w.RegisterTopic("/pose", "Pose", sc); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		p, err := msgdef.Encode(sc, map[string]any{"frame": "map", "x": float64(i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Write("/pose", int64(100+i), p); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCrashRecoveryDiscardsHalfMessage(t *testing.T) {
	business := "中断恢复-完整消息可读，半截帧不混入"
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	sc := schemaV1()
	writeN(t, w, sc, 5)
	w.Close()

	// 模拟崩溃：在段文件末尾追加若干垃圾字节（一条没写完的帧）。
	seg := filepath.Join(dir, "seg-00000.log")
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 0, 0, 0, 1, 200, 'g', 'a', 'r', 'b', 'a', 'g', 'e'})
	f.Close()

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("崩溃后应可打开: %v", err)
	}
	got, err := r.Query(nil, Span{})
	if err != nil {
		t.Fatalf("末尾半截帧不应算损坏: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("完整消息数 = %d, 期望 5（半截帧不得混入）", len(got))
	}
	recovered := false
	for _, is := range r.Health().Issues {
		if is.Kind == IssuePartialTail {
			recovered = true
		}
	}
	if !recovered {
		t.Fatal("健康报告应包含 partial_tail_recovered")
	}
	t.Logf("[业务:%s] 用例[崩溃后追加半截帧] 通过：读出 %d 条完整消息，半截帧被安全丢弃",
		business, len(got))

	// 写者重新打开时自动截断半截帧，之后数据仍可继续追加。
	w2, err := OpenWriter(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	writeNAppend := func() {
		p, _ := msgdef.Encode(sc, map[string]any{"frame": "map", "x": 9.0})
		if err := w2.Write("/pose", 200, p); err != nil {
			t.Fatal(err)
		}
	}
	writeNAppend()
	w2.Close()
	r2, _ := OpenReader(dir)
	got2, _ := r2.Query(nil, Span{})
	if len(got2) != 6 {
		t.Fatalf("恢复并续写后消息数 = %d, 期望 6", len(got2))
	}
	t.Logf("[业务:%s] 用例[恢复后续写] 通过：共 %d 条，顺序连续", business, len(got2))
}

func TestCorruptionDistinctFromEmptyRange(t *testing.T) {
	business := "损坏与空区间区分"
	dir := t.TempDir()
	w, _ := OpenWriter(dir, 1<<30)
	sc := schemaV1()
	writeN(t, w, sc, 6)
	w.Close()

	r, _ := OpenReader(dir)

	// 区间本来就没有数据：正常返回空切片、无错误。
	empty, err := r.Query(nil, Span{Start: 1_000_000, End: 2_000_000})
	if err != nil {
		t.Fatalf("空区间不应报错: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("空区间应返回 0 条，实际 %d", len(empty))
	}
	t.Logf("[业务:%s] 用例[区间无数据] 通过：返回空结果且无错误", business)

	// 全新未录制目录：ErrNoBag，也区别于损坏。
	if _, err := OpenReader(t.TempDir()); !errors.Is(err, ErrNoBag) {
		t.Fatalf("空目录应返回 ErrNoBag，实际 %v", err)
	}
	t.Logf("[业务:%s] 用例[目录从未录制] 通过：返回 ErrNoBag", business)

	// 在段中间篡改一帧（越过帧头，翻转负载字节），制造硬损坏。
	seg := filepath.Join(dir, "seg-00000.log")
	raw, _ := os.ReadFile(seg)
	// 第一条消息帧大致位于登记帧之后；翻转中段一个字节。
	pos := len(raw) / 2
	raw[pos] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("打开不应失败，损坏在查询时暴露: %v", err)
	}
	if r2.Health().Corruption() == nil {
		t.Fatal("健康报告应报告 corruption")
	}
	if _, err := r2.Query(nil, Span{}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("中间损坏查询必须返回 ErrCorruption，实际 %v", err)
	}
	t.Logf("[业务:%s] 用例[段中间篡改] 通过：查询明确返回 ErrCorruption，绝不静默返回部分数据", business)
}

func TestSameTimestampAndOutOfOrder(t *testing.T) {
	business := "同时间戳与乱序时间戳的确定性读取"
	dir := t.TempDir()
	w, _ := OpenWriter(dir, 1<<30)
	sc := schemaV1()
	if err := w.RegisterTopic("/a", "Pose", sc); err != nil {
		t.Fatal(err)
	}
	// 乱序写入：stamp 50, 10, 10, 30
	stamps := []int64{50, 10, 10, 30}
	for i, st := range stamps {
		p, _ := msgdef.Encode(sc, map[string]any{"frame": fmt.Sprintf("m%d", i), "x": float64(i)})
		if err := w.Write("/a", st, p); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	r, _ := OpenReader(dir)
	got, err := r.Query([]string{"/a"}, Span{})
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []int64{10, 10, 30, 50}
	if len(got) != 4 {
		t.Fatalf("消息数 = %d", len(got))
	}
	for i, e := range got {
		if e.Stamp != wantOrder[i] {
			t.Fatalf("位置 %d 时间戳 = %d, 期望 %d", i, e.Stamp, wantOrder[i])
		}
	}
	// 同一时间戳两条消息：用序列号确定先后，内容不颠倒。
	m0, _, _ := r.Decode(got[0], sc)
	m1, _, _ := r.Decode(got[1], sc)
	if m0["frame"] != "m1" || m1["frame"] != "m2" {
		t.Fatalf("同时间戳顺序错乱: %v %v", m0["frame"], m1["frame"])
	}
	t.Logf("[业务:%s] 用例[乱序写入] 通过：读取顺序=%v，已按(时间戳,序列号)归位", business, wantOrder)
	t.Logf("[业务:%s] 用例[相同时间戳] 通过：两条同戳消息保持写入先后，内容不颠倒", business)

	// 按话题 + 时间区间裁剪。
	sub, err := r.Query([]string{"/a"}, Span{Start: 20, End: 40})
	if err != nil {
		t.Fatal(err)
	}
	if len(sub) != 1 || sub[0].Stamp != 30 {
		t.Fatalf("区间[20,40) 裁剪错误，得到 %+v", sub)
	}
	t.Logf("[业务:%s] 用例[话题+时间区间裁剪] 通过：[20,40) 仅命中 stamp=30", business)
}

func TestSegmentRotation(t *testing.T) {
	business := "长时录制-段滚动不丢消息"
	dir := t.TempDir()
	w, err := OpenWriter(dir, 300) // 极小段，强制频繁滚动
	if err != nil {
		t.Fatal(err)
	}
	sc := schemaV1()
	if err := w.RegisterTopic("/p", "Pose", sc); err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		p, _ := msgdef.Encode(sc, map[string]any{"frame": "f", "x": float64(i)})
		if err := w.Write("/p", int64(i), p); err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	if len(segs) < 2 {
		t.Fatalf("期望发生段滚动，实际段数 %d", len(segs))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Query(nil, Span{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("跨段消息数 = %d，期望 %d", len(got), n)
	}
	for i, e := range got {
		if e.Stamp != int64(i) {
			t.Fatalf("跨段顺序错乱，位置 %d stamp=%d", i, e.Stamp)
		}
	}
	t.Logf("[业务:%s] 用例[%d条消息跨%d段] 通过：无丢失、全局有序", business, n, len(segs))
}
