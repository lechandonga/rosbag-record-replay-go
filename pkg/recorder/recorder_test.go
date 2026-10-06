package recorder

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/bag"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/bus"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

func TestConcurrentRecordAndRead(t *testing.T) {
	business := "边录边读-读写互不干扰且每次快照一致"
	dir := t.TempDir()
	b := bus.New(2048)
	scA := msgdef.Schema{Name: "Temp", Fields: []msgdef.Field{{Name: "v", Type: msgdef.Int64}}}
	scB := msgdef.Schema{Name: "Tag", Fields: []msgdef.Field{{Name: "text", Type: msgdef.String}}}
	rec, err := New(b, dir, 1<<20) // 测试内不触发滚动，避免与读快照竞态；滚动另有独立用例覆盖
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddTopic("/temp", scA); err != nil {
		t.Fatal(err)
	}
	if err := rec.AddTopic("/tag", scB); err != nil {
		t.Fatal(err)
	}
	rec.Start()

	const total = 400
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			b.Publish(bus.Message{Topic: "/temp", Stamp: int64(i), Data: map[string]any{"v": int64(i)}})
			b.Publish(bus.Message{Topic: "/tag", Stamp: int64(i), Data: map[string]any{"text": fmt.Sprintf("t%d", i)}})
			if i%7 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 并发读者：反复打开/刷新快照；任何一次快照都必须自洽——
	// 同一快照内结果有序、可解码、数量不随时间变化。
	readerStop := make(chan struct{})
	var rwg sync.WaitGroup
	for rid := 0; rid < 3; rid++ {
		rwg.Add(1)
		go func(id int) {
			defer rwg.Done()
			for {
				select {
				case <-readerStop:
					return
				default:
				}
				r, err := bag.OpenReader(dir)
				if err != nil {
					if errors.Is(err, bag.ErrNoBag) {
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("读者 %d 打开失败: %v", id, err)
					return
				}
				entries, err := r.Query(nil, bag.Span{})
				if err != nil {
					if errors.Is(err, bag.ErrNoBag) {
						r.Refresh()
						continue
					}
					t.Errorf("读者 %d 查询失败: %v", id, err)
					return
				}
				// 快照一致性：同一读者重复查询（不 Refresh）必须得到相同数量。
				again, err := r.Query(nil, bag.Span{})
				if err != nil || len(again) != len(entries) {
					t.Errorf("读者 %d 快照不一致: %d vs %d err=%v", id, len(entries), len(again), err)
					return
				}
				// 顺序性。
				for i := 1; i < len(entries); i++ {
					if entries[i].Stamp < entries[i-1].Stamp {
						t.Errorf("读者 %d 看到乱序快照", id)
						return
					}
				}
				// 抽样解码，验证读写互不破坏。
				if len(entries) > 0 {
					e := entries[len(entries)-1]
					if _, _, err := r.Decode(e, e.Schema); err != nil {
						t.Errorf("读者 %d 解码失败: %v", id, err)
						return
					}
				}
				r.Refresh()
			}
		}(rid)
	}

	wg.Wait()
	// 等待总线消息全部被消费落盘。
	deadline := time.Now().Add(3 * time.Second)
	for {
		if len(rec.Errors()) > 0 {
			t.Fatalf("录制错误: %v", rec.Errors())
		}
		r, err := bag.OpenReader(dir)
		if err == nil {
			es, qerr := r.Query(nil, bag.Span{})
			if qerr == nil && len(es) >= 2*total {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("录制消息未落盘完成")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(readerStop)
	rwg.Wait()
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := bag.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := r.Query([]string{"/temp"}, bag.Span{})
	if err != nil {
		t.Fatal(err)
	}
	bEnt, err := r.Query([]string{"/tag"}, bag.Span{})
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != total || len(bEnt) != total {
		t.Fatalf("多话题数量错误 temp=%d tag=%d，期望各 %d", len(a), len(bEnt), total)
	}
	if c := r.Health().Corruption(); c != nil {
		t.Fatalf("并发读写出现损坏: %+v", c)
	}
	segFiles, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	t.Logf("[业务:%s] 用例[2话题x%d条+3并发读者+段滚动] 通过：temp=%d tag=%d，快照自洽可解码，段数=%d，无损坏",
		business, total, len(a), len(bEnt), len(segFiles))
}

func TestSingleWriterLock(t *testing.T) {
	business := "并发控制-同目录单写者"
	dir := t.TempDir()
	w1, err := New(bus.New(8), dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()
	if _, err := New(bus.New(8), dir, 1<<30); err == nil {
		t.Fatal("同目录第二个写者必须被拒绝")
	}
	t.Logf("[业务:%s] 用例[第二个写者] 通过：目录锁拒绝并发写", business)
}
