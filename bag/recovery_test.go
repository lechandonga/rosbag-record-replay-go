package bag_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

func writeNSimple(t *testing.T, dir, topic string, n int) {
	t.Helper()
	st, _, err := bag.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterTopic(topic, simpleDef); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		err := st.Write(bag.Message{
			Topic: topic, Timestamp: time.Unix(0, int64((i+1)*1_000_000)),
			Values: map[string]any{"id": int64(i), "name": "n"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCrashRecovery(t *testing.T) {
	c := newBusiness(t, "中断恢复")
	defer c.end()

	dir := t.TempDir()
	const topic = "/chatter"
	const fileTopic = "%2fchatter"
	writeNSimple(t, dir, topic, 5)

	dataPath := filepath.Join(dir, "data", "%2fchatter.bag")
	raw, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = fileTopic

	// 用例1：崩溃留下半截尾帧——重新打开后完整帧可读，半帧被截断，不算“损坏”。
	t.Run("半截尾帧被截断且完整帧可读", func(t *testing.T) {
		corruptDir := filepath.Join(t.TempDir(), "bag")
		writeNSimple(t, corruptDir, topic, 5)
		p := filepath.Join(corruptDir, "data", "%2fchatter.bag")
		r0, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, append(r0, 0x9D, 0x5B, 0xA2, 0x17, 0x00, 0x00, 0x00, 0x40, 0x01), 0o644); err != nil {
			t.Fatal(err)
		}
		st, report, err := bag.Open(corruptDir)
		if err != nil {
			t.Fatal(err)
		}
		rec := report.Topics[0]
		snap, err := st.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snap.Close()
		msgs, err := snap.ReadRange(bag.RangeFilter{Topics: []string{topic}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.check("5 条完整消息均可读出", len(msgs) == 5, "got", len(msgs))
		c.check("半截尾帧被报告为截断", rec.TruncatedTail > 0, "bytes", rec.TruncatedTail)
		c.check("截断不属于数据损坏", !rec.Corrupt && report.Healthy())
		st.Close()

		// 恢复后继续写入，新消息必须接在完整帧边界之后且全部可读。
		if err := st2(t, corruptDir, topic, 3); err != nil {
			t.Fatal(err)
		}
		rd, _, err := bag.OpenReader(corruptDir)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		s2, _ := rd.Snapshot()
		defer s2.Close()
		all, err := s2.ReadRange(bag.RangeFilter{Topics: []string{topic}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.check("恢复后追加的消息与旧消息连续可读", len(all) == 8, "got", len(all))
		// seq 唯一性验证无重复。
		seen := map[uint64]bool{}
		for _, m := range all {
			if seen[m.Seq] {
				t.Fatalf("duplicate seq %d", m.Seq)
			}
			seen[m.Seq] = true
		}
		c.check("写入序号全局唯一无重复", len(seen) == 8)
	})

	// 用例2：文件中间被插入垃圾字节（非尾部）——识别为“数据损坏”，并能重同步读出后续帧。
	t.Run("中间坏帧被标记为损坏并可重同步", func(t *testing.T) {
		mid := len(raw) / 2
		damaged := append([]byte{}, raw[:mid]...)
		damaged = append(damaged, 0xDE, 0xAD, 0xBE, 0xEF, 0x42)
		damaged = append(damaged, raw[mid:]...)
		badDir := filepath.Join(t.TempDir(), "bag")
		writeNSimple(t, badDir, topic, 5)
		if err := os.WriteFile(filepath.Join(badDir, "data", "%2fchatter.bag"), damaged, 0o644); err != nil {
			t.Fatal(err)
		}
		rd, report, err := bag.OpenReader(badDir)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		var found bool
		for _, r := range report.Topics {
			if r.Topic == topic {
				found = r.Corrupt
			}
		}
		c.check("中间损坏被明确标记 Corrupt=true", found)
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err = snap.ReadRange(bag.RangeFilter{Topics: []string{topic}}, nil)
		c.check("损坏区间的读取返回 ErrCorrupt", bag.IsCorrupt(err), "err", err)
	})

	// 用例3：“区间本来就没有数据”与“损坏”必须可区分。
	t.Run("空区间返回ErrNoData而非损坏", func(t *testing.T) {
		rd, report, err := bag.OpenReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		c.check("正常文件健康", report.Healthy())
		snap, _ := rd.Snapshot()
		defer snap.Close()
		_, err = snap.ReadRange(bag.RangeFilter{
			Topics: []string{topic},
			Start:  time.Unix(0, 100_000_000),
			End:    time.Unix(0, 200_000_000),
		}, nil)
		c.check("无数据区间返回 ErrNoData", errors.Is(err, bag.ErrNoData), "err", err)
		c.check("ErrNoData 不是 ErrCorrupt", !bag.IsCorrupt(err))

		// 未注册的话题也属于“没有数据”，而不是错误。
		_, err = snap.ReadRange(bag.RangeFilter{Topics: []string{"/never-existed"}}, nil)
		c.check("未知话题返回 ErrNoData", errors.Is(err, bag.ErrNoData))
	})
}

func st2(t *testing.T, dir, topic string, n int) error {
	st, _, err := bag.Open(dir)
	if err != nil {
		return err
	}
	defer st.Close()
	for i := 0; i < n; i++ {
		if err := st.Write(bag.Message{
			Topic: topic, Timestamp: time.Unix(0, int64((10+i)*1_000_000)),
			Values: map[string]any{"id": int64(10 + i), "name": "n"},
		}); err != nil {
			return err
		}
	}
	return nil
}
