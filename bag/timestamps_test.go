package bag_test

import (
	"path/filepath"
	"testing"
	"time"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

func TestTimestampEdgeCases(t *testing.T) {
	c := newBusiness(t, "同一时间戳与乱序时间戳")
	defer c.end()

	dir := t.TempDir()
	st, _, err := bag.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterTopic("/a", simpleDef); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterTopic("/b", simpleDef); err != nil {
		t.Fatal(err)
	}
	// 乱序写入（时间戳 30 -> 10 -> 20），且 /a 与 /b 在同一时刻各有一条。
	same := time.Unix(0, 20_000_000)
	write := func(topic string, ts time.Time, seqHint int64) {
		if err := st.Write(bag.Message{Topic: topic, Timestamp: ts,
			Values: map[string]any{"id": seqHint, "name": "n"}}); err != nil {
			t.Fatal(err)
		}
	}
	write("/a", time.Unix(0, 30_000_000), 1)
	write("/a", time.Unix(0, 10_000_000), 2)
	write("/a", same, 3)
	write("/b", same, 4)
	write("/b", time.Unix(0, 10_000_000), 5)
	st.Close()

	rd, _, _ := bag.OpenReader(dir)
	defer rd.Close()
	snap, _ := rd.Snapshot()
	defer snap.Close()
	msgs, err := snap.ReadRange(bag.RangeFilter{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 期望顺序：10(/a,seq1) 10(/b,seq0) 20(/a,seq2) 20(/b,seq1) 30(/a,seq0)
	want := []struct {
		ts    int64
		topic string
	}{
		{10, "/a"}, {10, "/b"}, {20, "/a"}, {20, "/b"}, {30, "/a"},
	}
	ok := len(msgs) == 5
	for i, m := range msgs {
		if i >= len(want) || m.Timestamp.UnixMilli() != want[i].ts || m.Topic != want[i].topic {
			ok = false
		}
	}
	c.check("乱序写入后读取结果按(时间,话题,序号)有序", ok, dump(msgs))

	// 闭区间边界：Start=End=same 时，两个话题同一时刻的消息都必须命中。
	atSame, err := snap.ReadRange(bag.RangeFilter{Start: same, End: same}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.check("同一时间戳的多条消息全部命中闭区间", len(atSame) == 2, "got", len(atSame))
	c.check("同刻消息顺序为 /a 先于 /b", atSame[0].Topic == "/a" && atSame[1].Topic == "/b")

	// 区间裁剪：[10ms, 20ms] 共 4 条，不含 30ms。
	clip, err := snap.ReadRange(bag.RangeFilter{
		Start: time.Unix(0, 10_000_000),
		End:   time.Unix(0, 20_000_000),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.check("时间区间裁剪边界正确", len(clip) == 4, "got", len(clip))

	// 截取到新 bag 后内容一致且独立。
	dst := filepath.Join(t.TempDir(), "clipped")
	n, err := snap.Clip(dst, bag.RangeFilter{Start: same, End: same}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.check("Clip 截取同刻消息 2 条", n == 2, "n", n)
	rd2, _, _ := bag.OpenReader(dst)
	defer rd2.Close()
	s2, _ := rd2.Snapshot()
	defer s2.Close()
	topics := s2.Topics()
	c.check("Clip 保留涉及的两个话题", len(topics) == 2, topics)
	got, err := s2.ReadRange(bag.RangeFilter{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.check("Clip 输出 bag 数据一致", len(got) == 2 && got[0].Timestamp.Equal(same))
}

func dump(ms []bag.ReplayedMessage) []string {
	out := []string{}
	for _, m := range ms {
		out = append(out, m.Topic+"@"+m.Timestamp.Format("15:04:05.000"))
	}
	return out
}
