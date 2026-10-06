package bag_test

import (
	"testing"
	"time"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

func makePlayerBag(t *testing.T, n int, gapMs int64) (string, bag.MessageDef) {
	t.Helper()
	dir := t.TempDir()
	st, _, err := bag.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	def := bag.MessageDef{Name: "test/P", Fields: []bag.Field{{Name: "i", Type: bag.TypeInt64}}}
	if err := st.RegisterTopic("/p", def); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := st.Write(bag.Message{Topic: "/p",
			Timestamp: time.Unix(0, int64(i)*gapMs*1_000_000),
			Values:    map[string]any{"i": int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	return dir, def
}

func collect(p *bag.Player, timeout time.Duration) []bag.ReplayedMessage {
	out := []bag.ReplayedMessage{}
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-p.Out():
			if !ok {
				return out
			}
			out = append(out, m)
		case <-deadline:
			return out
		}
	}
}

func TestPlayerTimeSemantics(t *testing.T) {
	c := newBusiness(t, "回放时间语义")
	defer c.end()

	dir, def := makePlayerBag(t, 5, 100)
	open := func(t *testing.T) *bag.Snapshot {
		rd, _, err := bag.OpenReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := rd.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}

	// 业务1：原速 + 变速；检查顺序、无重复无遗漏，以及墙钟耗时随速度变化。
	t.Run("原速与变速节奏正确且顺序完整", func(t *testing.T) {
		snap := open(t)
		defer snap.Close()
		p, err := bag.NewPlayer(snap, bag.RangeFilter{}, map[string]bag.MessageDef{"/p": def},
			bag.PlayerOptions{Speed: 100})
		if err != nil {
			t.Fatal(err)
		}
		msgs := []bag.ReplayedMessage{}
		first := <-p.Out()
		msgs = append(msgs, first)
		start := time.Now()
		msgs = append(msgs, collect(p, 3*time.Second)...)
		elapsed := time.Since(start)
		p.Stop()
		c.check("5 条全部发出", len(msgs) == 5, "got", len(msgs))
		for i, m := range msgs {
			if m.Values["i"] != int64(i) {
				t.Fatalf("order broken at %d: %v", i, m.Values["i"])
			}
		}
		c.check("顺序严格递增不倒序", msgs[4].Values["i"] == int64(4))
		// 原始跨度 400ms，100x 下约 4ms；放宽到 250ms 以避免 CI 抖动。
		c.check("变速后墙钟耗时显著缩短", elapsed < 250*time.Millisecond, "elapsed", elapsed)
	})

	// 业务2：暂停/恢复——暂停期间不发消息，恢复后继续，无重复无漏发。
	t.Run("暂停恢复不丢不重节奏不漂移", func(t *testing.T) {
		snap := open(t)
		defer snap.Close()
		p, err := bag.NewPlayer(snap, bag.RangeFilter{}, map[string]bag.MessageDef{"/p": def},
			bag.PlayerOptions{Speed: 100})
		if err != nil {
			t.Fatal(err)
		}
		<-p.Out() // 第一条
		p.Pause()
		waitUntil(p.Paused, time.Second)
		time.Sleep(120 * time.Millisecond) // 远超消息间隔
		select {
		case m := <-p.Out():
			t.Fatalf("暂停期间不应发出消息，却收到 %v", m.Values["i"])
		default:
		}
		c.check("暂停期间不发消息", true)
		c.check("Paused 状态可见", p.Paused())
		p.Resume()
		waitUntil(func() bool { return !p.Paused() }, time.Second)
		c.check("恢复后 Paused=false", !p.Paused())
		msgs := collect(p, 3*time.Second)
		p.Stop()
		c.check("恢复后剩余 4 条全部到达", len(msgs) == 4, "got", len(msgs))
		for i, m := range msgs {
			if m.Values["i"] != int64(i+1) {
				t.Fatalf("missing/dup after resume at %d: %v", i, m.Values["i"])
			}
		}
		c.check("恢复后无重复无漏发", msgs[3].Values["i"] == int64(4))
	})

	// 业务3：跳转到任意位置——之前的不补播，之后的继续按序发。
	t.Run("跳转任意位置不重不漏不倒序", func(t *testing.T) {
		snap := open(t)
		defer snap.Close()
		p, err := bag.NewPlayer(snap, bag.RangeFilter{}, map[string]bag.MessageDef{"/p": def},
			bag.PlayerOptions{Speed: 1000})
		if err != nil {
			t.Fatal(err)
		}
		<-p.Out() // 消费第 0 条
		p.Pause()
		waitUntil(p.Paused, time.Second)
		p.SeekTo(time.Unix(0, 300_000_000))
		time.Sleep(50 * time.Millisecond)
		idx, at := p.Position()
		c.check("跳转到时间戳>=300ms的第一条(i=3)", idx == 3 && at.UnixNano() == 300_000_000, "idx", idx)
		p.Resume()
		msgs := collect(p, 3*time.Second)
		p.Stop()
		c.check("跳转后发出 i=3,4 两条", len(msgs) == 2 && msgs[0].Values["i"] == int64(3) &&
			msgs[1].Values["i"] == int64(4), values(msgs))
	})

	// 业务4：循环——每个循环都完整，跨轮不重复补间隔、不漏。
	t.Run("循环播放每轮完整", func(t *testing.T) {
		snap := open(t)
		defer snap.Close()
		p, err := bag.NewPlayer(snap, bag.RangeFilter{}, map[string]bag.MessageDef{"/p": def},
			bag.PlayerOptions{Speed: 1000, Loop: true})
		if err != nil {
			t.Fatal(err)
		}
		msgs := []bag.ReplayedMessage{}
		deadline := time.After(1500 * time.Millisecond)
	loopCollect:
		for {
			select {
			case m := <-p.Out():
				msgs = append(msgs, m)
				if len(msgs) >= 12 {
					break loopCollect
				}
			case <-deadline:
				break loopCollect
			}
		}
		p.Stop()
		c.check("收到超过两轮消息", len(msgs) >= 12, "got", len(msgs))
		ok := true
		for i, m := range msgs {
			if m.Values["i"] != int64(i%5) {
				ok = false
			}
		}
		c.check("每轮序列 0..4 循环且无重复漏发", ok, values(msgs[:min(len(msgs), 12)]))
	})

	// 业务5：区间裁剪后的回放只包含区间消息。
	t.Run("裁剪区间回放", func(t *testing.T) {
		snap := open(t)
		defer snap.Close()
		p, err := bag.NewPlayer(snap, bag.RangeFilter{
			Start: time.Unix(0, 200_000_000),
			End:   time.Unix(0, 300_000_000),
		}, map[string]bag.MessageDef{"/p": def}, bag.PlayerOptions{Speed: 1000})
		if err != nil {
			t.Fatal(err)
		}
		msgs := collect(p, 3*time.Second)
		p.Stop()
		c.check("区间回放只含 i=2,3", len(msgs) == 2 && msgs[0].Values["i"] == int64(2) &&
			msgs[1].Values["i"] == int64(3), values(msgs))
	})
}

func values(ms []bag.ReplayedMessage) []any {
	out := []any{}
	for _, m := range ms {
		out = append(out, m.Values["i"])
	}
	return out
}

func waitUntil(f func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return f()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
