package bag_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

func TestConcurrentRecordRead(t *testing.T) {
	c := newBusiness(t, "并发录制与读取")
	defer c.end()

	dir := t.TempDir()
	st, _, err := bag.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.RegisterTopic("/a", simpleDef); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterTopic("/b", simpleDef); err != nil {
		t.Fatal(err)
	}
	bus := bag.NewBus()
	rec := bag.NewRecorder(bus, st, []string{"/a", "/b"})

	const writers = 4
	const perWriter = 120
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				topic := "/a"
				if (id+i)%2 == 0 {
					topic = "/b"
				}
				bus.Publish(bag.Message{
					Topic:     topic,
					Timestamp: time.Unix(0, int64(id*100000+i)),
					Values:    map[string]any{"id": int64(id*perWriter + i), "name": fmt.Sprintf("w%d", id)},
				})
			}
		}(w)
	}

	// 读取方在录制过程中反复创建快照；每个快照的内容必须自洽（不变）。
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	snapshotSizes := []int{}
	var snapMu sync.Mutex
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				snap, err := st.Snapshot()
				if err != nil {
					t.Error(err)
					return
				}
				msgs, err := snap.ReadRange(bag.RangeFilter{}, nil)
				if err != nil && !bag.IsNoData(err) {
					t.Error(err)
					snap.Close()
					return
				}
				n := len(msgs)
				// 二次读取同一快照，数量必须一致（快照冻结）。
				msgs2, err := snap.ReadRange(bag.RangeFilter{}, nil)
				if err != nil && !bag.IsNoData(err) {
					t.Error(err)
				}
				if (err == nil || bag.IsNoData(err)) && len(msgs2) != n {
					t.Errorf("snapshot not consistent: %d vs %d", n, len(msgs2))
				}
				snapMu.Lock()
				snapshotSizes = append(snapshotSizes, n)
				snapMu.Unlock()
				snap.Close()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()
	rec.Stop()
	close(stop)
	readerWG.Wait()

	rd, _, err := bag.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	snap, _ := rd.Snapshot()
	defer snap.Close()
	msgs, err := snap.ReadRange(bag.RangeFilter{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.check("全部消息最终落盘无丢失", len(msgs) == writers*perWriter, "got", len(msgs))
	keys := map[string]bool{}
	ordered := true
	var last int64 = -1
	for _, m := range msgs {
		key := fmt.Sprintf("%s#%d@%d", m.Topic, m.Seq, m.Timestamp.UnixNano())
		if keys[key] {
			t.Fatalf("duplicate message %s", key)
		}
		keys[key] = true
		if m.Timestamp.UnixNano() < last {
			ordered = false
		}
		last = m.Timestamp.UnixNano()
	}
	c.check("无重复消息", len(keys) == writers*perWriter)
	c.check("读取结果全局按时间有序", ordered)
	c.check("录制过程中至少观察到多个不同快照", len(snapshotSizes) > 1, "snapshots", len(snapshotSizes))
	if len(snapshotSizes) > 1 {
		monotonic := true
		for i := 1; i < len(snapshotSizes); i++ {
			if snapshotSizes[i] < snapshotSizes[i-1] {
				monotonic = false
			}
		}
		c.check("快照可见消息数单调不减（读不到半帧）", monotonic, snapshotSizes)
	}
}
