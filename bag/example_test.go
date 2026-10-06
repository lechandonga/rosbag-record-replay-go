package bag_test

import (
	"fmt"
	"os"
	"time"

	bag "github.com/lechandonga/rosbag-record-replay-go/bag"
)

// Example 演示：注册定义 -> 本地发布订阅 -> 持续录制 -> 按新定义兼容回放的完整链路。
func Example() {
	dir, err := os.MkdirTemp("", "bag-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	poseV1 := bag.MessageDef{Name: "geometry/Pose", Fields: []bag.Field{
		{Name: "x", Type: bag.TypeFloat},
		{Name: "label", Type: bag.TypeString},
	}}

	st, report, err := bag.Open(dir)
	if err != nil {
		panic(err)
	}
	fmt.Println("open healthy:", report.Healthy())

	if err := st.RegisterTopic("/pose", poseV1); err != nil {
		panic(err)
	}
	bus := bag.NewBus()
	rec := bag.NewRecorder(bus, st, []string{"/pose"})

	bus.Publish(bag.Message{Topic: "/pose", Timestamp: time.Unix(0, 1_000_000),
		Values: map[string]any{"x": 1.5, "label": "home"}})
	bus.Publish(bag.Message{Topic: "/pose", Timestamp: time.Unix(0, 2_000_000),
		Values: map[string]any{"x": 2.5, "label": "dock"}})

	rec.Stop()
	if err := st.Close(); err != nil {
		panic(err)
	}

	// 定义演进：新增字段 y（旧数据回放时 y 得到固定默认值 0）。
	poseV2 := bag.MessageDef{Name: "geometry/Pose", Fields: []bag.Field{
		{Name: "x", Type: bag.TypeFloat},
		{Name: "y", Type: bag.TypeFloat},
		{Name: "label", Type: bag.TypeString},
	}}

	reader, _, err := bag.OpenReader(dir)
	if err != nil {
		panic(err)
	}
	snap, err := reader.Snapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Close()

	msgs, err := snap.ReadRange(bag.RangeFilter{Topics: []string{"/pose"}},
		map[string]bag.MessageDef{"/pose": poseV2})
	if err != nil {
		panic(err)
	}
	for _, m := range msgs {
		fmt.Printf("%s x=%v y=%v defaulted=%v\n", m.Topic, m.Values["x"], m.Values["y"], m.Defaulted)
	}
	// Output:
	// open healthy: true
	// /pose x=1.5 y=0 defaulted=[y]
	// /pose x=2.5 y=0 defaulted=[y]
}
