# rosbag-record-replay-go

不依赖真实 ROS 环境的本地话题录制与回放组件：本地发布/订阅模拟话题通信，
本地目录保存录制数据，提供崩溃恢复、消息定义演进兼容回放与按时间节奏回放。

## 快速开始

```go
st, report, _ := bag.Open(dir)            // 打开/恢复一个 bag
defer st.Close()
_ = st.RegisterTopic("/pose", poseV1)     // 注册消息定义（可演进）

bus := bag.NewBus()
rec := bag.NewRecorder(bus, st, []string{"/pose"}) // 持续录制
bus.Publish(bag.Message{Topic: "/pose", Timestamp: t, Values: vals})
rec.Stop()

rd, _, _ := bag.OpenReader(dir)
snap, _ := rd.Snapshot()                  // 一致性只读快照
msgs, err := snap.ReadRange(bag.RangeFilter{Start: t1, End: t2},
    map[string]bag.MessageDef{"/pose": poseV2}) // 用新定义回放旧数据

p, _ := bag.NewPlayer(snap, bag.RangeFilter{}, defs,
    bag.PlayerOptions{Speed: 2, Loop: true}) // 2 倍速循环回放
for m := range p.Out() { /* ... */ }
```

错误三分：`bag.ErrNoData`（区间无数据）、`bag.ErrCorrupt`（数据损坏）、
`*bag.IncompatibleError`（定义不兼容，含具体原因与默认字段列表）。

## 文档

- 存储与索引布局、帧格式、恢复扫描：`docs/design.md`
- 定义兼容规则、回放时间语义、验证方法：`docs/design.md`

## 测试

```bash
go test -v ./...
CGO_ENABLED=1 go test -race ./...
```
