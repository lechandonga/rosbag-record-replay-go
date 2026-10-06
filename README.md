# rosbag-record-replay-go

不依赖真实 ROS 环境的本地话题录制 / 回放组件：用进程内发布订阅模拟话题通信，
用本地只追加文件保存录制数据，提供崩溃可恢复、定义演进可预期、回放可控的保证。

## 包结构

- `pkg/bus`：本地发布订阅；
- `pkg/msgdef`：消息定义、按字段名编解码、定义演进兼容规则；
- `pkg/bag`：段文件存储、校验、崩溃恢复、话题/时间区间查询与索引；
- `pkg/recorder`：订阅总线并持续录制；
- `pkg/player`：按原始节奏回放，支持变速、区间、循环、暂停/恢复/跳转。

## 快速上手

```go
type Msg = map[string]any

b := bus.New(128)
pose := msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{
    {Name: "frame", Type: msgdef.String},
    {Name: "x", Type: msgdef.Float64},
}}

// 录制
rec, _ := recorder.New(b, "./runtime/bag1", 64*1024*1024)
rec.AddTopic("/pose", pose)
rec.Start()
b.Publish(bus.Message{Topic: "/pose", Stamp: nowNs(), Data: Msg{"frame": "map", "x": 1.0}})
// ... 长时间运行；异常退出后重新 OpenWriter 会自动丢弃半截帧后续写
rec.Close()

// 查询
r, _ := bag.OpenReader("./runtime/bag1")
entries, err := r.Query([]string{"/pose"}, bag.Span{Start: t1, End: t2})
// err == nil 且 len==0 表示区间本来就没有数据；
// errors.Is(err, bag.ErrCorruption) 表示数据损坏；
// errors.Is(err, bag.ErrNoBag)      表示目录从未成功录制。

// 回放（2 倍速 + 循环）
p, _ := player.New(r, player.Options{Rate: 2, Loop: true})
p.Start()
for ev := range p.Events() {
    // ev.Stamp 原始时间戳；ev.Notes 记录旧数据被默认值补齐的字段
    _ = ev
}
```

定义演进（新增字段的回放）：

```go
next := msgdef.Schema{Name: "Pose", Fields: []msgdef.Field{
    {Name: "frame", Type: msgdef.String},
    {Name: "x", Type: msgdef.Float64},
    {Name: "conf", Type: msgdef.Float64, Default: "0.95"}, // 旧数据补 0.95
}}
p, _ := player.New(r, player.Options{Rate: 1, Schemas: map[string]msgdef.Schema{"/pose": next}})
```

删除字段、改名、改类型会返回 `*msgdef.IncompatibleError`，不会按位置猜测。

## 文档

- [存储与索引布局](docs/storage.md)
- [消息定义兼容规则](docs/schema-compatibility.md)
- [回放时间语义与验证方法](docs/playback.md)

## 测试

```bash
go test ./... -count=1 -v
CGO_ENABLED=1 go test ./... -race
```
