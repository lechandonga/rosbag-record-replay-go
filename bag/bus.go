package bag

import "sync"

// Bus 是不依赖 ROS 的本地发布/订阅模拟：发布按顺序派发给该话题的全部订阅者。
// 派发持锁时间仅为一次非阻塞 channel 发送；订阅者 channel 满时按丢消息处理
// （模拟实时系统的队列溢出），并由订阅者自行决定策略。
type Bus struct {
	mu   sync.Mutex
	subs map[string][]chan Message
}

// NewBus 创建本地话题总线。
func NewBus() *Bus {
	return &Bus{subs: map[string][]chan Message{}}
}

// Subscribe 订阅若干话题，返回消息通道。buffer 为 channel 缓冲长度。
func (b *Bus) Subscribe(buffer int, topics ...string) <-chan Message {
	ch := make(chan Message, buffer)
	b.mu.Lock()
	for _, t := range topics {
		b.subs[t] = append(b.subs[t], ch)
	}
	b.mu.Unlock()
	return ch
}

// Publish 向某话题发布一条消息。
func (b *Bus) Publish(m Message) {
	b.mu.Lock()
	subs := append([]chan Message(nil), b.subs[m.Topic]...)
	b.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- m:
		default:
			// 订阅者跟不上实时流：丢弃该条（录制器应给足缓冲避免丢消息）。
		}
	}
}

// Recorder 把总线上的话题持续录制到 Storage。
type Recorder struct {
	store *Storage
	in    <-chan Message
	stop  chan struct{}
	done  chan struct{}
	count int64
}

// NewRecorder 订阅 topics 并开始后台持续录制。
func NewRecorder(bus *Bus, store *Storage, topics []string) *Recorder {
	r := &Recorder{
		store: store,
		in:    bus.Subscribe(4096, topics...),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go r.loop()
	return r
}

func (r *Recorder) loop() {
	defer close(r.done)
	for {
		select {
		case m, ok := <-r.in:
			if !ok {
				return
			}
			_ = r.store.Write(m)
		case <-r.stop:
			// 停止前先排空 channel，避免最后几条已发布消息丢失。
			for {
				select {
				case m := <-r.in:
					_ = r.store.Write(m)
				default:
					return
				}
			}
		}
	}
}

// Stop 停止录制（不关闭 Storage）。
func (r *Recorder) Stop() { close(r.stop); <-r.done }
