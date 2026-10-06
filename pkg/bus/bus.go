package bus

import (
	"sync"
)

// Message 是本地总线上传输的一条话题消息。
// Stamp 由发布方给出，表示消息产生的原始时间（纳秒，单调时钟基准无关）。
type Message struct {
	Topic string
	Stamp int64
	Data  map[string]any
}

// Subscription 是一个本地订阅句柄。
type Subscription struct {
	Topic  string
	C      <-chan Message
	ch     chan Message
	closed bool
	unsub  func()
}

// Close 取消订阅。
func (s *Subscription) Close() { s.unsub() }

// Bus 是进程内的发布订阅模拟，不依赖任何 ROS 环境。
type Bus struct {
	mu     sync.RWMutex
	subs   map[string][]*Subscription
	buffer int
}

// New 创建总线；buffer 为每个订阅者通道的缓冲大小。
func New(buffer int) *Bus {
	if buffer < 1 {
		buffer = 1
	}
	return &Bus{subs: map[string][]*Subscription{}, buffer: buffer}
}

// Subscribe 订阅指定话题。
func (b *Bus) Subscribe(topic string) *Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Message, b.buffer)
	s := &Subscription{Topic: topic, C: ch, ch: ch}
	s.unsub = func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		list := b.subs[topic]
		for i, cur := range list {
			if cur == s {
				b.subs[topic] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if !s.closed {
			s.closed = true
			close(ch)
		}
	}
	b.subs[topic] = append(b.subs[topic], s)
	return s
}

// Publish 向话题发布消息。慢订阅者会在缓冲耗尽后丢弃最旧消息，
// 但返回丢弃数以便上层观测；录制器应使用足够缓冲并在测试中控制节奏。
func (b *Bus) Publish(msg Message) int {
	b.mu.RLock()
	subs := append([]*Subscription(nil), b.subs[msg.Topic]...)
	b.mu.RUnlock()
	dropped := 0
	for _, s := range subs {
		for {
			select {
			case s.ch <- msg:
				goto delivered
			default:
			}
			select {
			case <-s.ch:
				dropped++
			default:
			}
		}
	delivered:
	}
	return dropped
}

// SubscriberCount 返回某话题当前订阅者数，便于测试断言。
func (b *Bus) SubscriberCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}
