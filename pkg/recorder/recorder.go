package recorder

import (
	"fmt"
	"sync"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/bag"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/bus"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

// Recorder 订阅若干话题并把消息持续写入一个 bag。
type Recorder struct {
	bus    *bus.Bus
	writer *bag.Writer

	mu      sync.Mutex
	subs    map[string]*bus.Subscription
	schemas map[string]topicSchema
	stop    chan struct{}
	wg      sync.WaitGroup
	closed  bool
	errors  []error
}

type topicSchema struct {
	typeName string
	schema   msgdef.Schema
}

// New 创建录制器；dir 为 bag 目录，maxSegmentBytes 控制段滚动。
func New(b *bus.Bus, dir string, maxSegmentBytes int64) (*Recorder, error) {
	w, err := bag.OpenWriter(dir, maxSegmentBytes)
	if err != nil {
		return nil, err
	}
	return &Recorder{bus: b, writer: w, subs: map[string]*bus.Subscription{}, schemas: map[string]topicSchema{}}, nil
}

// AddTopic 登记并订阅一个话题。必须在 Start 前或运行中调用（运行中同样生效）。
func (r *Recorder) AddTopic(topic string, schema msgdef.Schema) error {
	typeName := schema.Name
	if err := r.writer.RegisterTopic(topic, typeName, schema); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[topic] = topicSchema{typeName: typeName, schema: schema}
	if r.stop != nil {
		r.subscribeLocked(topic)
	}
	return nil
}

// Start 开始从总线消费并落盘。
func (r *Recorder) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stop != nil {
		return
	}
	r.stop = make(chan struct{})
	for topic := range r.schemas {
		r.subscribeLocked(topic)
	}
}

func (r *Recorder) subscribeLocked(topic string) {
	if _, ok := r.subs[topic]; ok {
		return
	}
	sub := r.bus.Subscribe(topic)
	r.subs[topic] = sub
	r.wg.Add(1)
	go r.loop(sub)
}

func (r *Recorder) loop(sub *bus.Subscription) {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			if err := r.handle(msg); err != nil {
				r.mu.Lock()
				r.errors = append(r.errors, err)
				r.mu.Unlock()
			}
		}
	}
}

func (r *Recorder) handle(msg bus.Message) error {
	r.mu.Lock()
	ts, ok := r.schemas[msg.Topic]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("recorder: no schema for topic %q", msg.Topic)
	}
	payload, err := msgdef.Encode(ts.schema, msg.Data)
	if err != nil {
		return fmt.Errorf("recorder topic %q: %w", msg.Topic, err)
	}
	return r.writer.Write(msg.Topic, msg.Stamp, payload)
}

// RecordNow 允许测试或非总线来源直接写一条消息（同样经过编码校验）。
func (r *Recorder) RecordNow(topic string, stamp int64, data map[string]any) error {
	r.mu.Lock()
	ts, ok := r.schemas[topic]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("recorder: no schema for topic %q", topic)
	}
	payload, err := msgdef.Encode(ts.schema, data)
	if err != nil {
		return err
	}
	return r.writer.Write(topic, stamp, payload)
}

// Errors 返回录制过程中出现的编码/写入错误快照。
func (r *Recorder) Errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]error(nil), r.errors...)
	return out
}

// Close 停止消费、等待在途消息写完、关闭写者（崩溃模拟场景可直接杀进程，不走 Close）。
func (r *Recorder) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	if r.stop != nil {
		close(r.stop)
	}
	subs := make([]*bus.Subscription, 0, len(r.subs))
	for _, s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.Unlock()
	r.wg.Wait()
	for _, s := range subs {
		s.Close()
	}
	return r.writer.Close()
}
