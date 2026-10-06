package player

import (
	"sync"
	"time"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/bag"
	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

// Event 是回放吐出的一条消息。
type Event struct {
	Topic string
	Stamp int64 // 录制时的原始时间戳
	Seq   uint64
	Data  map[string]any
	Notes []msgdef.FieldNote // 旧数据按新定义补齐字段的说明

	// Index 是本次回放计划内的位置（从 0 起），便于断言“无重复、无漏发”。
	Index int
	// Loop 是该消息所属的循环轮次（从 0 起）。
	Loop int
}

// Options 控制一次回放。
type Options struct {
	Topics []string // 空表示全部话题
	Span   bag.Span // 时间区间裁剪
	Rate   float64  // 变速：1.0 原始节奏，0 表示尽快（不睡眠）
	Loop   bool     // 是否循环
	// Schemas 给每个话题提供“当前定义”。缺省时使用录制时定义。
	Schemas map[string]msgdef.Schema
}

// Player 对 bag 快照做可暂停、可跳转、可变速的回放。
type Player struct {
	reader *bag.Reader
	opts   Options

	mu       sync.Mutex
	plan     []bag.Entry
	paused   bool
	pos      int // 下一条待发消息在 plan 中的下标
	loopCnt  int
	out      chan Event
	stop     chan struct{}
	wake     chan struct{}
	done     chan struct{}
	finished bool
	err      error

	// nowClock 抽象时间源，便于测试精确控制。
	startedAt time.Time
	// baseStamp/baseWall 用于把 stamp 映射到墙上时间。
	baseStamp int64
	baseWall  time.Time
	pauseWall time.Time
	gen       int // 跳转/变速/暂停恢复都会自增，用于丢弃旧等待
}

// New 基于读者创建播放器并立即构建一次一致性快照计划。
func New(reader *bag.Reader, opts Options) (*Player, error) {
	if opts.Rate == 0 {
		opts.Rate = 1
	}
	entries, err := reader.Query(opts.Topics, opts.Span)
	if err != nil {
		return nil, err
	}
	p := &Player{
		reader: reader,
		opts:   opts,
		plan:   entries,
		out:    make(chan Event),
		stop:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	return p, nil
}

// Length 返回本次回放计划的消息数（循环时每轮都是它）。
func (p *Player) Length() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.plan)
}

// IsEmpty 表示区间内本来就没有数据（与损坏报错区分：New 出错表示损坏）。
func (p *Player) IsEmpty() bool { return len(p.plan) == 0 }

// Events 返回回放事件通道。
func (p *Player) Events() <-chan Event { return p.out }

// Start 启动回放协程。
func (p *Player) Start() {
	p.mu.Lock()
	p.baseWall = time.Now()
	p.baseStamp = p.firstStampLocked()
	p.mu.Unlock()
	go p.run()
}

func (p *Player) firstStampLocked() int64 {
	if len(p.plan) == 0 {
		return 0
	}
	return p.plan[0].Stamp
}

func (p *Player) run() {
	defer close(p.done)
	defer close(p.out)
	instant := p.opts.Rate < 0
	for {
		p.mu.Lock()
		gen := p.gen
		pos := p.pos
		n := len(p.plan)
		p.mu.Unlock()

		if n == 0 {
			if !p.opts.Loop {
				return
			}
			// 空计划循环没有意义，直接结束。
			return
		}
		if pos >= n {
			if !p.opts.Loop {
				return
			}
			p.mu.Lock()
			if p.gen != gen || p.pos != pos {
				p.mu.Unlock()
				continue
			}
			p.loopCnt++
			p.pos = 0
			p.baseStamp = p.plan[0].Stamp
			p.baseWall = time.Now()
			p.gen++
			p.mu.Unlock()
			continue
		}

		// 取本条计划消息，同时处理暂停与节奏等待；任一步被打断都重新取状态。
		entry, loop, proceed := p.prepareNext(gen, pos, n, instant)
		if !proceed {
			select {
			case <-p.stop:
				return
			default:
				continue
			}
		}

		// 发送前在锁内完成“读取-占位”，保证任何跳转都不会让一条消息重发。
		p.mu.Lock()
		if p.gen != gen || p.pos != pos || p.loopCnt != loop || p.paused {
			p.mu.Unlock()
			continue
		}
		ev, err := p.decodeLocked(entry, pos, loop)
		if err != nil {
			p.err = err
			p.mu.Unlock()
			return
		}
		p.pos = pos + 1
		p.gen++ // 已发送后旧的等待全部失效
		p.mu.Unlock()

		select {
		case p.out <- ev:
		case <-p.stop:
			return
		}
	}
}

// prepareNext 处理暂停与时间等待。返回 proceed=false 表示状态被改变，需要重试。
func (p *Player) prepareNext(gen, pos, n int, instant bool) (bag.Entry, int, bool) {
	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		// 等待恢复信号或停止；不推进时间。
		select {
		case <-p.stop:
			return bag.Entry{}, 0, false
		case <-p.wake:
		}
		return bag.Entry{}, 0, false
	}
	entry := p.plan[pos]
	loop := p.loopCnt
	rate := p.opts.Rate
	baseStamp, baseWall := p.baseStamp, p.baseWall
	p.mu.Unlock()

	if !instant && rate > 0 {
		delayNs := float64(entry.Stamp-baseStamp) / rate
		due := baseWall.Add(time.Duration(delayNs))
		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-p.stop:
				timer.Stop()
				return bag.Entry{}, 0, false
			case <-p.wake:
				timer.Stop()
				return bag.Entry{}, 0, false
			case <-timer.C:
			}
		}
	}
	return entry, loop, true
}

func (p *Player) decodeLocked(e bag.Entry, pos, loop int) (Event, error) {
	rs, ok := p.opts.Schemas[e.Topic]
	var (
		data  map[string]any
		notes []msgdef.FieldNote
		err   error
	)
	if ok {
		data, notes, err = p.reader.Decode(e, rs)
	} else {
		var payload []byte
		payload, err = p.reader.ReadPayload(e)
		if err == nil {
			data, notes, err = msgdef.Decode(e.Schema, e.Schema, payload)
		}
	}
	if err != nil {
		return Event{}, err
	}
	return Event{Topic: e.Topic, Stamp: e.Stamp, Seq: e.Seq, Data: data,
		Notes: notes, Index: pos, Loop: loop}, nil
}

// Pause 暂停回放；暂停期间不发消息，恢复后不补发、不重复。
func (p *Player) Pause() {
	p.mu.Lock()
	p.paused = true
	p.pauseWall = time.Now()
	p.gen++
	p.mu.Unlock()
}

// Resume 恢复，时间基准平移暂停时长，保证原始时间间隔语义不被暂停压缩。
func (p *Player) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.baseWall = p.baseWall.Add(time.Since(p.pauseWall))
	p.paused = false
	p.gen++
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// SetRate 运行中变速；从当前时间点重新锚定时间基准。
func (p *Player) SetRate(rate float64) {
	if rate == 0 {
		rate = 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.plan) == 0 || p.pos >= len(p.plan) {
		p.opts.Rate = rate
		return
	}
	p.baseStamp = p.plan[p.pos].Stamp
	p.baseWall = time.Now()
	p.opts.Rate = rate
	p.gen++
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// SeekTo 跳转到计划内绝对位置（0..Length）。已发过的消息不会重发，
// 目标位置之前的消息也不会补发；时间基准重新锚定到目标消息。
func (p *Player) SeekTo(pos int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pos < 0 || pos > len(p.plan) {
		return errOutOfRange(pos, len(p.plan))
	}
	p.pos = pos
	p.loopCnt = 0
	p.gen++
	if pos < len(p.plan) {
		p.baseStamp = p.plan[pos].Stamp
		p.baseWall = time.Now()
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

// SeekStamp 跳到第一个 stamp >= t 的位置（裁剪区间内）。
func (p *Player) SeekStamp(t int64) error {
	p.mu.Lock()
	n := len(p.plan)
	p.mu.Unlock()
	idx := n
	for i, e := range p.planSnapshot() {
		if e.Stamp >= t {
			idx = i
			break
		}
	}
	return p.SeekTo(idx)
}

// Position 返回当前位置与循环轮次。
func (p *Player) Position() (pos, length, loop int, paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pos, len(p.plan), p.loopCnt, p.paused
}

// Err 返回回放中发生的解码错误（如 schema 不兼容）。
func (p *Player) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Close 停止回放并等待协程退出。
func (p *Player) Close() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}

func (p *Player) planSnapshot() []bag.Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bag.Entry(nil), p.plan...)
}
