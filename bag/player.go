package bag

import (
	"sort"
	"sync"
	"time"
)

// Player 按消息原始时间戳节奏回放一份冻结快照。
//
// 时间语义：
//   - 消息按 (timestamp, topic, seq) 排序，同一时间戳的消息间隔为 0，
//     仍严格按顺序相邻发出，保证不漏、不重、不倒序；
//   - 相邻消息墙钟等待 = (t2 - t1) / Speed（首条立即发出）；
//   - 暂停只暂停墙钟：恢复后把暂停时长补回锚点，消息节奏不被压缩或拉伸；
//   - 跳转：游标移动到“时间戳 >= t 的第一条”，并以该条重置时间锚点，
//     因此跳转点之前的消息不会补播（不重）、之后的消息继续按节奏发（不漏）；
//   - 循环：发完最后一条后游标回到 0 并在当前墙钟重新锚定，不补跨轮回放间隔。
type Player struct {
	out  chan ReplayedMessage
	cmd  chan command
	done chan struct{}
	mu   sync.Mutex

	msgs  []ReplayedMessage
	loop  bool
	speed float64

	state  playerState
	cursor int
	// 时间锚点：anchorWall 墙钟时刻对应消息时间 anchorTs。
	anchorWall time.Time
	anchorTs   int64
	pausedAt   time.Time
	endHandled bool
}

type playerState int

const (
	statePlaying playerState = iota
	statePaused
	stateFinished
)

type commandKind int

const (
	cmdPause commandKind = iota
	cmdResume
	cmdSeek
	cmdStop
)

type command struct {
	kind commandKind
	at   time.Time // cmdSeek：跳转到时间戳 >= at 的第一条
	idx  int       // cmdSeek：at 零值时按索引跳转
}

// PlayerOptions 控制回放行为。
type PlayerOptions struct {
	Speed float64 // 1.0 原速；2.0 两倍速；0.5 半速
	Loop  bool    // 是否循环
}

// NewPlayer 从快照的 filter 区间构造回放器；消息列表在构造时一次性冻结。
// defs 为各话题“当前定义”，与 ReadRange 使用同一套兼容规则。
func NewPlayer(snap *Snapshot, f RangeFilter, defs map[string]MessageDef, opt PlayerOptions) (*Player, error) {
	if opt.Speed <= 0 {
		opt.Speed = 1
	}
	msgs, err := snap.ReadRange(f, defs)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		ti, tj := msgs[i].Timestamp.UnixNano(), msgs[j].Timestamp.UnixNano()
		if ti != tj {
			return ti < tj
		}
		if msgs[i].Topic != msgs[j].Topic {
			return msgs[i].Topic < msgs[j].Topic
		}
		return msgs[i].Seq < msgs[j].Seq
	})
	p := &Player{
		out:   make(chan ReplayedMessage, 64),
		cmd:   make(chan command, 8),
		done:  make(chan struct{}),
		msgs:  msgs,
		loop:  opt.Loop,
		speed: opt.Speed,
	}
	p.resetAnchorLocked(time.Now(), 0)
	go p.run()
	return p, nil
}

// Out 返回消息输出通道；播放器结束后通道关闭。
func (p *Player) Out() <-chan ReplayedMessage { return p.out }

// Pause 暂停回放；暂停期间不发出消息，时间锚点冻结。
func (p *Player) Pause() { p.cmd <- command{kind: cmdPause} }

// Resume 恢复回放；暂停时长不计入节奏。
func (p *Player) Resume() { p.cmd <- command{kind: cmdResume} }

// SeekTo 跳转到时间戳 >= t 的第一条消息（在回放列表内）。
func (p *Player) SeekTo(t time.Time) { p.cmd <- command{kind: cmdSeek, at: t} }

// SeekIndex 跳转到回放列表中的第 idx 条（越界钳制到末尾）。
func (p *Player) SeekIndex(idx int) { p.cmd <- command{kind: cmdSeek, idx: idx} }

// Stop 终止回放并关闭输出通道。
func (p *Player) Stop() {
	p.cmd <- command{kind: cmdStop}
	<-p.done
}

// Position 返回当前游标（下一条待发消息的索引）与其时间戳；全部发完时索引为 len。
func (p *Player) Position() (int, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.positionSnapshotLocked()
}

// Paused 报告播放器当前是否处于暂停状态。
func (p *Player) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state == statePaused
}

// Finished 报告非循环播放是否已发完。
func (p *Player) Finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state == stateFinished
}

func (p *Player) positionSnapshotLocked() (int, time.Time) {
	if len(p.msgs) == 0 {
		return 0, time.Time{}
	}
	idx := p.cursor
	if idx >= len(p.msgs) {
		idx = len(p.msgs) - 1
	}
	return p.cursor, p.msgs[idx].Timestamp
}

func (p *Player) resetAnchorLocked(wall time.Time, idx int) {
	p.anchorWall = wall
	if idx < len(p.msgs) {
		p.anchorTs = p.msgs[idx].Timestamp.UnixNano()
	}
}

func (p *Player) run() {
	defer close(p.done)
	defer close(p.out)
	for {
		switch p.state {
		case statePaused:
			cmd, ok := p.recvCmd()
			if !ok {
				return
			}
			if !p.handle(cmd) {
				return
			}
		case stateFinished:
			// 非循环播放结束：让输出缓冲排空后退出并关闭通道。
			if len(p.out) > 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			return
		case statePlaying:
			p.mu.Lock()
			if p.cursor >= len(p.msgs) {
				if p.loop {
					p.cursor = 0
					p.resetAnchorLocked(time.Now(), 0)
					p.mu.Unlock()
					continue
				}
				p.state = stateFinished
				p.mu.Unlock()
				continue
			}
			now := time.Now()
			ts := p.msgs[p.cursor].Timestamp.UnixNano()
			deltaNs := float64(ts-p.anchorTs) / p.speed
			target := p.anchorWall.Add(time.Duration(deltaNs))
			p.mu.Unlock()
			wait := target.Sub(now)
			if wait <= 0 {
				// 立即发送，同时保留对控制指令的响应，避免变速极快时无法暂停/停止。
				select {
				case cmd, ok := <-p.cmd:
					if !ok || !p.handle(cmd) {
						return
					}
				case p.out <- p.msgs[p.cursor]:
					p.mu.Lock()
					p.cursor++
					p.mu.Unlock()
				}
				continue
			}
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
				p.out <- p.msgs[p.cursor]
				p.mu.Lock()
				p.cursor++
				p.mu.Unlock()
			case cmd, ok := <-p.cmd:
				if !timer.Stop() {
					<-timer.C
				}
				if !ok {
					return
				}
				if !p.handle(cmd) {
					return
				}
			}
		}
	}
}

func (p *Player) recvCmd() (command, bool) {
	cmd, ok := <-p.cmd
	return cmd, ok
}

func (p *Player) handle(cmd command) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch cmd.kind {
	case cmdPause:
		if p.state == statePlaying {
			p.state = statePaused
			p.pausedAt = time.Now()
		}
	case cmdResume:
		if p.state == statePaused {
			// 把暂停时长补进锚点：恢复后下一条仍按原节奏发出。
			pausedFor := time.Since(p.pausedAt)
			p.anchorWall = p.anchorWall.Add(pausedFor)
			p.state = statePlaying
		}
	case cmdSeek:
		idx := cmd.idx
		if !cmd.at.IsZero() {
			idx = sort.Search(len(p.msgs), func(i int) bool {
				return !p.msgs[i].Timestamp.Before(cmd.at)
			})
		}
		if idx < 0 {
			idx = 0
		}
		if idx > len(p.msgs) {
			idx = len(p.msgs)
		}
		p.cursor = idx
		p.endHandled = false
		// 无论暂停与否都重锚；暂停中跳转只移动游标，恢复时从新位置计时。
		p.resetAnchorLocked(time.Now(), idx)
		if p.state == stateFinished {
			p.state = statePlaying
		}
	case cmdStop:
		return false
	}
	return true
}
