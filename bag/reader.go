package bag

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

// ReplayedMessage 是按“当前消息定义”解码后的一条历史消息。
type ReplayedMessage struct {
	Topic     string
	Timestamp time.Time
	Seq       uint64
	DefName   string // 该消息落盘时使用的定义名
	Values    map[string]any
	Defaulted []string // 被填入固定默认值的新增字段路径
}

// Snapshot 是某一时刻的只读一致性视图：
// 索引在创建瞬间冻结，数据通过独立的只读文件句柄按偏移读取，
// 因此 Writer 继续追加、fsync 都不会改变快照内容，也不会被半帧影响。
type Snapshot struct {
	st       *Storage
	indexes  map[string][]indexEntry // 深拷贝的冻结索引
	recovery map[string]TopicRecovery
	files    map[string]*os.File
}

// Snapshot 创建一致性只读快照（可并发调用，与 Write 互不干扰）。
func (s *Storage) Snapshot() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := &Snapshot{
		st:       s,
		indexes:  map[string][]indexEntry{},
		recovery: map[string]TopicRecovery{},
		files:    map[string]*os.File{},
	}
	for topic, idx := range s.indexes {
		cp := make([]indexEntry, len(idx.entries))
		copy(cp, idx.entries)
		snap.indexes[topic] = cp
		snap.recovery[topic] = s.recovery[topic]
	}
	return snap, nil
}

// Close 释放快照持有的独立文件句柄。
func (s *Snapshot) Close() {
	for _, f := range s.files {
		f.Close()
	}
	s.files = nil
}

// Topics 返回快照中的话题。
func (s *Snapshot) Topics() []string {
	names := make([]string, 0, len(s.indexes))
	for t := range s.indexes {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

func (s *Snapshot) openFile(topic string) (*os.File, error) {
	if f, ok := s.files[topic]; ok {
		return f, nil
	}
	f, err := os.Open(s.st.topicDataPath(topic))
	if err != nil {
		return nil, err
	}
	s.files[topic] = f
	return f, nil
}

func (s *Snapshot) readFrame(topic string, e indexEntry) (parsedFrame, error) {
	f, err := s.openFile(topic)
	if err != nil {
		return parsedFrame{}, err
	}
	buf := make([]byte, e.Length)
	if _, err := f.ReadAt(buf, e.Offset); err != nil {
		return parsedFrame{}, err
	}
	frame, _, err := parseOne(buf, 0)
	if err != nil {
		// 快照指向的偏移在创建时已验证；读出来反而坏了属于介质级损坏。
		return parsedFrame{}, fmt.Errorf("%w: topic %q offset %d", ErrCorrupt, topic, e.Offset)
	}
	return frame, nil
}

// RangeFilter 描述一次查询/截取的条件。时间区间为闭区间 [Start, End]；
// Start/End 零值表示不限制该侧。Topics 为空表示全部话题。
type RangeFilter struct {
	Topics []string
	Start  time.Time
	End    time.Time
	// Lenient 为 true 时，即使话题文件存在中间损坏也尽力返回可重同步读出的帧，
	// 调用方需自行通过 Snapshot.Recovery 感知丢帧；默认严格，遇损坏返回 ErrCorrupt。
	Lenient bool
}

func (s *Snapshot) topicNames(f RangeFilter) []string {
	if len(f.Topics) > 0 {
		return f.Topics
	}
	return s.Topics()
}

func inRange(t int64, f RangeFilter) bool {
	if !f.Start.IsZero() && t < f.Start.UnixNano() {
		return false
	}
	if !f.End.IsZero() && t > f.End.UnixNano() {
		return false
	}
	return true
}

// Records 只做索引级查询，返回区间内的帧句柄（不做解码）。
// 区间内本来就没有完整消息时返回 ErrNoData（可用 errors.Is 判断），
// 这与数据损坏是两个不同的错误。
func (s *Snapshot) Records(f RangeFilter) ([]Record, error) {
	topics := s.topicNames(f)
	var out []Record
	for _, topic := range topics {
		if rec, bad := s.recovery[topic]; bad && rec.Corrupt && !f.Lenient {
			return nil, fmt.Errorf("%w: topic %q: %s", ErrCorrupt, topic, rec.CorruptDetail)
		}
		idx, ok := s.indexes[topic]
		if !ok {
			continue // 未知话题等同于无数据
		}
		for _, e := range idx {
			if inRange(e.Timestamp, f) {
				out = append(out, Record{
					Topic: topic, Timestamp: time.Unix(0, e.Timestamp),
					Seq: e.Seq, SchemaID: e.SchemaID, Offset: e.Offset, Length: e.Length,
				})
			}
		}
	}
	if len(out) == 0 {
		// 若查询范围覆盖的话题文件存在中间损坏，严格模式应让调用方知道：
		// “没有数据”可能是坏帧造成的。
		for _, topic := range topics {
			if r, ok := s.recovery[topic]; ok && r.Corrupt {
				return nil, fmt.Errorf("%w: topic %q: %s", ErrCorrupt, topic, r.CorruptDetail)
			}
		}
		return nil, ErrNoData
	}
	// 全局顺序：时间戳升序；同一时间戳按话题、再按写入序号，保证稳定可重放。
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].Timestamp.UnixNano(), out[j].Timestamp.UnixNano()
		if ti != tj {
			return ti < tj
		}
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Seq < out[j].Seq
	})
	return out, nil
}

// LatestDef 返回某话题当前（最新注册）的消息定义。
func (s *Storage) LatestDef(topic string) (MessageDef, error) {
	s.mu.Lock()
	ids := append([]string(nil), s.manifest.Topics[topic]...)
	s.mu.Unlock()
	if len(ids) == 0 {
		return MessageDef{}, fmt.Errorf("bag: unknown topic %q", topic)
	}
	return s.schemas.load(ids[len(ids)-1])
}

func (s *Snapshot) def(id string) (MessageDef, error) {
	return s.st.schemas.load(id)
}

// ReadRange 用 defs 中提供的“当前定义”解码区间内的全部消息。
//   - 未在 defs 中给出定义的话题，使用其落盘时的原始定义（等价于无演进）；
//   - 新增字段填固定零值，Defaulted 中列出字段路径；
//   - 删除/改名/类型不符返回 *IncompatibleError（errors.As 可取具体原因）；
//   - 空区间返回 ErrNoData；介质/文件损坏返回 ErrCorrupt。
func (s *Snapshot) ReadRange(f RangeFilter, defs map[string]MessageDef) ([]ReplayedMessage, error) {
	recs, err := s.Records(f)
	if err != nil {
		return nil, err
	}
	out := make([]ReplayedMessage, 0, len(recs))
	for _, rec := range recs {
		frame, err := s.readFrame(rec.Topic, indexEntry{
			Timestamp: rec.Timestamp.UnixNano(), Seq: rec.Seq,
			SchemaID: rec.SchemaID, Offset: rec.Offset, Length: rec.Length,
		})
		if err != nil {
			return nil, err
		}
		recordedDef, err := s.def(rec.SchemaID)
		if err != nil {
			return nil, err
		}
		target := recordedDef
		if d, ok := defs[rec.Topic]; ok {
			if err := d.Validate(); err != nil {
				return nil, err
			}
			target = d
		}
		dec, err := decodeFields(frame.payload, target.Fields, rec.Topic)
		if err != nil {
			return nil, err
		}
		out = append(out, ReplayedMessage{
			Topic:     rec.Topic,
			Timestamp: rec.Timestamp,
			Seq:       rec.Seq,
			DefName:   recordedDef.Name,
			Values:    dec.Values,
			Defaulted: dec.Defaulted,
		})
	}
	return out, nil
}

// ReadOne 解码单条帧句柄。
func (s *Snapshot) ReadOne(rec Record, target MessageDef) (ReplayedMessage, error) {
	if err := target.Validate(); err != nil {
		return ReplayedMessage{}, err
	}
	e := indexEntry{Timestamp: rec.Timestamp.UnixNano(), Seq: rec.Seq,
		SchemaID: rec.SchemaID, Offset: rec.Offset, Length: rec.Length}
	frame, err := s.readFrame(rec.Topic, e)
	if err != nil {
		return ReplayedMessage{}, err
	}
	recordedDef, err := s.def(rec.SchemaID)
	if err != nil {
		return ReplayedMessage{}, err
	}
	dec, err := decodeFields(frame.payload, target.Fields, rec.Topic)
	if err != nil {
		return ReplayedMessage{}, err
	}
	return ReplayedMessage{
		Topic: rec.Topic, Timestamp: rec.Timestamp, Seq: rec.Seq,
		DefName: recordedDef.Name, Values: dec.Values, Defaulted: dec.Defaulted,
	}, nil
}

// Recovery 返回快照中某话题的打开时恢复结论。
func (s *Snapshot) Recovery(topic string) (TopicRecovery, bool) {
	r, ok := s.recovery[topic]
	return r, ok
}

// IsNoData / IsCorrupt / AsIncompatible 便于调用方区分三类结果。
func IsNoData(err error) bool  { return errors.Is(err, ErrNoData) }
func IsCorrupt(err error) bool { return errors.Is(err, ErrCorrupt) }
func AsIncompatible(err error) (*IncompatibleError, bool) {
	var ie *IncompatibleError
	if errors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}
