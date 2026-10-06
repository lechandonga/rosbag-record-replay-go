package bag

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Storage 是一个本地 bag（目录）。同一进程中可同时存在多个 Reader 快照，
// Reader 各自持有独立文件句柄，Writer 的追加与 fsync 不影响已有快照。
type Storage struct {
	dir      string
	readOnly bool
	mu       sync.Mutex // 保护 manifest、索引、文件句柄、seq
	manifest *manifest
	schemas  *schemaStore
	dataDir  string
	indexes  map[string]*topicIndex
	recovery map[string]TopicRecovery
	files    map[string]*os.File // 打开的追加句柄
	nextSeq  map[string]uint64
}

// RecoveryReport 汇总打开 bag 时每个 topic 的恢复结论。
type RecoveryReport struct {
	Topics []TopicRecovery
}

// Healthy 报告是否所有 topic 都没有中间损坏（崩溃截断不算损坏）。
func (r RecoveryReport) Healthy() bool {
	for _, t := range r.Topics {
		if t.Corrupt {
			return false
		}
	}
	return true
}

func (r RecoveryReport) String() string {
	s := ""
	for i, t := range r.Topics {
		if i > 0 {
			s += "; "
		}
		s += t.describe()
	}
	return s
}

// Open 以读写方式打开或创建 bag，并执行崩溃恢复：
// 完整帧进入索引，尾部半帧被截断丢弃（TruncatedTail），中间坏帧被隔离并报告。
func Open(dir string) (*Storage, RecoveryReport, error) {
	return open(dir, false)
}

// OpenReader 以只读方式打开 bag，不改动任何文件。
func OpenReader(dir string) (*Storage, RecoveryReport, error) {
	return open(dir, true)
}

func open(dir string, readOnly bool) (*Storage, RecoveryReport, error) {
	if err := os.MkdirAll(filepath.Join(dir, "schemas"), 0o755); err != nil {
		return nil, RecoveryReport{}, err
	}
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, RecoveryReport{}, err
	}
	m, err := loadManifest(dir)
	if err != nil {
		return nil, RecoveryReport{}, err
	}
	st := &Storage{
		dir:      dir,
		readOnly: readOnly,
		manifest: m,
		schemas:  &schemaStore{dir: filepath.Join(dir, "schemas")},
		dataDir:  dataDir,
		indexes:  map[string]*topicIndex{},
		recovery: map[string]TopicRecovery{},
		files:    map[string]*os.File{},
		nextSeq:  map[string]uint64{},
	}
	report, err := st.scanAll()
	if err != nil {
		return nil, RecoveryReport{}, err
	}
	if !readOnly {
		// 持久化恢复后的 idx 缓存，供工具检查；不参与正确性。
		for topic := range st.indexes {
			if err := st.persistIndex(topic); err != nil {
				return nil, RecoveryReport{}, err
			}
		}
	}
	return st, report, nil
}

// topicFileName 把任意话题名（允许以 "/" 开头、包含 "/" 层级）编码成安全的
// 单层数据文件名，避免路径穿越和 "/a" 与 "a" 冲突。编码可逆，规则：
// 字母数字 . _ - 原样保留，其余字节（含 "/"）转成 %xx。
func topicFileName(topic string) string {
	var b []byte
	for i := 0; i < len(topic); i++ {
		ch := topic[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '.', ch == '_', ch == '-':
			b = append(b, ch)
		default:
			b = append(b, '%', hexDigits[ch>>4], hexDigits[ch&0xF])
		}
	}
	return string(b)
}

const hexDigits = "0123456789abcdef"

func (s *Storage) topicDataPath(topic string) string {
	return filepath.Join(s.dataDir, topicFileName(topic)+".bag")
}

func (s *Storage) scanAll() (RecoveryReport, error) {
	report := RecoveryReport{}
	topics := map[string]bool{}
	for t := range s.manifest.Topics {
		topics[t] = true
	}
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return RecoveryReport{}, err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bag" {
			name, err := decodeTopicFileName(e.Name()[:len(e.Name())-4])
			if err == nil {
				topics[name] = true
			}
		}
	}
	names := make([]string, 0, len(topics))
	for t := range topics {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, topic := range names {
		rec, err := s.scanTopic(topic)
		if err != nil {
			return RecoveryReport{}, err
		}
		report.Topics = append(report.Topics, rec)
	}
	return report, nil
}

func (s *Storage) scanTopic(topic string) (TopicRecovery, error) {
	raw, err := os.ReadFile(s.topicDataPath(topic))
	if os.IsNotExist(err) {
		s.indexes[topic] = &topicIndex{}
		rec := TopicRecovery{Topic: topic}
		s.recovery[topic] = rec
		return rec, nil
	}
	if err != nil {
		return TopicRecovery{}, err
	}
	frames, rec := scanFrames(raw, topic)
	idx := &topicIndex{entries: make([]indexEntry, 0, len(frames))}
	var maxSeq uint64
	for _, f := range frames {
		idx.entries = append(idx.entries, indexEntry{
			Timestamp: f.ts.UnixNano(),
			Seq:       f.seq,
			SchemaID:  f.schemaID,
			Offset:    f.start,
			Length:    f.length,
		})
		if f.seq >= maxSeq {
			maxSeq = f.seq
		}
	}
	if len(frames) > 0 {
		s.nextSeq[topic] = maxSeq + 1
	}
	s.indexes[topic] = idx
	s.recovery[topic] = rec

	// 恢复写：把崩溃残留的尾部半帧物理截断，保证后续追加落在完整帧边界之后。
	if !s.readOnly && rec.TruncatedTail > 0 {
		validLen := int64(0)
		if n := len(frames); n > 0 {
			last := frames[n-1]
			validLen = last.start + int64(last.length)
		}
		if err := os.Truncate(s.topicDataPath(topic), validLen); err != nil {
			return TopicRecovery{}, err
		}
	}
	return rec, nil
}

// RegisterTopic 声明（或追加演进）一个 topic 的消息定义。
// 同一 topic 允许注册内容不同的新定义，形成定义演进历史；写入时使用最新定义。
func (s *Storage) RegisterTopic(topic string, def MessageDef) error {
	if s.readOnly {
		return errors.New("bag: storage opened read-only")
	}
	if topic == "" {
		return errors.New("bag: empty topic name")
	}
	if err := def.Validate(); err != nil {
		return err
	}
	id, err := s.schemas.save(def)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.manifest.Topics[topic] {
		if existing == id {
			return nil
		}
	}
	s.manifest.Topics[topic] = append(s.manifest.Topics[topic], id)
	if err := atomicWriteJSON(filepath.Join(s.dir, "manifest.json"), s.manifest); err != nil {
		return err
	}
	if _, ok := s.indexes[topic]; !ok {
		s.indexes[topic] = &topicIndex{}
		s.recovery[topic] = TopicRecovery{Topic: topic}
	}
	return nil
}

// Write 按最新注册的定义编码并追加一条消息，落盘（fsync）后才返回。
func (s *Storage) Write(msg Message) error {
	if s.readOnly {
		return errors.New("bag: storage opened read-only")
	}
	s.mu.Lock()
	ids := s.manifest.Topics[msg.Topic]
	if len(ids) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("bag: topic %q is not registered", msg.Topic)
	}
	schemaID := ids[len(ids)-1]
	def, err := s.schemas.load(schemaID)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	payload, err := encodeFields(def.Fields, msg.Values)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	seq := s.nextSeq[msg.Topic]
	s.nextSeq[msg.Topic] = seq + 1
	frame := encodeFrame(msg.Timestamp, seq, schemaID, payload)

	f, ok := s.files[msg.Topic]
	if !ok {
		f, err = os.OpenFile(s.topicDataPath(msg.Topic), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.files[msg.Topic] = f
	}
	s.mu.Unlock()

	// 一次 Write 写整帧（O_APPEND 原子定位），再 fsync；崩溃时要么整帧在、要么不在。
	if _, err := f.Write(frame); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	s.mu.Lock()
	off := int64(0)
	if es := s.indexes[msg.Topic].entries; len(es) > 0 {
		last := es[len(es)-1]
		off = last.Offset + int64(last.Length)
	}
	s.indexes[msg.Topic].entries = append(s.indexes[msg.Topic].entries, indexEntry{
		Timestamp: msg.Timestamp.UnixNano(),
		Seq:       seq,
		SchemaID:  schemaID,
		Offset:    off,
		Length:    len(frame),
	})
	rec := s.recovery[msg.Topic]
	if rec.CompleteFrames == 0 {
		rec.FirstTime = msg.Timestamp
	}
	rec.LastTime = msg.Timestamp
	rec.CompleteFrames++
	s.recovery[msg.Topic] = rec
	s.mu.Unlock()
	return nil
}

// Close 关闭打开的写入句柄。
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, f := range s.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.files = map[string]*os.File{}
	return firstErr
}

// Topics 返回 bag 中已知的全部话题。
func (s *Storage) Topics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sortedTopics(s.manifest)
}

// TimeRange 返回某话题当前已完整落盘消息的最早/最晚时间戳。
func (s *Storage) TimeRange(topic string) (time.Time, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.indexes[topic]
	if !ok || len(idx.entries) == 0 {
		return time.Time{}, time.Time{}, false
	}
	return time.Unix(0, idx.entries[0].Timestamp), time.Unix(0, idx.entries[len(idx.entries)-1].Timestamp), true
}

func decodeTopicFileName(name string) (string, error) {
	var b []byte
	for i := 0; i < len(name); i++ {
		if name[i] == '%' {
			if i+2 >= len(name) {
				return "", ErrCorrupt
			}
			hi, err1 := hexVal(name[i+1])
			lo, err2 := hexVal(name[i+2])
			if err1 != nil || err2 != nil {
				return "", ErrCorrupt
			}
			b = append(b, hi<<4|lo)
			i += 2
			continue
		}
		b = append(b, name[i])
	}
	return string(b), nil
}

func hexVal(ch byte) (byte, error) {
	switch {
	case ch >= '0' && ch <= '9':
		return ch - '0', nil
	case ch >= 'a' && ch <= 'f':
		return ch - 'a' + 10, nil
	case ch >= 'A' && ch <= 'F':
		return ch - 'A' + 10, nil
	default:
		return 0, ErrCorrupt
	}
}
