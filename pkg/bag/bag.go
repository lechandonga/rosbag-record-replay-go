package bag

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

// Entry 是一条已落盘消息在索引中的位置。
type Entry struct {
	Topic  string
	Stamp  int64
	Seq    uint64
	Schema msgdef.Schema

	segment string
	offset  int64
	length  int64
}

// Span 描述一段纳秒时间区间 [Start, End)。零值边界表示不限。
type Span struct {
	Start int64 // 0 表示从头
	End   int64 // 0 表示到尾
}

func (s Span) contains(stamp int64) bool {
	if s.Start != 0 && stamp < s.Start {
		return false
	}
	if s.End != 0 && stamp >= s.End {
		return false
	}
	return true
}

// IssueKind 描述打开时段文件的健康问题类别。
type IssueKind string

const (
	IssuePartialTail IssueKind = "partial_tail_recovered" // 崩溃半截帧，已安全丢弃
	IssueCorruption  IssueKind = "corruption"             // 段中间损坏，该段不可信
)

// ScanIssue 是段扫描时发现的问题。
type ScanIssue struct {
	Segment string
	Kind    IssueKind
	Detail  string
	Offset  int64
}

// HealthReport 汇总一次打开看到的数据健康状况。
type HealthReport struct {
	Issues []ScanIssue
}

// Corruption 返回第一条硬损坏问题（若有）。半截尾巴不属于损坏。
func (h HealthReport) Corruption() *ScanIssue {
	for i := range h.Issues {
		if h.Issues[i].Kind == IssueCorruption {
			return &h.Issues[i]
		}
	}
	return nil
}

type topicReg struct {
	TypeName string
	FP       string
}

type metadataFile struct {
	Schemas map[string]msgdef.Schema `json:"schemas"`
	Topics  map[string]topicReg      `json:"topics"`
}

// Reader 对 bag 目录做只读访问，可与唯一写者并发打开多个。
type Reader struct {
	dir    string
	mu     sync.RWMutex
	snap   snapshot
	health HealthReport
}

type snapshot struct {
	entries []Entry
	topics  map[string]topicReg
	schemas map[string]msgdef.Schema
	// corruptSegs 标记不可信段；对这些段的查询必须报错而不是静默漏掉。
	corruptSegs map[string]ScanIssue
	empty       bool
}

// OpenReader 只读打开目录并建立索引快照。目录不存在或从未成功写入时返回 ErrNoBag。
func OpenReader(dir string) (*Reader, error) {
	r := &Reader{dir: dir}
	if err := r.Refresh(); err != nil {
		return nil, err
	}
	return r, nil
}

// Refresh 重新扫描目录，建立新的一致性快照（每次 Refresh 之间读到的内容不变）。
func (r *Reader) Refresh() error {
	snap, health, err := buildSnapshot(r.dir)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.snap = snap
	r.health = health
	r.mu.Unlock()
	return nil
}

// Health 返回打开/最近一次 Refresh 时的健康报告。
func (r *Reader) Health() HealthReport {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.health
}

// Topics 返回当前登记过的话题列表。
func (r *Reader) Topics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.snap.topics))
	for t := range r.snap.topics {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// HasData 报告 bag 是否存在任何完整消息。
func (r *Reader) HasData() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.snap.empty && len(r.snap.entries) > 0
}

// Query 按话题集合（空表示全部）和时间区间查询消息索引，
// 结果按 (时间戳, 序列号) 全局排序；同一时间戳有确定次序，乱序写入也会被归位。
func (r *Reader) Query(topics []string, span Span) ([]Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snap.empty {
		return nil, ErrNoBag
	}
	// 任何段存在硬损坏都意味着区间结果可能不完整，必须显式报错，
	// 不能静默漏掉一段后返回“空结果”（空结果只表示区间本来无数据）。
	if len(r.snap.corruptSegs) > 0 {
		for _, issue := range r.snap.corruptSegs {
			return nil, fmt.Errorf("%w: segment %s cannot be trusted (%s at offset %d: %s)",
				ErrCorruption, issue.Segment, issue.Kind, issue.Offset, issue.Detail)
		}
	}
	want := map[string]bool{}
	for _, t := range topics {
		want[t] = true
	}
	var out []Entry
	for _, e := range r.snap.entries {
		if len(want) > 0 && !want[e.Topic] {
			continue
		}
		if span.contains(e.Stamp) {
			out = append(out, e)
		}
	}
	return out, nil
}

// ReadPayload 读取单条消息的原始编码负载。
func (r *Reader) ReadPayload(e Entry) ([]byte, error) {
	f, err := os.Open(filepath.Join(r.dir, e.segment))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, e.length)
	if _, err := f.ReadAt(buf, e.offset); err != nil {
		return nil, fmt.Errorf("%w: payload read failed: %v", ErrCorruption, err)
	}
	return buf, nil
}

// Decode 用读取方 schema 解码一条索引消息；schema 演进规则见 msgdef 包。
func (r *Reader) Decode(e Entry, readerSchema msgdef.Schema) (map[string]any, []msgdef.FieldNote, error) {
	payload, err := r.ReadPayload(e)
	if err != nil {
		return nil, nil, err
	}
	return msgdef.Decode(e.Schema, readerSchema, payload)
}

func listSegments(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, filepath.Base(m))
	}
	sort.Strings(names)
	return names, nil
}

func buildSnapshot(dir string) (snapshot, HealthReport, error) {
	snap := snapshot{topics: map[string]topicReg{}, schemas: map[string]msgdef.Schema{}, corruptSegs: map[string]ScanIssue{}}
	var health HealthReport
	if _, err := os.Stat(dir); err != nil {
		return snap, health, ErrNoBag
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return snap, health, ErrNoBag
	}
	segs, err := listSegments(dir)
	if err != nil {
		return snap, health, err
	}
	if len(segs) == 0 {
		snap.empty = true
		return snap, health, ErrNoBag
	}
	for _, name := range segs {
		// listSegments 与打开之间写者可能已滚动；段文件只增不删，
		// 正常不会缺失；若缺失只可能是目录被外部清理，跳过即可。
		path := filepath.Join(dir, name)
		f, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return snap, health, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return snap, health, err
		}
		res, scanErr := scanSegment(f, st.Size())
		f.Close()
		if scanErr != nil {
			var issue ScanIssue
			if errors.Is(scanErr, ErrCorruption) {
				issue = ScanIssue{Segment: name, Kind: IssueCorruption, Detail: scanErr.Error()}
				health.Issues = append(health.Issues, issue)
				snap.corruptSegs[name] = issue
				continue
			}
			return snap, health, scanErr
		}
		if res.partialTail {
			health.Issues = append(health.Issues, ScanIssue{
				Segment: name, Kind: IssuePartialTail, Offset: res.goodEnd,
				Detail: "incomplete trailing frame from an abrupt process exit was discarded",
			})
		}
		for _, fr := range res.frames {
			switch fr.kind {
			case kindTopic:
				if fr.meta.Schema != nil {
					snap.schemas[fr.meta.SchemaFP] = *fr.meta.Schema
				}
			case kindAttach:
				snap.topics[fr.meta.Topic] = topicReg{TypeName: fr.meta.TypeName, FP: fr.meta.SchemaFP}
			case kindMsg:
				schemas := snap.schemas
				sc, ok := schemas[fr.meta.SchemaFP]
				if !ok {
					issue := ScanIssue{Segment: name, Kind: IssueCorruption,
						Detail: fmt.Sprintf("message references unknown schema fingerprint %s", fr.meta.SchemaFP)}
					health.Issues = append(health.Issues, issue)
					snap.corruptSegs[name] = issue
				} else {
					snap.entries = append(snap.entries, Entry{
						Topic: fr.meta.Topic, Stamp: fr.stamp, Seq: fr.seq, Schema: sc,
						segment: name,
						offset:  fr.bodyOff,
						length:  int64(len(fr.body)),
					})
				}
			}
		}
	}
	sort.SliceStable(snap.entries, func(i, j int) bool {
		if snap.entries[i].Stamp != snap.entries[j].Stamp {
			return snap.entries[i].Stamp < snap.entries[j].Stamp
		}
		if snap.entries[i].Seq != snap.entries[j].Seq {
			return snap.entries[i].Seq < snap.entries[j].Seq
		}
		return snap.entries[i].segment < snap.entries[j].segment
	})
	if len(snap.entries) == 0 && len(snap.corruptSegs) == 0 {
		snap.empty = true
	}
	return snap, health, nil
}
