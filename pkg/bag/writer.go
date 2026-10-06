package bag

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

// Writer 是 bag 目录的唯一写者（目录锁保护）。
type Writer struct {
	dir       string
	lockFile  *os.File
	mu        sync.Mutex
	cur       *os.File
	curName   string
	curSize   int64
	segIndex  int
	seq       uint64
	maxSeg    int64
	schemas   map[string]msgdef.Schema
	topics    map[string]topicReg
	syncEvery bool
}

type frameWrite struct {
	kind  frameKind
	stamp int64
	seq   uint64
	meta  frameMeta
	body  []byte
}

// OpenWriter 以写者身份打开目录；同一目录同时只允许一个写者。
// 若上次进程崩溃留下半截尾帧，打开时会先把段文件截断到最后一个完整帧之后。
func OpenWriter(dir string, maxSegmentBytes int64) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("bag directory %q is already open for writing: %w", dir, err)
	}
	w := &Writer{
		dir:       dir,
		lockFile:  lf,
		maxSeg:    maxSegmentBytes,
		schemas:   map[string]msgdef.Schema{},
		topics:    map[string]topicReg{},
		syncEvery: true,
	}
	if err := w.recoverOrInit(); err != nil {
		_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
		lf.Close()
		return nil, err
	}
	return w, nil
}

// SetSyncEachWrite 控制每条消息是否 fsync。默认 true，保证崩溃时只丢“正在写的这一帧”。
func (w *Writer) SetSyncEachWrite(v bool) { w.syncEvery = v }

func (w *Writer) recoverOrInit() error {
	// 加载元数据（可能不存在）。
	data, err := os.ReadFile(filepath.Join(w.dir, "metadata.json"))
	switch {
	case err == nil:
		var md metadataFile
		if err := json.Unmarshal(data, &md); err != nil {
			return fmt.Errorf("metadata.json unreadable: %w", err)
		}
		if md.Schemas != nil {
			w.schemas = md.Schemas
		}
		if md.Topics != nil {
			w.topics = md.Topics
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return err
	}
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, name := range segs {
		path := filepath.Join(w.dir, name)
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		res, scanErr := scanSegment(f, st.Size())
		f.Close()
		if scanErr != nil {
			return scanErr // 硬损坏不自动截断，避免掩盖问题
		}
		if res.partialTail {
			// 截断崩溃留下的半截帧。
			if err := os.Truncate(path, res.goodEnd); err != nil {
				return err
			}
		}
		for _, fr := range res.frames {
			switch fr.kind {
			case kindTopic:
				if fr.meta.Schema != nil {
					w.schemas[fr.meta.SchemaFP] = *fr.meta.Schema
				}
			case kindAttach:
				w.topics[fr.meta.Topic] = topicReg{TypeName: fr.meta.TypeName, FP: fr.meta.SchemaFP}
			case kindMsg:
				if fr.seq > maxSeq {
					maxSeq = fr.seq
				}
			}
		}
		var idx int
		fmt.Sscanf(name, "seg-%d.log", &idx)
		if idx >= w.segIndex {
			w.segIndex = idx
		}
	}
	w.seq = maxSeq
	if len(segs) == 0 {
		return w.rotateLocked(0)
	}
	// 追加到最后一个段。
	last := segs[len(segs)-1]
	path := filepath.Join(w.dir, last)
	// 扫描得到最后一个完整帧边界（已含对 EOF 标记/半截帧的判断）。
	sf, err := os.Open(path)
	if err != nil {
		return err
	}
	st, _ := sf.Stat()
	res, scanErr := scanSegment(sf, st.Size())
	sf.Close()
	if scanErr != nil {
		return scanErr
	}
	// 统一截断：去掉半截帧与旧 EOF 标记，随后重新追加标记。
	if err := os.Truncate(path, res.goodEnd); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := writeEOFMarker(f, res.goodEnd); err != nil {
		f.Close()
		return err
	}
	w.cur = f
	w.curName = last
	w.curSize = res.goodEnd + int64(eofSize)
	return nil
}

func (w *Writer) rotateLocked(idx int) error {
	if w.cur != nil {
		_ = w.cur.Close()
	}
	name := fmt.Sprintf("seg-%05d.log", idx)
	f, err := os.OpenFile(filepath.Join(w.dir, name), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := writeEOFMarker(f, 0); err != nil {
		f.Close()
		return err
	}
	w.cur = f
	w.curName = name
	w.segIndex = idx
	w.curSize = int64(eofSize)
	return nil
}

func writeEOFMarker(f *os.File, dataLen int64) error {
	var b [eofSize]byte
	binary.BigEndian.PutUint64(b[:], eofMarker)
	_, err := f.WriteAt(b[:], dataLen)
	return err
}

// RegisterTopic 登记一个话题及其消息定义。同一话题再次登记时，
// schema 指纹必须与此前一致；定义演进请用新的类型指纹（本组件按名匹配校验规则）。
func (w *Writer) RegisterTopic(topic, typeName string, schema msgdef.Schema) error {
	if err := schema.Validate(); err != nil {
		return err
	}
	fp := schema.Fingerprint()
	w.mu.Lock()
	defer w.mu.Unlock()
	if reg, ok := w.topics[topic]; ok {
		if reg.FP != fp {
			return fmt.Errorf("topic %q already registered with schema %s, got %s", topic, reg.FP, fp)
		}
		return nil
	}
	var frames []frameWrite
	if _, ok := w.schemas[fp]; !ok {
		sc := schema
		frames = append(frames, frameWrite{kind: kindTopic, meta: frameMeta{TypeName: typeName, SchemaFP: fp, Schema: &sc}})
		w.schemas[fp] = schema
	}
	frames = append(frames, frameWrite{kind: kindAttach, meta: frameMeta{Topic: topic, TypeName: typeName, SchemaFP: fp}})
	if err := w.appendLocked(frames...); err != nil {
		return err
	}
	w.topics[topic] = topicReg{TypeName: typeName, FP: fp}
	return w.persistMetadataLocked()
}

// Write 写入一条消息。payload 为 msgdef.Encode 的结果。
// stamp 是消息原始时间戳；不要求单调（乱序允许，读取侧统一排序）。
func (w *Writer) Write(topic string, stamp int64, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	reg, ok := w.topics[topic]
	if !ok {
		return fmt.Errorf("topic %q is not registered", topic)
	}
	w.seq++
	return w.appendLocked(frameWrite{
		kind: kindMsg, stamp: stamp, seq: w.seq,
		meta: frameMeta{Topic: topic, TypeName: reg.TypeName, SchemaFP: reg.FP},
		body: payload,
	})
}

func (w *Writer) appendLocked(items ...frameWrite) error {
	for _, it := range items {
		buf, err := encodeFrame(it.kind, it.stamp, it.seq, it.meta, it.body)
		if err != nil {
			return err
		}
		dataLen := w.curSize - int64(eofSize)
		if w.maxSeg > 0 && dataLen+int64(len(buf)) > w.maxSeg {
			if err := w.rotateLocked(w.segIndex + 1); err != nil {
				return err
			}
			dataLen = 0
		}
		// 帧先写到 EOF 标记之后（读者尚不可见），再把标记移到帧尾使其原子可见。
		if _, err := w.cur.WriteAt(buf, dataLen); err != nil {
			return err
		}
		if err := writeEOFMarker(w.cur, dataLen+int64(len(buf))); err != nil {
			return err
		}
		w.curSize = dataLen + int64(len(buf)) + int64(eofSize)
	}
	if w.syncEvery {
		if err := w.cur.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// persistMetadataLocked 原子重命名写元数据，保证读者永远不会看到半份 JSON。
func (w *Writer) persistMetadataLocked() error {
	md := metadataFile{Schemas: w.schemas, Topics: w.topics}
	buf, err := json.MarshalIndent(md, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(w.dir, "metadata.json.tmp")
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, "metadata.json")); err != nil {
		return err
	}
	return nil
}

// CurrentSegment 返回正在写入的段文件名（测试与观测用）。
func (w *Writer) CurrentSegment() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.curName
}

// Close 冲刷并释放目录锁。
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.cur != nil {
		err = w.cur.Close()
	}
	if w.lockFile != nil {
		_ = syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
		_ = w.lockFile.Close()
	}
	return err
}
