package bag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// 磁盘布局（一个 bag 就是一个目录）：
//
//	<bagDir>/
//	  manifest.json           版本号与 topic -> schema id 列表（定义演进历史）
//	  schemas/<schemaID>.json 消息定义（不可变，内容定址）
//	  data/<topic>.bag        该 topic 的追加帧文件
//	  data/<topic>.idx        启动扫描生成的索引（缓存，可删除重建）
//
// 帧格式（全部大端）：
//
//	magic   4 字节  9D 5B A2 17
//	u32     payloadLen
//	u64     timestamp（UnixNano）
//	u64     seq（同一 topic 内单调递增的写入序号）
//	u16     schemaID 长度 + schemaID 字节
//	payload TLV 消息体
//	u32     CRC32-IEEE，覆盖 magic 到 payload 末尾
//
// 帧没有跨帧引用，天然可追加；读取端顺序扫描即可重建索引。

const bagVersion = 1

var frameMagic = [4]byte{0x9D, 0x5B, 0xA2, 0x17}

type manifest struct {
	Version int                 `json:"version"`
	Topics  map[string][]string `json:"topics"` // topic -> 按注册顺序的 schema id
}

// TopicRecovery 描述一个 topic 数据文件在打开时的恢复情况。
type TopicRecovery struct {
	Topic          string
	CompleteFrames int    // 成功识别且 CRC 正确的完整帧
	TruncatedTail  int    // 文件尾部被截断丢弃的字节数（崩溃时正在写的半帧）
	Corrupt        bool   // 文件中间存在无法重同步的坏帧
	CorruptDetail  string // 损坏原因（字节位置等）
	FirstTime      time.Time
	LastTime       time.Time
}

func (r TopicRecovery) describe() string {
	return fmt.Sprintf("topic %q: %d complete frames, %d truncated tail bytes, corrupt=%v",
		r.Topic, r.CompleteFrames, r.TruncatedTail, r.Corrupt)
}

// schemaID 是定义规范化 JSON 的 sha256 前 16 字节十六进制。
// 定义内容相同即同 id，与注册顺序无关。
func schemaID(d MessageDef) (string, error) {
	canon, err := canonicalDef(d)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:16]), nil
}

func canonicalDef(d MessageDef) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	// json.Marshal 对 map 排序、struct 按字段声明顺序，输出稳定。
	return json.Marshal(d)
}

type schemaStore struct {
	dir string
	mu  sync.Mutex
}

func (s *schemaStore) path(id string) string { return filepath.Join(s.dir, id+".json") }

func (s *schemaStore) save(d MessageDef) (string, error) {
	if err := d.Validate(); err != nil {
		return "", err
	}
	id, err := schemaID(d)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.path(id)); err == nil {
		return id, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	b, err := canonicalDef(d)
	if err != nil {
		return "", err
	}
	tmp := s.path(id) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, s.path(id)); err != nil {
		return "", err
	}
	return id, nil
}

func (s *schemaStore) load(id string) (MessageDef, error) {
	b, err := os.ReadFile(s.path(id))
	if err != nil {
		return MessageDef{}, fmt.Errorf("bag: schema %q unavailable: %w", id, err)
	}
	var d MessageDef
	if err := json.Unmarshal(b, &d); err != nil {
		return MessageDef{}, fmt.Errorf("bag: schema %q invalid: %w", id, err)
	}
	if err := d.Validate(); err != nil {
		return MessageDef{}, err
	}
	return d, nil
}

func loadManifest(dir string) (*manifest, error) {
	m := &manifest{Version: bagVersion, Topics: map[string][]string{}}
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("bag: manifest invalid: %w", err)
	}
	if m.Version != bagVersion {
		return nil, fmt.Errorf("bag: unsupported manifest version %d", m.Version)
	}
	if m.Topics == nil {
		m.Topics = map[string][]string{}
	}
	return m, nil
}

func atomicWriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Record 是一条已落盘的完整消息在内存中的句柄。
type Record struct {
	Topic     string
	Timestamp time.Time
	Seq       uint64
	SchemaID  string
	Offset    int64 // 在 data 文件中的帧起始偏移
	Length    int   // 帧总长度
}

// indexEntry 与 Record 对应，用于 idx 缓存文件。
type indexEntry struct {
	Timestamp int64  `json:"t"`
	Seq       uint64 `json:"s"`
	SchemaID  string `json:"d"`
	Offset    int64  `json:"o"`
	Length    int    `json:"l"`
}

type topicIndex struct {
	entries []indexEntry
}

func sortedTopics(m *manifest) []string {
	names := make([]string, 0, len(m.Topics))
	for t := range m.Topics {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}
