package bag

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func (s *Storage) persistIndex(topic string) error {
	idx, ok := s.indexes[topic]
	if !ok {
		return nil
	}
	b, err := json.Marshal(idx.entries)
	if err != nil {
		return err
	}
	p := filepath.Join(s.dataDir, topicFileName(topic)+".idx")
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// Clip 把 filter 覆盖区间内的完整消息截取到 dstDir（新 bag）。
// 截取结果按“当前定义”重新编码并注册：因此目标 bag 里的消息定义与调用方给出的
// defs 一致（未给的话题沿用落盘时的原始定义）。演进不兼容会原样报错并中止。
func (s *Snapshot) Clip(dstDir string, f RangeFilter, defs map[string]MessageDef) (int, error) {
	dst, _, err := Open(dstDir)
	if err != nil {
		return 0, err
	}
	defer dst.Close()

	msgs, err := s.ReadRange(f, defs)
	if err != nil {
		return 0, err
	}
	registered := map[string]bool{}
	n := 0
	for _, m := range msgs {
		if !registered[m.Topic] {
			target, ok := defs[m.Topic]
			if !ok {
				d, err := s.def(schemaIDOf(s, m))
				if err != nil {
					return n, err
				}
				target = d
			}
			if err := dst.RegisterTopic(m.Topic, target); err != nil {
				return n, err
			}
			registered[m.Topic] = true
		}
		if err := dst.Write(Message{Topic: m.Topic, Timestamp: m.Timestamp, Values: m.Values}); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// schemaIDOf 通过快照中的记录查找消息落盘定义 id（Clip 内部辅助）。
func schemaIDOf(s *Snapshot, m ReplayedMessage) string {
	for _, e := range s.indexes[m.Topic] {
		if e.Seq == m.Seq && e.Timestamp == m.Timestamp.UnixNano() {
			return e.SchemaID
		}
	}
	return ""
}
