package bag

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// encodeFrame 生成一帧的完整字节。payload 为 TLV 消息体。
func encodeFrame(ts time.Time, seq uint64, schemaID string, payload []byte) []byte {
	sid := []byte(schemaID)
	headerLen := 4 + 4 + 8 + 8 + 2 + len(sid)
	plen := uint32(headerLen - 4 + len(payload) + 4)
	b := make([]byte, 0, headerLen+len(payload)+4)
	b = append(b, frameMagic[:]...)
	b = binary.BigEndian.AppendUint32(b, plen)
	b = binary.BigEndian.AppendUint64(b, uint64(ts.UnixNano()))
	b = binary.BigEndian.AppendUint64(b, seq)
	b = binary.BigEndian.AppendUint16(b, uint16(len(sid)))
	b = append(b, sid...)
	b = append(b, payload...)
	crc := crc32.ChecksumIEEE(b)
	b = binary.BigEndian.AppendUint32(b, crc)
	return b
}

// parsedFrame 是扫描得到的一帧。
type parsedFrame struct {
	ts       time.Time
	seq      uint64
	schemaID string
	payload  []byte
	start    int64
	length   int
}

// scanFrames 顺序扫描一个 topic 的数据文件。
// 返回完整帧和恢复报告。语义：
//   - 文件尾部不完整（长度不足、payloadLen 超出文件、CRC 错）一律视为崩溃截断，
//     记入 TruncatedTail，不返回该帧；
//   - 帧必须严格连续（下一帧 magic 紧跟当前帧尾），一旦在非 EOF 处无法解析，
//     尝试从下一个 magic 重同步；重同步成功则判定为中间损坏（Corrupt），
//     跳过坏帧继续；无法重同步则剩余部分全部按截断处理且标记 Corrupt。
func scanFrames(raw []byte, topic string) ([]parsedFrame, TopicRecovery) {
	rec := TopicRecovery{Topic: topic}
	var frames []parsedFrame
	pos := 0
	for pos < len(raw) {
		frame, next, err := parseOne(raw, pos)
		if err == nil {
			if len(frames) == 0 {
				rec.FirstTime = frame.ts
			}
			rec.LastTime = frame.ts
			frames = append(frames, frame)
			pos = next
			continue
		}
		// 解析失败：从 pos+1 起寻找下一个 magic 重同步点。
		resync := findMagic(raw, pos+1)
		if resync < 0 {
			// 找不到后续帧边界：无法区分“崩溃写了一半”与“尾部整块损坏”，
			// 统一按崩溃截断处理（这些字节没有 CRC，从不返回给读取方）。
			// 真正的中间损坏（后面还有可重同步的完整帧）走下面的 Corrupt 分支。
			rec.TruncatedTail += len(raw) - pos
			pos = len(raw)
			break
		}
		rec.Corrupt = true
		if rec.CorruptDetail == "" {
			rec.CorruptDetail = "bad frame at offset " + itoa(int64(pos)) + ", resynced at " + itoa(int64(resync))
		}
		pos = resync
	}
	rec.CompleteFrames = len(frames)
	return frames, rec
}

var errBadFrame = ErrCorrupt

func parseOne(raw []byte, pos int) (parsedFrame, int, error) {
	start := pos
	if pos+8 > len(raw) {
		return parsedFrame{}, 0, errBadFrame
	}
	for i := range frameMagic {
		if raw[pos+i] != frameMagic[i] {
			return parsedFrame{}, 0, errBadFrame
		}
	}
	plen := int(binary.BigEndian.Uint32(raw[pos+4:]))
	end := pos + 4 + plen
	if plen < 8+8+2+4 || end > len(raw) {
		return parsedFrame{}, 0, errBadFrame
	}
	body := raw[pos : end-4]
	wantCRC := binary.BigEndian.Uint32(raw[end-4:])
	if crc32.ChecksumIEEE(body) != wantCRC {
		return parsedFrame{}, 0, errBadFrame
	}
	p := pos + 8
	ts := time.Unix(0, int64(binary.BigEndian.Uint64(raw[p:])))
	p += 8
	seq := binary.BigEndian.Uint64(raw[p:])
	p += 8
	sidLen := int(binary.BigEndian.Uint16(raw[p:]))
	p += 2
	if sidLen > plen {
		return parsedFrame{}, 0, errBadFrame
	}
	sid := string(raw[p : p+sidLen])
	p += sidLen
	payload := raw[p : end-4]
	return parsedFrame{
		ts:       ts,
		seq:      seq,
		schemaID: sid,
		payload:  payload,
		start:    int64(start),
		length:   end - start,
	}, end, nil
}

func findMagic(raw []byte, from int) int {
	for i := from; i+4 <= len(raw); i++ {
		if raw[i] == frameMagic[0] &&
			raw[i+1] == frameMagic[1] &&
			raw[i+2] == frameMagic[2] &&
			raw[i+3] == frameMagic[3] {
			return i
		}
	}
	return -1
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
