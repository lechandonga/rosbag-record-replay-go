package bag

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/lechandonga/rosbag-record-replay-go/pkg/msgdef"
)

// 单帧布局（全部大端）：
//
//	偏移 0    4B   magic "LOGB"
//	偏移 4    1B   版本
//	偏移 5    1B   kind（1=schema 2=attach 3=message）
//	偏移 6    2B   保留
//	偏移 8    8B   stamp（纳秒）
//	偏移 16   8B   seq
//	偏移 24   8B   metaLen   （gob 元数据长度，可为 0）
//	偏移 32   8B   bodyLen   （消息负载长度，可为 0）
//	偏移 40   32B  sha256(meta + body)
//	偏移 72   8B   commit = totalLen（提交标记，永远最后写）
//	固定头合计 80 字节，其后是 meta、body。
//
// 读取时前向扫描：commit == totalLen 且校验和通过才算完整帧；
// 文件尾部剩余无法闭合的字节就是“正在写/崩溃半截帧”，安全丢弃。

const (
	magicStr     = "LOGB"
	frameVer1    = 1
	headerSize   = 80
	commitOffset = 72
	sumOffset    = 40
	sumSize      = 32
	lenSize      = 8
	eofSize      = 8
	// eofMarker 写在段文件最末 8 字节；其之前的字节才是“已提交数据”。
	eofMarker = uint64(0x454F46004C4F4742) // "EOF\0LOGB"
)

var (
	ErrCorruption = errors.New("bag segment corrupted")
	ErrNoBag      = errors.New("no recorded data in bag directory")
)

type frameKind byte

const (
	kindTopic  frameKind = 1
	kindAttach frameKind = 2
	kindMsg    frameKind = 3
)

type frameMeta struct {
	Topic    string
	TypeName string
	SchemaFP string
	Schema   *msgdef.Schema
}

type rawFrame struct {
	kind    frameKind
	stamp   int64
	seq     uint64
	meta    frameMeta
	body    []byte
	start   int64
	total   int64
	bodyOff int64
}

type scanResult struct {
	frames      []rawFrame
	goodEnd     int64
	partialTail bool
}

// encodeFrame 生成整帧字节。commit 字段在最末 8 字节，一次 Write 落盘。
func encodeFrame(kind frameKind, stamp int64, seq uint64, m frameMeta, body []byte) ([]byte, error) {
	var metaBuf bytes.Buffer
	if err := gob.NewEncoder(&metaBuf).Encode(m); err != nil {
		return nil, err
	}
	meta := metaBuf.Bytes()
	total := int64(headerSize + len(meta) + len(body))
	out := make([]byte, headerSize, total)
	copy(out[0:4], magicStr)
	out[4] = frameVer1
	out[5] = byte(kind)
	binary.BigEndian.PutUint64(out[8:], uint64(stamp))
	binary.BigEndian.PutUint64(out[16:], seq)
	binary.BigEndian.PutUint64(out[24:], uint64(len(meta)))
	binary.BigEndian.PutUint64(out[32:], uint64(len(body)))
	out = append(out, meta...)
	out = append(out, body...)
	sum := sha256.Sum256(out[headerSize:])
	copy(out[sumOffset:sumOffset+sumSize], sum[:])
	binary.BigEndian.PutUint64(out[commitOffset:commitOffset+lenSize], uint64(total))
	return out, nil
}

func scanSegment(r io.ReaderAt, size int64) (scanResult, error) {
	res := scanResult{goodEnd: size}
	// 段尾若存在 EOF 标记，则标记之前才是已提交数据；
	// 标记不存在（崩溃时标记还没移过来）时，用最后一个完整帧边界。
	readable := size
	hasMarker := false
	if size >= int64(eofSize) {
		mark := make([]byte, eofSize)
		if _, err := r.ReadAt(mark, size-int64(eofSize)); err != nil && err != io.EOF {
			return res, err
		}
		if binary.BigEndian.Uint64(mark) == eofMarker {
			readable = size - int64(eofSize)
			hasMarker = true
		}
	}
	hdr := make([]byte, headerSize)
	var off int64
	for off < readable {
		if readable-off < int64(headerSize) {
			res.partialTail = true
			res.goodEnd = off
			break
		}
		if _, err := r.ReadAt(hdr, off); err != nil && err != io.EOF {
			return res, err
		}
		total := int64(binary.BigEndian.Uint64(hdr[commitOffset : commitOffset+lenSize]))
		metaLen := int64(binary.BigEndian.Uint64(hdr[24:32]))
		bodyLen := int64(binary.BigEndian.Uint64(hdr[32:40]))
		want := int64(headerSize) + metaLen + bodyLen
		// 帧不完整：
		// 有 EOF 标记却读不到整帧 => 段中间/尾部损坏（标记之前理应都是完整帧）；
		// 无标记（崩溃恢复路径）且停在可读区末尾 => 半截帧；否则也是损坏。
		if total <= 0 || total != want || total > readable-off {
			if hasMarker {
				return res, fmt.Errorf("%w: committed region has incomplete frame at offset %d (total=%d want=%d remaining=%d)",
					ErrCorruption, off, total, want, readable-off)
			}
			if off == 0 || bytes.Equal(hdr[commitOffset:commitOffset+lenSize], make([]byte, 8)) {
				res.partialTail = true
				res.goodEnd = off
				break
			}
			res.partialTail = true
			res.goodEnd = off
			break
		}
		buf := make([]byte, total-headerSize)
		if _, err := r.ReadAt(buf, off+headerSize); err != nil && err != io.EOF {
			return res, err
		}
		sum := sha256.Sum256(buf[:metaLen+bodyLen])
		if !bytes.Equal(sum[:], hdr[sumOffset:sumOffset+sumSize]) {
			if !hasMarker && off+total == readable {
				res.partialTail = true
				res.goodEnd = off
				break
			}
			return res, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorruption, off)
		}
		if string(hdr[0:4]) != magicStr || hdr[4] != frameVer1 {
			return res, fmt.Errorf("%w: bad magic/version at offset %d", ErrCorruption, off)
		}
		var m frameMeta
		if metaLen > 0 {
			if err := gob.NewDecoder(bytes.NewReader(buf[:metaLen])).Decode(&m); err != nil {
				return res, fmt.Errorf("%w: meta decode failed at offset %d: %v", ErrCorruption, off, err)
			}
		}
		rf := rawFrame{
			kind: frameKind(hdr[5]), stamp: int64(binary.BigEndian.Uint64(hdr[8:16])),
			seq: binary.BigEndian.Uint64(hdr[16:24]), meta: m,
			start: off, total: total, bodyOff: off + headerSize + metaLen,
		}
		rf.body = append([]byte(nil), buf[metaLen:metaLen+bodyLen]...)
		res.frames = append(res.frames, rf)
		off += total
	}
	return res, nil
}
