package bag_test

import (
	"fmt"
	"testing"
)

// caseLogger 让每个业务分组的测试日志清晰可见：
//
//	=== [业务: 中断恢复] 用例: 半截尾帧被截断且完整帧可读 ... PASS
//
// 运行 `go test -v` 即可看出覆盖了哪些业务、每个业务有哪些用例及其结果。
type caseLogger struct {
	business string
	t        *testing.T
}

func newBusiness(t *testing.T, business string) *caseLogger {
	t.Helper()
	t.Logf("=== [业务: %s] 测试开始 ===", business)
	return &caseLogger{business: business, t: t}
}

func (c *caseLogger) check(name string, ok bool, detail ...any) {
	c.t.Helper()
	status := "PASS"
	if !ok {
		status = "FAIL"
	}
	msg := fmt.Sprintf("=== [业务: %s] 用例: %s ... %s", c.business, name, status)
	if len(detail) > 0 {
		msg += " | " + fmt.Sprint(detail...)
	}
	c.t.Log(msg)
	if !ok {
		c.t.FailNow()
	}
}

func (c *caseLogger) end() {
	c.t.Helper()
	c.t.Logf("=== [业务: %s] 测试结束 ===", c.business)
}
