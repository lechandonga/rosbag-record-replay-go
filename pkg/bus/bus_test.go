package bus

import "testing"

func TestPubSub(t *testing.T) {
	business := "本地发布订阅模拟"
	b := New(4)
	s1 := b.Subscribe("/t")
	s2 := b.Subscribe("/other")
	if b.SubscriberCount("/t") != 1 {
		t.Fatal("订阅计数错误")
	}
	b.Publish(Message{Topic: "/t", Stamp: 1, Data: map[string]any{"v": int64(1)}})
	msg := <-s1.C
	if msg.Stamp != 1 || msg.Topic != "/t" {
		t.Fatalf("收到异常消息: %+v", msg)
	}
	select {
	case m := <-s2.C:
		t.Fatalf("话题隔离失败，/other 收到 %v", m)
	default:
	}
	t.Logf("[业务:%s] 用例[发布/订阅/话题隔离] 通过：订阅者收到消息，其他话题不受影响", business)
	s1.Close()
	if b.SubscriberCount("/t") != 0 {
		t.Fatal("取消订阅失败")
	}
	if _, ok := <-s1.C; ok {
		t.Fatal("取消订阅后通道应关闭")
	}
	t.Logf("[业务:%s] 用例[取消订阅] 通过：通道关闭且计数归零", business)
}
