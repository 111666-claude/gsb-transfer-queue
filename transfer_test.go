package transferq

import "testing"

func service() *Service {
	return NewService(map[string]int{"s1": 5, "s2": 5})
}

func TestSubmitToFreeServerMoves(t *testing.T) {
	ok, status := service().Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	if !ok || status != "moved" {
		t.Fatalf("空服应该直接转过去：%v %s", ok, status)
	}
}

func TestLoadIncrements(t *testing.T) {
	instance := service()
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	if instance.Load("s1") != 1 {
		t.Fatalf("转服之后负载应该加一：%d", instance.Load("s1"))
	}
}

func TestUnknownServerRejected(t *testing.T) {
	if ok, _ := service().Submit(Request{RequestID: "r1", Account: "a", To: "s9", AtMs: 0}); ok {
		t.Fatal("不存在的服务器不该接受")
	}
}

func TestConstants(t *testing.T) {
	if DailyQuota != 2 || QueueWaitMs != 30000 {
		t.Fatal("转服常量被改了")
	}
}
