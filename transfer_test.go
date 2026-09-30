package transferq

import (
	"fmt"
	"testing"
)

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

func TestDuplicateRequestIdempotent(t *testing.T) {
	instance := service()
	first, _ := instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	second, status := instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 10})
	if !first {
		t.Fatal("第一次提交应该成功")
	}
	if second || status != "idempotent" {
		t.Fatalf("重发应该幂等拒绝：%v %s", second, status)
	}
	if instance.Load("s1") != 1 {
		t.Fatalf("重发不该改负载：%d", instance.Load("s1"))
	}
	if ok, _ := instance.Submit(Request{RequestID: "r2", Account: "a", To: "s1", AtMs: 20}); !ok {
		t.Fatal("额度没被重发吃掉，第二次应该成功")
	}
}

func TestDailyQuota(t *testing.T) {
	instance := NewService(map[string]int{"s1": 9, "s2": 9, "s3": 9})
	for index, target := range []string{"s1", "s2", "s3"} {
		ok, status := instance.Submit(Request{
			RequestID: fmt.Sprintf("r%d", index), Account: "a", To: target, AtMs: int64(index),
		})
		if index < DailyQuota {
			if !ok || status != "moved" {
				t.Fatalf("前 %d 次应该成功：%v %s", DailyQuota, ok, status)
			}
		} else if ok || status != "quota" {
			t.Fatalf("超出每日额度应该拒绝：%v %s", ok, status)
		}
	}
	if instance.Load("s3") != 0 {
		t.Fatalf("超额请求不该加负载：%d", instance.Load("s3"))
	}
}

func TestCapacityQueuesAndReleasePromotes(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	ok, status := instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
	if !ok || status != "queued" {
		t.Fatalf("容量满了应该排队：%v %s", ok, status)
	}
	if instance.Queued() != 1 || instance.Load("s1") != 1 {
		t.Fatalf("排队不该加负载：queued=%d load=%d", instance.Queued(), instance.Load("s1"))
	}
	instance.Release("s1")
	if instance.Load("s1") != 1 || instance.Queued() != 0 {
		t.Fatalf("释放后应该按序补位：queued=%d load=%d", instance.Queued(), instance.Load("s1"))
	}
}

func TestQueueTimeoutCleanup(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
	instance.Submit(Request{RequestID: "r3", Account: "c", To: "s1", AtMs: 20})
	if instance.Queued() != 2 {
		t.Fatalf("应该有两个排队：%d", instance.Queued())
	}
	instance.Sweep(10 + QueueWaitMs + 1)
	if instance.Queued() != 1 {
		t.Fatalf("超时请求应该被清掉：%d", instance.Queued())
	}
	instance.Release("s1")
	if instance.Load("s1") != 1 || instance.Queued() != 0 {
		t.Fatalf("补位应该跳过超时请求：queued=%d load=%d", instance.Queued(), instance.Load("s1"))
	}
}

func TestLoadNeverExceedsCapacity(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	for index := 0; index < 100; index++ {
		instance.Submit(Request{
			RequestID: fmt.Sprintf("r%d", index), Account: fmt.Sprintf("a%d", index%26), To: "s1", AtMs: int64(index),
		})
		if instance.Load("s1") > 1 {
			t.Fatalf("负载不该超过容量：%d", instance.Load("s1"))
		}
	}
}

func TestQueueFIFOOrder(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a1", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "a2", To: "s1", AtMs: 1})
	instance.Submit(Request{RequestID: "r3", Account: "a3", To: "s1", AtMs: 2})
	instance.Release("s1")
	instance.Release("s1")
	instance.Release("s1")
	if instance.Load("s1") != 0 || instance.Queued() != 0 {
		t.Fatalf("三次释放后应该清空：load=%d queued=%d", instance.Load("s1"), instance.Queued())
	}
	if instance.used["0/a2"] != 1 || instance.used["0/a3"] != 1 {
		t.Fatal("补位顺序应该按入队先后：r2、r3 各转一次")
	}
}

func TestReplayDeterministic(t *testing.T) {
	caps := map[string]int{"s1": 1, "s2": 2}
	run := func() (loads map[string]int, queued int) {
		instance := NewService(caps)
		instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
		instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
		instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 10})
		instance.Submit(Request{RequestID: "r3", Account: "c", To: "s1", AtMs: 20})
		instance.Release("s1")
		instance.Sweep(QueueWaitMs + 21)
		instance.Release("s1")
		return map[string]int{"s1": instance.Load("s1"), "s2": instance.Load("s2")}, instance.Queued()
	}
	loads1, queued1 := run()
	loads2, queued2 := run()
	if queued1 != queued2 || loads1["s1"] != loads2["s1"] || loads1["s2"] != loads2["s2"] {
		t.Fatalf("同一串序列重复执行结果不一致：%v/%d vs %v/%d", loads1, queued1, loads2, queued2)
	}
}
