package transferq

import (
	"strconv"
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

// ---- 修复后的口径测试 ----

func TestDuplicateRequestIdempotent(t *testing.T) {
	instance := service()
	if ok, _ := instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0}); !ok {
		t.Fatal("首次提交应该成功")
	}
	ok, status := instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 10})
	if ok || status != "idempotent" {
		t.Fatalf("重发同一个 RequestID 应该幂等拒绝：%v %s", ok, status)
	}
	if instance.Load("s1") != 1 {
		t.Fatalf("重发不该改负载：%d", instance.Load("s1"))
	}
	// 重发也不该占额度：r1 占一份，r2 还能成功，r3 才被 quota 拒。
	instance.Submit(Request{RequestID: "r2", Account: "a", To: "s1", AtMs: 20})
	if ok, status := instance.Submit(Request{RequestID: "r3", Account: "a", To: "s1", AtMs: 30}); ok || status != "quota" {
		t.Fatalf("重发不该占额度，r3 才该被 quota 拒：%v %s", ok, status)
	}
}

func TestDailyQuota(t *testing.T) {
	instance := NewService(map[string]int{"s1": 9})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "a", To: "s1", AtMs: 1})
	if ok, status := instance.Submit(Request{RequestID: "r3", Account: "a", To: "s1", AtMs: 2}); ok || status != "quota" {
		t.Fatalf("当天第三次应该被额度拒掉：%v %s", ok, status)
	}
	if instance.Load("s1") != 2 {
		t.Fatalf("被额度拒的请求不该加负载：%d", instance.Load("s1"))
	}
	if ok, _ := instance.Submit(Request{RequestID: "r4", Account: "b", To: "s1", AtMs: 3}); !ok {
		t.Fatal("别的账号不受额度影响")
	}
}

func TestDailyQuotaResetsNextDay(t *testing.T) {
	instance := NewService(map[string]int{"s1": 9})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "a", To: "s1", AtMs: 1})
	if ok, status := instance.Submit(Request{RequestID: "r3", Account: "a", To: "s1", AtMs: dayMs}); !ok || status != "moved" {
		t.Fatalf("新的一天额度应该重置：%v %s", ok, status)
	}
}

func TestCapacityFullQueues(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	ok, status := instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
	if !ok || status != "queued" {
		t.Fatalf("容量满了应该排队：%v %s", ok, status)
	}
	if instance.Load("s1") != 1 {
		t.Fatalf("排队不该加负载：%d", instance.Load("s1"))
	}
	if instance.Queued() != 1 {
		t.Fatalf("应该有一个排队请求：%d", instance.Queued())
	}
}

func TestReleasePromotesInOrder(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
	instance.Submit(Request{RequestID: "r3", Account: "c", To: "s1", AtMs: 20})
	instance.Release("s1")
	if instance.Load("s1") != 1 || instance.Queued() != 1 {
		t.Fatalf("释放后应该按入队顺序补位一个：load=%d queued=%d", instance.Load("s1"), instance.Queued())
	}
	instance.Release("s1")
	if instance.Load("s1") != 1 || instance.Queued() != 0 {
		t.Fatalf("再释放应该再补位一个：load=%d queued=%d", instance.Load("s1"), instance.Queued())
	}
}

func TestLoadNeverExceedsCapacity(t *testing.T) {
	instance := NewService(map[string]int{"s1": 2})
	for i := 0; i < 10; i++ {
		instance.Submit(Request{RequestID: "r" + string(rune('0'+i)), Account: "a" + string(rune('0'+i)), To: "s1", AtMs: int64(i)})
	}
	if instance.Load("s1") != 2 {
		t.Fatalf("负载永远不该超过容量：%d", instance.Load("s1"))
	}
	if instance.Queued() != 8 {
		t.Fatalf("多余的请求应该排队：%d", instance.Queued())
	}
}

func TestQueuedRequestExpires(t *testing.T) {
	instance := NewService(map[string]int{"s1": 0})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: QueueWaitMs})
	if got := instance.Queued(); got != 2 {
		t.Fatalf("刚好等满 QueueWaitMs 不算超时：%d", got)
	}
	instance.Submit(Request{RequestID: "r3", Account: "c", To: "s1", AtMs: QueueWaitMs + 1})
	if got := instance.Queued(); got != 2 {
		t.Fatalf("排队超过 QueueWaitMs 的 r1 应该被清掉：%d", got)
	}
}

func TestExpiredRequestNotPromoted(t *testing.T) {
	instance := NewService(map[string]int{"s1": 1})
	instance.Submit(Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 0})
	instance.Submit(Request{RequestID: "r3", Account: "c", To: "s1", AtMs: QueueWaitMs + 1})
	instance.Release("s1")
	if instance.Load("s1") != 1 || instance.Queued() != 0 {
		t.Fatalf("超时的 r2 不该补位，r3 补上后队列应清空：load=%d queued=%d", instance.Load("s1"), instance.Queued())
	}
}

func TestScannedDoesNotGrowWithQueue(t *testing.T) {
	instance := NewService(map[string]int{"s1": 0})
	const total = 500000
	for i := 0; i < total; i++ {
		id := strconv.Itoa(i)
		instance.Submit(Request{RequestID: "r" + id, Account: "a" + id, To: "s1", AtMs: int64(i / 20000)})
	}
	if got := instance.Queued(); got != total {
		t.Fatalf("50 万请求都应该在队列里：%d", got)
	}
	if instance.Scanned() > total {
		t.Fatalf("单次提交摊还 O(1)，Scanned 不该随队列长度线性增长：%d", instance.Scanned())
	}
}
