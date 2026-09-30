package transferq

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// refEntry 是参考实现里的排队条目。
type refEntry struct {
	req Request
}

// reference 是按 README 口径写的朴素参考实现：幂等台账 + 每日额度 + 容量排队，
// 数据结构与被测实现刻意不同，用来做差分对照。
type reference struct {
	capacity map[string]int
	load     map[string]int
	used     map[string]int
	handled  map[string]Request
	queues   map[string][]refEntry
	queued   int
	now      int64
}

func newReference(capacity map[string]int) *reference {
	return &reference{
		capacity: capacity,
		load:     map[string]int{},
		used:     map[string]int{},
		handled:  map[string]Request{},
		queues:   map[string][]refEntry{},
	}
}

func refQuotaKey(account string, atMs int64) string {
	return quotaKey(account, atMs)
}

// dropFront 弹出某服队首的一个条目。
func (r *reference) dropFront(server string) refEntry {
	q := r.queues[server]
	item := q[0]
	r.queues[server] = q[1:]
	r.queued--
	return item
}

func (r *reference) advance(atMs int64) {
	if atMs > r.now {
		r.now = atMs
	}
}

func (r *reference) expireFront(server string) {
	for len(r.queues[server]) > 0 && r.now-r.queues[server][0].req.AtMs > QueueWaitMs {
		r.dropFront(server)
	}
}

func (r *reference) submit(req Request) (bool, string) {
	r.advance(req.AtMs)
	if _, ok := r.handled[req.RequestID]; ok {
		return false, "idempotent"
	}
	if _, ok := r.capacity[req.To]; !ok {
		return false, "noserver"
	}
	r.expireFront(req.To)
	if r.load[req.To] >= r.capacity[req.To] {
		r.handled[req.RequestID] = req
		r.queues[req.To] = append(r.queues[req.To], refEntry{req: req})
		r.queued++
		return true, "queued"
	}
	key := refQuotaKey(req.Account, r.now)
	if r.used[key] >= DailyQuota {
		return false, "quota"
	}
	r.handled[req.RequestID] = req
	r.used[key]++
	r.load[req.To]++
	return true, "moved"
}

func (r *reference) release(server string) {
	if _, ok := r.capacity[server]; !ok {
		return
	}
	if r.load[server] > 0 {
		r.load[server]--
	}
	for r.load[server] < r.capacity[server] {
		r.expireFront(server)
		if len(r.queues[server]) == 0 {
			break
		}
		item := r.dropFront(server)
		key := refQuotaKey(item.req.Account, r.now)
		if r.used[key] >= DailyQuota {
			continue
		}
		r.used[key]++
		r.load[server]++
	}
}

func (r *reference) sweep(atMs int64) {
	r.advance(atMs)
	for server := range r.queues {
		r.expireFront(server)
	}
}

// op 是随机序列里的一个操作。
type op struct {
	kind   string
	req    Request
	server string
	atMs   int64
}

func TestRandomDifferential(t *testing.T) {
	const runs = 200
	rng := rand.New(rand.NewSource(20261001))
	servers := []string{"s1", "s2"}

	for run := 0; run < runs; run++ {
		caps := map[string]int{
			"s1": 1 + rng.Intn(3),
			"s2": 1 + rng.Intn(3),
		}
		actual := NewService(caps)
		ref := newReference(caps)

		accountCount := 2 + rng.Intn(3)
		accounts := make([]string, accountCount)
		for index := range accounts {
			accounts[index] = fmt.Sprintf("acc%d", index)
		}

		var now int64
		idCounter := 0
		history := []Request{}
		ops := 60 + rng.Intn(120)

		snapshot := func(stage string) {
			for _, server := range servers {
				if actual.Load(server) != ref.load[server] {
					t.Fatalf("第 %d 组 %s：服 %s 负载不一致 actual=%d ref=%d",
						run, stage, server, actual.Load(server), ref.load[server])
				}
				if actual.Load(server) > caps[server] {
					t.Fatalf("第 %d 组 %s：服 %s 负载 %d 超过容量 %d",
						run, stage, server, actual.Load(server), caps[server])
				}
			}
			if actual.Queued() != ref.queued {
				t.Fatalf("第 %d 组 %s：队列长度不一致 actual=%d ref=%d",
					run, stage, actual.Queued(), ref.queued)
			}
		}

		for step := 0; step < ops; step++ {
			roll := rng.Intn(100)
			switch {
			case roll < 55 || len(history) == 0:
				// 提交新请求；小概率跨天验证额度重置。
				if rng.Intn(40) == 0 {
					now += 24 * 60 * 60 * 1000
				}
				now += int64(rng.Intn(2000))
				req := Request{
					RequestID: fmt.Sprintf("r%d", idCounter),
					Account:   accounts[rng.Intn(accountCount)],
					To:        servers[rng.Intn(len(servers))],
					AtMs:      now,
				}
				idCounter++
				history = append(history, req)
				ok1, st1 := actual.Submit(req)
				ok2, st2 := ref.submit(req)
				if ok1 != ok2 || st1 != st2 {
					t.Fatalf("第 %d 组第 %d 步提交结果不一致 %+v：actual=(%v,%s) ref=(%v,%s)",
						run, step, req, ok1, st1, ok2, st2)
				}
			case roll < 75:
				// 重发历史请求，大半原样重发，小半带更晚的时间戳。
				req := history[rng.Intn(len(history))]
				if rng.Intn(3) == 0 {
					req.AtMs += int64(rng.Intn(40000))
				}
				ok1, st1 := actual.Submit(req)
				ok2, st2 := ref.submit(req)
				if ok1 != ok2 || st1 != st2 {
					t.Fatalf("第 %d 组第 %d 步重发结果不一致 %+v：actual=(%v,%s) ref=(%v,%s)",
						run, step, req, ok1, st1, ok2, st2)
				}
			case roll < 90:
				server := servers[rng.Intn(len(servers))]
				now += int64(rng.Intn(2000))
				if rng.Intn(5) == 0 {
					now += QueueWaitMs + 1
				}
				actual.Release(server)
				ref.release(server)
			default:
				now += int64(rng.Intn(2000))
				if rng.Intn(2) == 0 {
					now += QueueWaitMs + 1
				}
				actual.Sweep(now)
				ref.sweep(now)
			}
			snapshot(fmt.Sprintf("第 %d 步", step))
		}
	}
}

func TestScale500k(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过规模测试")
	}
	instance := NewService(map[string]int{"s1": 1})
	const queuedTotal = 500000
	// 全部请求同一时刻入队，避免触发超时清理，制造 50 万积压。
	for index := 0; index <= queuedTotal; index++ {
		instance.Submit(Request{
			RequestID: fmt.Sprintf("r%d", index), Account: "a", To: "s1", AtMs: 0,
		})
	}
	if instance.Queued() != queuedTotal || instance.Load("s1") != 1 {
		t.Fatalf("初始状态不对：queued=%d load=%d", instance.Queued(), instance.Load("s1"))
	}

	const operations = 100000
	start := time.Now()
	for index := 0; index < operations; index++ {
		switch index % 3 {
		case 0:
			instance.Submit(Request{RequestID: "r0", Account: "a", To: "s1", AtMs: 0})
		case 1:
			instance.Queued()
		default:
			instance.Load("s1")
		}
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Fatalf("%d 次提交与查询耗时 %s，未达到每秒 10 万次", operations, elapsed)
	}
	// 仅 50 万零 1 次入队提交 + 热循环里的重发各贡献 1 次台账探测，
	// 50 万队列长度本身不产生扫描。
	const resends = operations/3 + 1
	if instance.Scanned() != queuedTotal+1+resends {
		t.Fatalf("Scanned=%d，预期 %d", instance.Scanned(), queuedTotal+1+resends)
	}
	t.Logf("%d 次操作耗时 %s（%.0f ops/s），Scanned=%d",
		operations, elapsed, float64(operations)/elapsed.Seconds(), instance.Scanned())
}
