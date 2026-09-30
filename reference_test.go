package transferq

import (
	"fmt"
	"math/rand"
	"testing"
)

// refService 是「幂等台账 + 每日额度 + 容量排队」的朴素参考实现：
// 每次操作都全量扫描，语义直白，用来对照索引实现每一步的结果。
type refService struct {
	capacity map[string]int
	load     map[string]int
	quota    map[quotaKey]int
	handled  map[string]bool
	queue    []Request
	nowMs    int64
}

func newRefService(capacity map[string]int) *refService {
	return &refService{
		capacity: capacity,
		load:     map[string]int{},
		quota:    map[quotaKey]int{},
		handled:  map[string]bool{},
	}
}

func (r *refService) evictExpired() {
	kept := r.queue[:0]
	for _, req := range r.queue {
		if r.nowMs-req.AtMs <= QueueWaitMs {
			kept = append(kept, req)
		}
	}
	r.queue = kept
}

func (r *refService) submit(req Request) (bool, string) {
	if req.AtMs > r.nowMs {
		r.nowMs = req.AtMs
	}
	r.evictExpired()
	if _, ok := r.capacity[req.To]; !ok {
		return false, "noserver"
	}
	if r.handled[req.RequestID] {
		return false, "idempotent"
	}
	key := quotaKey{account: req.Account, day: req.AtMs / dayMs}
	if r.quota[key] >= DailyQuota {
		return false, "quota"
	}
	r.handled[req.RequestID] = true
	r.quota[key]++
	if r.load[req.To] < r.capacity[req.To] {
		r.load[req.To]++
		return true, "moved"
	}
	r.queue = append(r.queue, req)
	return true, "queued"
}

func (r *refService) release(server string) {
	r.evictExpired()
	if r.load[server] > 0 {
		r.load[server]--
	}
	for r.load[server] < r.capacity[server] {
		index := -1
		for i, req := range r.queue {
			if req.To == server {
				index = i
				break
			}
		}
		if index < 0 {
			return
		}
		r.queue = append(r.queue[:index], r.queue[index+1:]...)
		r.load[server]++
	}
}

func (r *refService) queued() int {
	r.evictExpired()
	return len(r.queue)
}

// TestMatchesReference 跑 200 组随机「提交 + 重发 + 超额 + 释放」序列，
// 每一步比对索引实现与朴素参考实现的状态、各服负载、队列长度。
func TestMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20260930))
	servers := []string{"s1", "s2", "s3"}
	accounts := []string{"a", "b"}
	for trial := 0; trial < 200; trial++ {
		capacity := make(map[string]int, len(servers))
		for _, server := range servers {
			capacity[server] = rng.Intn(4)
		}
		indexed := NewService(capacity)
		ref := newRefService(capacity)
		var nowMs int64
		var ids []string
		steps := 30 + rng.Intn(40)
		for step := 0; step < steps; step++ {
			nowMs += int64(rng.Intn(20000))
			if rng.Intn(20) == 0 {
				nowMs += dayMs // 偶尔跨天，覆盖每日额度重置
			}
			if len(ids) == 0 || rng.Intn(10) < 7 {
				id := fmt.Sprintf("t%03d-r%03d", trial, step)
				if len(ids) > 0 && rng.Intn(10) < 4 {
					id = ids[rng.Intn(len(ids))] // 客户端重发
				}
				req := Request{
					RequestID: id,
					Account:   accounts[rng.Intn(len(accounts))],
					To:        servers[rng.Intn(len(servers))],
					AtMs:      nowMs,
				}
				okGot, statusGot := indexed.Submit(req)
				okWant, statusWant := ref.submit(req)
				if okGot != okWant || statusGot != statusWant {
					t.Fatalf("第 %d 组第 %d 步提交 %+v：索引实现 (%v,%s)，参考实现 (%v,%s)",
						trial, step, req, okGot, statusGot, okWant, statusWant)
				}
				ids = append(ids, id)
			} else {
				server := servers[rng.Intn(len(servers))]
				indexed.Release(server)
				ref.release(server)
			}
			for _, server := range servers {
				if indexed.Load(server) > capacity[server] {
					t.Fatalf("第 %d 组第 %d 步 %s 负载 %d 超过容量 %d",
						trial, step, server, indexed.Load(server), capacity[server])
				}
				if indexed.Load(server) != ref.load[server] {
					t.Fatalf("第 %d 组第 %d 步 %s 负载：索引实现 %d，参考实现 %d",
						trial, step, server, indexed.Load(server), ref.load[server])
				}
			}
			if indexed.Queued() != ref.queued() {
				t.Fatalf("第 %d 组第 %d 步队列长度：索引实现 %d，参考实现 %d",
					trial, step, indexed.Queued(), ref.queued())
			}
		}
	}
}
