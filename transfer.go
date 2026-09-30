// Package transferq 是角色转服：请求幂等、每日额度、目标服容量、排队补位。
package transferq

import "strconv"

// DailyQuota 是每个账号每天的转服额度。
const DailyQuota = 2

// QueueWaitMs 是排队请求的最长等待时间。
const QueueWaitMs = 30000

// Request 是一次转服请求。
type Request struct {
	RequestID string
	Account   string
	To        string
	AtMs      int64
}

// entry 是队列里的一条排队请求。
type entry struct {
	req Request
}

// Service 是转服服务。
// 幂等台账、每日额度、容量、队列全部走索引或队首推进，单次提交摊还 O(1)，
// Scanned 只随提交次数线性，不随队列长度线性。
type Service struct {
	capacity map[string]int
	load     map[string]int
	used     map[string]int
	handled  map[string]Request
	queues   map[string][]entry
	head     map[string]int
	queued   int
	now      int64
	scanned  int
}

// NewService 按各服容量建服务。
func NewService(capacity map[string]int) *Service {
	return &Service{
		capacity: capacity,
		load:     map[string]int{},
		used:     map[string]int{},
		handled:  map[string]Request{},
		queues:   map[string][]entry{},
		head:     map[string]int{},
	}
}

// Load 是某服的当前负载。
func (s *Service) Load(server string) int { return s.load[server] }

// Queued 是排队中的请求数。
func (s *Service) Queued() int { return s.queued }

// Scanned 是累计扫描的台账条数（规模观测）。
func (s *Service) Scanned() int { return s.scanned }

// quotaKey 是账号在某天的额度桶键。
func quotaKey(account string, atMs int64) string {
	const msPerDay = 24 * 60 * 60 * 1000
	return strconv.FormatInt(atMs/msPerDay, 10) + "/" + account
}

// expire 清掉某服队首等待超过 QueueWaitMs 的请求。
// 队首之后的过期请求由后续释放继续清，单条请求在整个生命周期内最多被推进一次，
// 因此任意操作的摊还代价都是 O(1)。
func (s *Service) expire(server string) {
	q := s.queues[server]
	h := s.head[server]
	for h < len(q) && s.now-q[h].req.AtMs > QueueWaitMs {
		h++
		s.queued--
	}
	s.head[server] = h
	if len(q) > 0 && (h == len(q) || h*2 >= len(q)) {
		remaining := copy(q, q[h:])
		s.queues[server] = q[:remaining]
		s.head[server] = 0
	}
}

// advance 推进服务时钟，时间只会单调向前。
func (s *Service) advance(atMs int64) {
	if atMs > s.now {
		s.now = atMs
	}
}

// Submit 提交一次转服请求，返回是否被接受与结果状态。
// 同一 RequestID 只生效一次；超出每日额度拒绝；容量未满直接 moved，
// 容量满了 queued 且不加负载。
func (s *Service) Submit(req Request) (bool, string) {
	s.advance(req.AtMs)
	s.scanned++
	if _, ok := s.handled[req.RequestID]; ok {
		return false, "idempotent"
	}
	if _, ok := s.capacity[req.To]; !ok {
		return false, "noserver"
	}
	s.expire(req.To)
	if s.load[req.To] >= s.capacity[req.To] {
		s.handled[req.RequestID] = req
		s.queues[req.To] = append(s.queues[req.To], entry{req: req})
		s.queued++
		return true, "queued"
	}
	if s.used[quotaKey(req.Account, s.now)] >= DailyQuota {
		return false, "quota"
	}
	s.handled[req.RequestID] = req
	s.used[quotaKey(req.Account, s.now)]++
	s.load[req.To]++
	return true, "moved"
}

// Release 释放一个名额，清掉队首超时请求后按入队顺序补位。
func (s *Service) Release(server string) {
	if _, ok := s.capacity[server]; !ok {
		return
	}
	if s.load[server] > 0 {
		s.load[server]--
	}
	s.fill(server)
}

// fill 让某服从队首依次补位，直到占满或队列清空。
func (s *Service) fill(server string) {
	q := s.queues[server]
	h := s.head[server]
	for s.load[server] < s.capacity[server] {
		for h < len(q) && s.now-q[h].req.AtMs > QueueWaitMs {
			h++
			s.queued--
		}
		if h == len(q) {
			break
		}
		req := q[h].req
		key := quotaKey(req.Account, s.now)
		if s.used[key] >= DailyQuota {
			h++
			s.queued--
			continue
		}
		h++
		s.queued--
		s.used[key]++
		s.load[server]++
	}
	s.head[server] = h
	if len(q) > 0 && (h == len(q) || h*2 >= len(q)) {
		remaining := copy(q, q[h:])
		s.queues[server] = q[:remaining]
		s.head[server] = 0
	}
}

// Sweep 推进到指定时刻并清掉所有服队首的超时请求。
func (s *Service) Sweep(atMs int64) {
	s.advance(atMs)
	for server := range s.queues {
		s.expire(server)
	}
}
