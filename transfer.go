// Package transferq 是角色转服：请求幂等、每日额度、目标服容量、排队补位。
// 幂等台账、每日额度、目标服容量全部走索引；排队按目标服分链，
// 超时请求按事件时间惰性摘除，单次操作摊还 O(1)，负载永不超过容量。
package transferq

import "container/list"

// DailyQuota 是每个账号每天的转服额度。
const DailyQuota = 2

// QueueWaitMs 是排队请求的最长等待时间。
const QueueWaitMs = 30000

// dayMs 是一天的毫秒数，每日额度按自然日划分。
const dayMs int64 = 24 * 60 * 60 * 1000

// Request 是一次转服请求。
type Request struct {
	RequestID string
	Account   string
	To        string
	AtMs      int64
}

// quotaKey 按账号与自然日统计额度。
type quotaKey struct {
	account string
	day     int64
}

// queuedRequest 是排队中的请求，同时挂在目标服队列与全局超时队列上，
// 两个链上的节点都持有指针，摘除时 O(1)。
type queuedRequest struct {
	req        Request
	serverElem *list.Element
	expiryElem *list.Element
}

// Service 是转服服务。
type Service struct {
	capacity map[string]int
	load     map[string]int
	quota    map[quotaKey]int
	handled  map[string]struct{}
	queues   map[string]*list.List
	expiry   *list.List
	queued   int
	nowMs    int64
	scanned  int
}

// NewService 按各服容量建服务。
func NewService(capacity map[string]int) *Service {
	return &Service{
		capacity: capacity,
		load:     map[string]int{},
		quota:    map[quotaKey]int{},
		handled:  map[string]struct{}{},
		queues:   map[string]*list.List{},
		expiry:   list.New(),
	}
}

// Load 是某服的当前负载。
func (s *Service) Load(server string) int { return s.load[server] }

// Queued 是排队中的请求数（已清掉超时请求）。
func (s *Service) Queued() int {
	s.evictExpired()
	return s.queued
}

// Scanned 是累计扫描的台账条数（规模观测）。
func (s *Service) Scanned() int { return s.scanned }

// Submit 提交一次转服请求，返回是否被接受与结果状态：
// 容量未满直接 "moved" 并加负载；容量已满返回 (true, "queued") 入队等待补位；
// 重复 RequestID 返回 (false, "idempotent")，不改负载与额度；
// 超出当日 DailyQuota 返回 (false, "quota")；目标服不存在返回 (false, "noserver")。
// 调用方需按 AtMs 非递减的事件时间顺序提交。
func (s *Service) Submit(req Request) (bool, string) {
	if req.AtMs > s.nowMs {
		s.nowMs = req.AtMs
	}
	s.evictExpired()
	// 幂等台账走索引，一次提交只查一条。
	s.scanned++
	if _, ok := s.capacity[req.To]; !ok {
		return false, "noserver"
	}
	if _, dup := s.handled[req.RequestID]; dup {
		return false, "idempotent"
	}
	key := quotaKey{account: req.Account, day: req.AtMs / dayMs}
	if s.quota[key] >= DailyQuota {
		return false, "quota"
	}
	s.handled[req.RequestID] = struct{}{}
	s.quota[key]++
	if s.load[req.To] < s.capacity[req.To] {
		s.load[req.To]++
		return true, "moved"
	}
	entry := &queuedRequest{req: req}
	queue := s.queues[req.To]
	if queue == nil {
		queue = list.New()
		s.queues[req.To] = queue
	}
	entry.serverElem = queue.PushBack(entry)
	entry.expiryElem = s.expiry.PushBack(entry)
	s.queued++
	return true, "queued"
}

// Release 释放一个名额并按入队顺序给同服队列补位。
func (s *Service) Release(server string) {
	s.evictExpired()
	if s.load[server] > 0 {
		s.load[server]--
	}
	queue := s.queues[server]
	for queue != nil && queue.Len() > 0 && s.load[server] < s.capacity[server] {
		entry := queue.Front().Value.(*queuedRequest)
		s.removeQueued(entry)
		s.load[server]++
	}
}

// evictExpired 清掉排队超过 QueueWaitMs 的请求；按入队顺序（AtMs 非递减）
// 从头摘除，每个请求最多被摘一次，摊还 O(1)。
func (s *Service) evictExpired() {
	for s.expiry.Len() > 0 {
		entry := s.expiry.Front().Value.(*queuedRequest)
		if s.nowMs-entry.req.AtMs <= QueueWaitMs {
			return
		}
		s.removeQueued(entry)
	}
}

// removeQueued 把排队请求同时从目标服队列与全局超时队列摘除。
func (s *Service) removeQueued(entry *queuedRequest) {
	s.queues[entry.req.To].Remove(entry.serverElem)
	s.expiry.Remove(entry.expiryElem)
	s.queued--
}
