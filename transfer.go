// Package transferq 是角色转服：请求幂等、每日额度、目标服容量、排队补位。
// 缺陷：重复请求会重复生效、不看每日额度、容量满了也照样放行、
// 排队请求从不超时清理、每次提交都扫一遍队列。
package transferq

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

// Service 是转服服务。
type Service struct {
	capacity map[string]int
	load     map[string]int
	quota    map[string]int
	handled  map[string]bool
	queue    []Request
	scanned  int
}

// NewService 按各服容量建服务。
func NewService(capacity map[string]int) *Service {
	return &Service{
		capacity: capacity,
		load:     map[string]int{},
		quota:    map[string]int{},
		handled:  map[string]bool{},
	}
}

// Load 是某服的当前负载。
func (s *Service) Load(server string) int { return s.load[server] }

// Queued 是排队中的请求数。
func (s *Service) Queued() int { return len(s.queue) }

// Scanned 是累计扫描的台账条数（规模观测）。
func (s *Service) Scanned() int { return s.scanned }

// Submit 提交一次转服请求，返回是否被接受与结果状态。
// 缺陷：不查幂等台账、不查每日额度、不看目标服容量，每次都直接 load 加一。
func (s *Service) Submit(req Request) (bool, string) {
	s.scanned += len(s.handled)
	if _, ok := s.capacity[req.To]; !ok {
		return false, "noserver"
	}
	s.handled[req.RequestID] = true
	s.load[req.To]++
	s.quota[req.Account]++
	return true, "moved"
}

// Release 释放一个名额并按队列补位。
// 缺陷：只是把负载减一，队列里的请求永远不动，超时请求也不清理。
func (s *Service) Release(server string) {
	if s.load[server] > 0 {
		s.load[server]--
	}
}
