# transfer-queue

角色转服：请求幂等、每日额度、目标服容量、排队补位，只用 Go 标准库。

```
go test ./...
go run ./cmd/transfer-queue --sample idem
go run ./cmd/transfer-queue --sample quota
go run ./cmd/transfer-queue --sample capacity
```

## 口径（README 为准）

- **请求幂等**：同一个 `RequestID` 只生效一次，第二次返回 `(false, "idempotent")` 且不改负载与额度。
- **每日额度**：同一账号每天最多成功转服 `DailyQuota` 次，超出返回 `(false, "quota")`。
- **容量与排队**：目标服 `Load < Capacity` 时直接转（`"moved"`，负载加一）；
  容量满时入队（`(true, "queued")`，不加负载），`Release` 后按入队顺序补位。
- **超时清理**：排队超过 `QueueWaitMs` 的请求必须从队列里清掉。
- **查询代价**：幂等台账、额度、容量都用索引，`Scanned` 不随队列长度线性增长。
- **规模**：单服 50 万排队请求、每秒 10 万次提交与查询，单次摊还 O(1)。

## 输出契约（不改格式）

```
first=true second=false status=idempotent load=1
third=false
ok=true status=queued queued=1 load=1
scanned<=6000
```

## 目录

```
transfer.go             转服、额度与排队
cmd/transfer-queue      命令行入口
transfer_test.go        go test 用例
```
