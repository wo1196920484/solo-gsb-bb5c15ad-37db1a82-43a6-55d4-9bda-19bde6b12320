# 乱序事件归并 API（Python · FastAPI · SQLite）

客户端按 `stream_id` 提交带有 `event_id`、`sequence`（从 0 开始递增）和整数 `delta`
的事件。服务端只把**从当前 `next_sequence` 起连续**的事件依次累加到余额；遇到缺号
时，后续事件先暂存。当缺失事件到达时，在**同一个数据库事务**内自动完成追赶
（catch-up）。

核心特性：

- **幂等重放**：相同 `event_id` 以相同内容重复提交返回 `200`，不会重复累加；
  内容不一致返回 `409 Conflict`。
- **序号唯一**：同一 `stream_id` 下每个 `sequence` 只能属于一个 `event_id`，
  重复序号（即使 `delta` 相同）返回 `409 Conflict`。
- **原子追赶**：缺失事件到达后，连续事件链的累加、`applied` 标记和
  `next_sequence`/`balance` 更新全部在同一事务内提交；冲突时整体回滚，已暂存
  事件不受影响。
- **并发安全**：SQLite WAL 模式 + `BEGIN IMMEDIATE`，并发提交串行化，每个序号
  恰好应用一次。
- **消费游标与租约**：消费者可领取（claim）从自身游标开始、已应用且连续的事件批，
  服务端发放带 `expires_at` 的 `lease_token`；租约未到期重复领取返回**同一批次、
  同一 token**，持 token 确认（ack）后游标原子推进；过期/旧 token 返回 `409`，
  租约到期后同批事件可被再次领取（at-least-once）。多个实例并发领取也只会存在
  一个有效租约。
- **消费拒绝与死信（nack / dead letter）**：`POST /streams/{stream_id}/consumers/{consumer_id}/nack`
  携带 `lease_token` 与**非空** `reason` 拒绝一个**非空**有效批次：**不推进游标**，
  对批次**首事件**累加一次连续失败次数并**立即释放租约**，失败次数未达环境变量
  `CONSUMER_MAX_ATTEMPTS`（Compose 默认 `3`）时，下次 claim 仍从原游标返回该首事件；
  达到阈值的那次 nack 在**同一个 `BEGIN IMMEDIATE` 事务**内写入死信记录并把游标
  **恰好越过该事件一个序号**，之后的 claim 从下一序号继续。空批次不可 nack（`409
  empty_batch_not_nackable`），旧/伪造/已过期/已 nack 后又领取新租约的 token 返回
  `409`；同一 token 在新租约产生前重试返回**原结果**且**不重复计数**（`replayed:
  true`）。ack 成功会清零被越过事件的连续失败次数。死信可经
  `GET /streams/{stream_id}/consumers/{consumer_id}/dead-letters` 按 `sequence`
  升序查询（事件内容、失败次数、最后原因、隔离时间），且按消费者相互隔离。
- **流分叉（fork）**：把源流 `[0, through_sequence)` 的**已应用**事件整体复制为
  一个新的、可继续追加事件的目标流；`fork_id` 在同一源流下是幂等键，重试安全，
  分叉记录、目标流和复制事件在同一个 `BEGIN IMMEDIATE` 事务内提交，并发请求不会
  产生半成品。
- **跨事件流原子转账（transfer）**：`POST /transfers` 以全局幂等键 `transfer_id`
  从一个已有流扣款、向另一个已有流入账；扣款（`delta=-amount`）与入账
  （`delta=+amount`）两条事件分别追加在两侧当前 `next_sequence` 上，并在**同一个
  `BEGIN IMMEDIATE` 事务**内完成连续应用与余额更新，任一步失败整体回滚。相同
  `transfer_id` 携带相同参数重试返回原结果且不重复扣款，参数不一致返回 `409`，
  并发重试只提交一次。
- **事件撤销（reversal）**：`POST /streams/{stream_id}/events/{event_id}/reversals`
  以全局幂等键 `reversal_id` 撤销一个**已应用且尚未撤销**的事件：在当前
  `next_sequence` 追加一条 `event_id=reversal:{reversal_id}`、`delta=-原 delta`
  的补偿事件，并在**同一个 `BEGIN IMMEDIATE` 事务**内完成连续应用、状态更新与
  撤销记录，任一步失败整体回滚。相同 `reversal_id` 对相同目标重试返回 `200` 与
  最新状态，对不同目标返回 `409`；同一事件换一个 `reversal_id` 再次撤销返回
  `409`；并发重试只补偿一次。补偿事件就是普通的已应用事件，可被查询、消费与
  分叉接口正常观察。
- **不可逆封存（seal）**：`POST /streams/{stream_id}/seal` 以流内幂等键
  `seal_id` 与乐观版本号 `expected_next_sequence` 封存一个**无暂存事件、版本匹配**
  的流：在同一个 `BEGIN IMMEDIATE` 事务内按 sequence 升序把每条
  `[sequence,event_id,delta]` 编码为**保留 Unicode、无空白**的紧凑 UTF-8 JSON，
  以换行拼接后计算 SHA-256 并写入封存记录。封存不可逆：封存后普通提交、转账（任一侧）
  与新撤销都会被拒绝（`409 stream_sealed`），但这些操作在封存**之前已完成**的幂等
  重放仍返回原结果；封存流仍可查询、消费（claim/ack）并作为 fork 源，fork 出来的
  新流不受影响、可继续追加。相同请求重试返回原封存结果（`200`），参数冲突返回
  `409`，并发重试只封存一次。
- **事件历史快照导出（snapshot export）**：`GET /streams/{stream_id}/events?limit=...&cursor=...`
  首请求在**一次读事务**中固定当前 `next_sequence` 作为快照上界
  `snapshot_next_sequence`，之后按 sequence 升序分页返回 `[0, snapshot_next_sequence)`
  的已应用事件与该版本余额；后续请求凭 `next_cursor` 继续**同一个快照**——并发追加的
  新事件不会混入、不漏项、不重复，末页 `next_cursor=null`。游标为
  `base64url(规范JSON).base64url(HMAC-SHA256)`，绑定 `stream_id`、快照上界与下一序号；
  签名损坏、跨流使用或字段非法返回 `400`，流不存在返回 `404`。签名密钥从环境变量
  `SNAPSHOT_CURSOR_SECRET` 读取（Compose 已提供可用默认值）。
- 无外部账号、无外部依赖，`docker compose up --build` 一条命令启动。

## 一键启动

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `events-data`（容器内 `/data`）中，
容器重启不丢数据。

健康检查：

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>

> 也可不使用 Docker 本地运行：
> `pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 接口

### 1. 提交事件

```
POST /streams/{stream_id}/events
Content-Type: application/json
```

请求体：

| 字段       | 类型   | 约束                  | 说明                       |
| ---------- | ------ | --------------------- | -------------------------- |
| `event_id` | string | 非空，流内唯一        | 客户端分配的幂等键         |
| `sequence` | int    | `>= 0`，流内唯一      | 从 0 开始的单调递增序号    |
| `delta`    | int    | 整数                  | 事件应用时累加到余额的值   |

响应：

- `201 Created`：事件首次写入；
- `200 OK`：幂等重放（`replayed: true`）；
- `409 Conflict`：同一 `event_id` 内容不一致（`replay_conflict`），或
  `sequence` 已被其他 `event_id` 占用（`sequence_conflict`）；
- `422 Unprocessable Entity`：请求体校验失败。

### 2. 查询流状态

```
GET /streams/{stream_id}
```

```json
{
  "stream_id": "acct-1",
  "balance": 250,
  "next_sequence": 2,
  "pending_sequences": [3, 5]
}
```

- `balance`：已连续应用事件的累加余额；
- `next_sequence`：下一个可应用的序号（即连续前缀的长度）；
- `pending_sequences`：已暂存、但因缺号尚未应用的序号（升序）。

流不存在时返回 `404`。

### 3. 消费者领取事件（claim）

```
POST /streams/{stream_id}/consumers/{consumer_id}/claim
Content-Type: application/json
```

请求体：

| 字段        | 类型 | 约束              | 说明                                   |
| ----------- | ---- | ----------------- | -------------------------------------- |
| `max_events` | int  | `1 <= max_events <= 10000` | 本批最多返回多少事件                   |

从该消费者**游标**开始，返回已应用（applied）且连续的事件，最多 `max_events`
条；同时生成一个带 `expires_at`（POSIX 毫秒）的租约 `lease_token`。

- 租约**未到期**时重复领取（`consumer_id` 相同）始终返回同一批次、同一 token、
  同一 `expires_at`，忽略新的 `max_events`；
- 没有可消费事件时返回空批次（`events: []`，`from_sequence == to_sequence`），
  但同样发放租约；
- 游标之后若有事件尚未应用（缺号），本批只包含连续已应用部分；
- 流不存在返回 `404`，参数非法返回 `422`。

```json
{
  "stream_id": "acct-1",
  "consumer_id": "worker-a",
  "lease_token": "dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
  "expires_at": 1789676192280,
  "from_sequence": 0,
  "to_sequence": 2,
  "events": [
    {"event_id": "e0", "sequence": 0, "delta": 100},
    {"event_id": "e1", "sequence": 1, "delta": 200}
  ]
}
```

### 4. 消费者确认批次（ack）

```
POST /streams/{stream_id}/consumers/{consumer_id}/ack
Content-Type: application/json
```

请求体：`{"lease_token": "<claim 返回的 token>"}`。

只有 token **匹配当前未过期租约**时，才在同一个事务内把游标原子推进到
`to_sequence` 并清除租约：

```json
{"status": "ok", "stream_id": "acct-1", "consumer_id": "worker-a", "cursor": 2}
```

- `409 Conflict` 的情形：
  - `lease_not_active`：该消费者当前没有有效租约（尚未 claim，或已 ack）；
  - `lease_token_mismatch`：token 与当前租约不符（旧租约的 token、伪造 token）；
  - `lease_expired`：token 匹配的租约已过期。
- 租约到期后 token 失效（ack 返回 `409`），同批事件可被再次领取，新领取会得到
  新 token；
- 流不存在返回 `404`，请求体缺少 `lease_token` 返回 `422`。

不同 `consumer_id` 的游标与租约彼此独立；游标和租约都持久化在 SQLite 中。

### 5. 消费者拒绝批次（nack）与死信

```
POST /streams/{stream_id}/consumers/{consumer_id}/nack
Content-Type: application/json
```

请求体：

| 字段          | 类型   | 约束   | 说明                                   |
| ------------- | ------ | ------ | -------------------------------------- |
| `lease_token` | string | 非空   | claim 返回的租约 token                 |
| `reason`      | string | 非空白 | 本次拒绝的原因（不能是空串或纯空白）   |

对**当前未过期且非空**的批次 nack 时，服务端在**同一个 `BEGIN IMMEDIATE`
事务**内：

1. **不推进游标**，对批次的**第一个事件**（`from_sequence`）累加一次连续失败
   次数（`consumer_failures`），记录最后原因；
2. **立即释放租约**（清空租约列）——不必等待租约到期，客户端马上可以重新 claim；
3. 若失败次数**未达** `CONSUMER_MAX_ATTEMPTS`（默认 `3`）：游标保持不动，下次
   claim 仍从**原游标**返回同一个首事件；
4. 若失败次数**达到阈值**：删除该事件的失败计数，写入一条**死信记录**
   （`dead_letters`：事件坐标与 `delta` 快照、失败次数、最后原因、隔离时间），
   并把游标**恰好推进到 `head_sequence + 1`**——同批的其余事件未被确认，之后的
   claim 从下一序号继续返回它们。

死信判定与游标推进在同一事务提交，因此"已隔离的事件绝不会再被任何 claim 发出"
与"失败计数恰好达到阈值"都由写锁串行保证，多实例并发 nack 也只会结算一次。

```json
{
  "status": "ok",
  "stream_id": "acct-1",
  "consumer_id": "worker-a",
  "lease_token": "dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
  "cursor": 0,
  "attempts": 1,
  "max_attempts": 3,
  "dead_lettered": false,
  "reason": "payment failed",
  "replayed": false
}
```

- `cursor`：本次 nack 后的游标——重试期间保持不变；第 3 次（阈值）nack 时变为
  `head_sequence + 1`；
- `attempts`：批次首事件当前的**连续** nack 次数；`max_attempts` 为本次采用的阈值；
- `dead_lettered`：本次 nack 是否把首事件送入死信；
- `replayed`：同一已结算 token 的幂等重试返回 `true`，其余字段为**首次结算时的
  原结果**（原 `cursor`/`attempts`/`reason`/`dead_lettered`），**不重复计数**。

连续失败次数的语义：

- 只统计批次**首事件**——每次重新 claim 都从原游标开始，卡住的首事件会反复成为
  批次首事件；批次中的后续事件在首事件被 ack 或死信越过之前不会被计数；
- **ack 成功**会把游标越过的事件的失败计数全部清除（故障恢复后重新计数）；
- 首事件被死信越过后其计数行一并删除，计数只存在于仍在重试的事件上。

错误与边界：

- `409 Conflict`：
  - `lease_not_active`：当前没有有效租约（尚未 claim，或已 ack/nack），且 token
    不是最近一次已结算的 nack token；
  - `lease_token_mismatch`：token 与当前租约不符（伪造 token、旧租约 token、
    **已 nack 后又 claim 到新租约**再拿旧 token 重试）；
  - `lease_expired`：token 匹配的租约已过期（不计数、不释放，需重新 claim）；
  - `empty_batch_not_nackable`：空批次（`from_sequence == to_sequence`）没有可
    拒绝的事件；租约保持有效，仍可正常 ack；
- 同一 token 在**新租约产生之前**重试：返回首次结算的原结果（`replayed: true`，
  HTTP `200`），不重复计数、不产生第二条死信；
- `422 Unprocessable Entity`：缺少 `lease_token`/`reason`，或 `reason` 为空白；
- `404 Not Found`：流不存在（`stream_not_found`）。

不同 `consumer_id` 的失败计数与死信相互独立：一个消费者把某事件死信，不影响其他
消费者继续 claim/ack 同一事件。

### 6. 查询死信记录（dead letters）

```
GET /streams/{stream_id}/consumers/{consumer_id}/dead-letters
```

返回该消费者已隔离的全部死信，**按 `sequence` 升序**：

```json
{
  "stream_id": "acct-1",
  "consumer_id": "worker-a",
  "dead_letters": [
    {
      "stream_id": "acct-1",
      "consumer_id": "worker-a",
      "sequence": 0,
      "event_id": "e0",
      "delta": 100,
      "attempts": 3,
      "last_reason": "payment failed",
      "dead_lettered_at": 1789676192280
    }
  ]
}
```

| 字段               | 说明                                                       |
| ------------------ | ---------------------------------------------------------- |
| `sequence`         | 被隔离事件的序号（游标越过它之后从下一序号继续）           |
| `event_id` / `delta` | 死信时从不可变的已应用事件复制的事件内容                 |
| `attempts`         | 隔离时累计的连续 nack 次数（即 `CONSUMER_MAX_ATTEMPTS`）   |
| `last_reason`      | 最后一次（达到阈值那次）nack 提交的非空原因                |
| `dead_lettered_at` | 隔离时间，POSIX 毫秒                                       |

- 流不存在返回 `404 stream_not_found`；消费者从未 claim 过时返回 `dead_letters: []`；
- 死信是**只追加**记录：游标不会回退，每个 `(stream, consumer, sequence)` 至多一条。

### 7. 分叉流（fork）

```
POST /streams/{stream_id}/forks
Content-Type: application/json
```

请求体：

| 字段               | 类型   | 约束                                   | 说明                                   |
| ------------------ | ------ | -------------------------------------- | -------------------------------------- |
| `fork_id`          | string | 非空，源流内唯一                       | 客户端分配的幂等键（重试键）           |
| `target_stream_id` | string | 非空，必须是尚不存在的流               | 分叉创建的新流                         |
| `through_sequence` | int    | `>= 0` 且 `<=` 源流的 `next_sequence`  | 独占上界，复制 `[0, through_sequence)` |

把源流中序号位于 `[0, through_sequence)` 的**已应用**事件（`applied=1`）原样复制
到目标流：目标流的事件 `applied=1`，`next_sequence = through_sequence`，
`balance` 为这些事件 `delta` 之和。因缺号而暂存的事件（`pending_sequences`）不在
连续前缀内，不会被复制。目标流创建后即可用普通的提交事件接口从
`through_sequence` 继续追加。

```json
{
  "source_stream_id": "acct-1",
  "target_stream_id": "acct-1-backup",
  "through_sequence": 2,
  "balance": 300,
  "next_sequence": 2
}
```

- `201 Created`：分叉首次创建；
- `200 OK`：同一 `(源流, fork_id)` 携带完全相同参数的幂等重试，返回既有分叉；
- `409 Conflict`：
  - `target_stream_exists`：目标流已存在（含其他分叉已占用该 `target_stream_id`）；
  - `fork_replay_conflict`：同一 `fork_id` 重试时 `target_stream_id` 或
    `through_sequence` 与首次不同；
- `422 Unprocessable Entity`：`through_sequence` 超出 `[0, next_sequence]`
  （`through_sequence_out_of_range`），或请求体字段校验失败；
- `404 Not Found`：源流不存在（`stream_not_found`），此时不会创建任何目标流。

分叉记录、目标流行与复制的事件在**同一个 `BEGIN IMMEDIATE` 事务**内写入：并发的
相同 `fork_id` 请求只有一个创建分叉，其余全部拿到既有结果；并发的不同请求若争抢
同一目标流，只有一个成功，其余得到 `409`，数据库中不会残留半成品目标流。

### 8. 跨事件流原子转账（transfer）

```
POST /transfers
Content-Type: application/json
```

请求体：

| 字段               | 类型   | 约束                       | 说明                             |
| ------------------ | ------ | -------------------------- | -------------------------------- |
| `transfer_id`      | string | 非空，全局唯一             | 客户端分配的全局幂等键（重试键） |
| `source_stream_id` | string | 非空，必须是**已存在**的流 | 扣款流                           |
| `target_stream_id` | string | 非空，必须是**已存在**的流 | 入账流，且必须与源流不同         |
| `amount`           | int    | 正整数（`> 0`）            | 转账金额                         |

转账在**同一个 `BEGIN IMMEDIATE` 事务**内完成：

1. 在源流当前 `next_sequence` 追加一条扣款事件（`delta = -amount`，事件 id 为
   `transfer:<transfer_id>:debit`）；
2. 在目标流当前 `next_sequence` 追加一条入账事件（`delta = +amount`，事件 id 为
   `transfer:<transfer_id>:credit`）；
3. 两侧分别执行普通的连续追赶（catch-up）——若追加位置之前还有因缺号暂存的事件链，
   这次追加也会把它们一并连续应用；
4. 更新两侧的 `balance`/`next_sequence` 并写入 `transfers` 转账记录，统一提交。

任一步失败（流不存在、余额不足、约束冲突等）整体回滚，**不会留下任何事件或余额
变化**。转账事件就是普通事件：随后可以通过 `GET /streams/{id}`、消费 claim/ack、
fork 等既有接口正常观察和处理。

- `201 Created`：转账首次提交；
- `200 OK`：相同 `transfer_id` 携带**完全相同参数**的幂等重试，返回原扣款/入账
  事件序号与两侧**最新**流状态（重试不会再次扣款）；
- `409 Conflict`：`transfer_replay_conflict`——相同 `transfer_id` 重试时源流、
  目标流或 `amount` 与首次不一致；
- `422 Unprocessable Entity`：`insufficient_funds`——源流当前 `balance`（连续前缀
  余额，不含暂存事件）小于 `amount`；或请求体字段校验失败（金额非正整数、源流与
  目标流相同等）；
- `404 Not Found`：`source_stream_not_found` / `target_stream_not_found`——对应流
  不存在（转账不会自动创建流）。

并发的相同 `transfer_id` 请求在写锁上串行，数据库中只有一条 `transfers` 记录、
两侧各只有一条转账事件：恰好一个请求得到 `201`，其余全部拿到 `200` 原结果。

```json
{
  "status": "ok",
  "replayed": false,
  "transfer_id": "txn-1",
  "amount": 50,
  "source": {
    "stream_id": "acct-1",
    "event_id": "transfer:txn-1:debit",
    "sequence": 3,
    "state": {"stream_id": "acct-1", "balance": 50, "next_sequence": 4, "pending_sequences": []}
  },
  "target": {
    "stream_id": "acct-2",
    "event_id": "transfer:txn-1:credit",
    "sequence": 1,
    "state": {"stream_id": "acct-2", "balance": 50, "next_sequence": 2, "pending_sequences": []}
  }
}
```

### 9. 撤销已应用事件（reversal）

```
POST /streams/{stream_id}/events/{event_id}/reversals
Content-Type: application/json
```

请求体：

| 字段          | 类型   | 约束             | 说明                             |
| ------------- | ------ | ---------------- | -------------------------------- |
| `reversal_id` | string | 非空，全局唯一   | 客户端分配的全局幂等键（重试键） |

仅允许撤销该流中**已应用（`applied=1`）且尚未被撤销**的事件。撤销在**同一个
`BEGIN IMMEDIATE` 事务**内完成：

1. 在流当前 `next_sequence` 追加一条补偿事件（`event_id=reversal:{reversal_id}`，
   `delta=-原事件 delta`，`applied=0`）；
2. 执行普通的连续追赶（catch-up）——补偿事件紧邻连续前缀，必定立即应用；若其
   之后还有因缺号暂存的事件链，这次追加也会把它们一并连续应用；
3. 更新 `balance`/`next_sequence` 并写入 `reversals` 撤销记录，统一提交。

任一步失败整体回滚，**不会留下任何事件、余额变化或撤销记录**。补偿事件就是
普通事件：随后可以通过 `GET /streams/{id}`、消费 claim/ack、fork 等既有接口
正常观察和处理。

- `201 Created`：撤销首次提交；
- `200 OK`：相同 `reversal_id` 对**相同目标**（同一 `stream_id` + `event_id`）
  的幂等重试，返回原补偿事件序号与流的**最新**状态（重试不会再次补偿）；
- `409 Conflict`：
  - `reversal_replay_conflict`：相同 `reversal_id` 重试时目标流/事件与首次不同；
  - `event_already_reversed`：该事件已被其他 `reversal_id` 撤销过（每个事件
    至多撤销一次）；
  - `event_not_applied`：目标事件仍因缺号暂存、尚未应用，没有可补偿的效果；
- `404 Not Found`：`stream_not_found` / `event_not_found`——流或事件不存在；
- `422 Unprocessable Entity`：请求体校验失败（`reversal_id` 缺失或为空白）。

并发的相同 `reversal_id` 请求在写锁上串行：数据库中只有一条 `reversals` 记录、
一条补偿事件，恰好一个请求得到 `201`，其余全部拿到 `200` 原结果；并发的不同
`reversal_id` 撤销同一事件时，也只有一个成功，其余得到
`409 event_already_reversed`，不会重复补偿。

```json
{
  "status": "ok",
  "replayed": false,
  "reversal_id": "rev-1",
  "stream_id": "acct-1",
  "event_id": "e0",
  "reversal_event_id": "reversal:rev-1",
  "sequence": 3,
  "delta": -100,
  "stream": {"stream_id": "acct-1", "balance": 250, "next_sequence": 4, "pending_sequences": []}
}
```

### 10. 不可逆封存流（seal）

```
POST /streams/{stream_id}/seal
Content-Type: application/json
```

请求体：

| 字段                      | 类型   | 约束           | 说明                                     |
| ------------------------- | ------ | -------------- | ---------------------------------------- |
| `seal_id`                 | string | 非空，流内唯一 | 客户端分配的幂等键（重试键）             |
| `expected_next_sequence`  | int    | `>= 0`         | 乐观版本号，必须等于流当前 `next_sequence` |

仅当**流存在、没有暂存（pending）事件且 `expected_next_sequence` 与流当前版本一致**
时，才在**同一个 `BEGIN IMMEDIATE` 事务**内完成封存：

1. 按 `sequence` 升序读取全部已应用事件（已应用事件恰好占据
   `[0, next_sequence)`）；
2. 每条事件编码为一行 `[sequence,event_id,delta]` 的**规范 JSON**：使用
   `json.dumps(..., ensure_ascii=False, separators=(",", ":"))`——
   **不转义非 ASCII 字符**（Unicode 原样保留）、**不含任何多余空白**、键（数组）
   顺序固定，并以 UTF-8 编码；
3. 各行以 `\n` 拼接（**行之间一个换行，末尾无换行**），对拼接字节计算 SHA-256；
   空流（0 条事件）的哈希即空输入的 SHA-256
   （`e3b0c442...b855`）；
4. 写入 `seals` 封存记录（`seal_id`、封存版本、事件数、SHA-256），统一提交。

封存**不可逆**。封存后：

- 会向该流新增事件的操作一律拒绝：普通事件提交、以该流为源流或目标流的**新**转账、
  针对该流的**新**撤销，均返回 `409 stream_sealed`，不写入任何数据；
- 这些操作在封存**之前已完成**的幂等重放（相同 `event_id` / `transfer_id` /
  `reversal_id`）仍返回**原结果**（`200`），不会重复追加事件；
- 流仍可 `GET` 查询、消费者 claim/ack 正常消费，并可作为 fork 源整体复制；
  fork 出来的目标流是独立的新流，**不继承封存状态**，可继续追加事件。

- `201 Created`：首次封存成功；
- `200 OK`：相同 `seal_id` 与相同 `expected_next_sequence` 的幂等重试，返回原封存
  记录（相同 SHA-256）；
- `409 Conflict`：
  - `version_mismatch`：流当前 `next_sequence` 与 `expected_next_sequence` 不一致
    （客户端版本过期），失败不会消耗 `seal_id`；
  - `stream_has_pending_events`：流中仍有因缺号暂存、尚未应用的事件，需先补齐
    缺号再封存；
  - `seal_replay_conflict`：流已封存，但重试的 `seal_id` 或
    `expected_next_sequence` 与原封存不同；
  - `stream_sealed`：对已封存流发起任何新增事件操作（提交/转账/撤销）；
- `404 Not Found`：`stream_not_found`——流不存在（封存不会自动建流）；
- `422 Unprocessable Entity`：请求体字段校验失败。

并发的相同封存请求在写锁上串行：恰好一个请求得到 `201`，其余全部拿到 `200`
原结果；`seals` 表以 `stream_id` 为主键，保证每流至多一条封存记录。此外数据库层
的 `BEFORE INSERT` 触发器也会阻止向已封存流直接插入事件（纵深防御）。

```json
{
  "status": "ok",
  "replayed": false,
  "stream_id": "acct-1",
  "seal_id": "seal-1",
  "next_sequence": 3,
  "event_count": 3,
  "sha256": "f3d08fa746c6fea7fbc20dcbd239518bec04b1224b5fcb406443ae15d8cd359b"
}
```

### 11. 事件历史快照导出（snapshot export）

```
GET /streams/{stream_id}/events?limit=...&cursor=...
```

查询参数：

| 参数     | 类型   | 约束                     | 说明                                       |
| -------- | ------ | ------------------------ | ------------------------------------------ |
| `limit`  | int    | `1 <= limit <= 1000`，缺省 `100` | 每页最多返回多少条事件               |
| `cursor` | string | 可选                     | 上一页返回的 `next_cursor`；缺省表示开启新快照 |

**首请求（不带 `cursor`）**在**一次读事务**中固定流当前的 `next_sequence` 作为快照
上界 `snapshot_next_sequence`，并返回第一页；**后续请求**携带上一页的 `next_cursor`，
继续读取**同一个快照**。每一页返回：

```json
{
  "stream_id": "acct-1",
  "snapshot_next_sequence": 5,
  "balance": 150,
  "events": [
    {"event_id": "e0", "sequence": 0, "delta": 10},
    {"event_id": "e1", "sequence": 1, "delta": 20}
  ],
  "next_cursor": "eyJuZXh0X3NlcXVlbmNlIjoyLCJzbmFwc2hvdF9uZXh0X3NlcXVlbmNlIjo1LCJzdHJlYW1faWQiOiJhY2N0LTEifQ==.MEJ7rPpRz_q6G_yzlQefOS8dVoXtpdTEcdXuiTPD1O8="
}
```

- `snapshot_next_sequence`：首请求固定的快照上界（独占），整次导出各页保持一致；
- `balance`：**快照版本**的余额，即 `[0, snapshot_next_sequence)` 已应用事件的
  `delta` 之和——导出期间流继续追加事件也不会改变它；
- `events`：序号位于 `[起点, snapshot_next_sequence)` 的**已应用**事件，按
  `sequence` 升序，至多 `limit` 条；因缺号暂存（pending）的事件不会被导出；
- `next_cursor`：下一页的游标；**末页为 `null`**，导出结束。

快照一致性：已应用事件一旦进入 `[0, snapshot_next_sequence)` 就不再变化，因此凭
游标翻页时，**并发追加的新事件不会混入**本次快照，已有事件**不漏项、不重复**；
重复使用同一游标返回相同的一页（幂等翻页）。需要更新数据时，重新发起不带
`cursor` 的请求即可固定一个新快照。

**游标格式**：`<base64url(payload)>.<base64url(signature)>`。`payload` 是
`{"next_sequence":M,"snapshot_next_sequence":N,"stream_id":"..."}` 的**规范
JSON**（键序固定、紧凑分隔符无空白、UTF-8、非 ASCII 字符原样保留）；`signature`
是以 `SNAPSHOT_CURSOR_SECRET` 为密钥对 payload 计算的 **HMAC-SHA256**。游标绑定
`stream_id`、快照上界与下一序号三个字段。

- `400 Bad Request`：
  - `invalid_cursor`：游标结构非法、Base64 解码失败、**签名损坏/不符**、payload
    非 JSON 对象、字段缺失/多余/类型非法（如序号越界、布尔值、负数）；
  - `cursor_stream_mismatch`：**跨流使用**——游标是其他流签发的；
- `404 Not Found`：`stream_not_found`——流不存在（无论是否携带游标）；
- `422 Unprocessable Entity`：`limit` 越界或不是整数。

## 工作机制示例

```bash
# 序号 1 先到：缺 0，暂存，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","sequence":1,"delta":200}'
# 201 -> balance=0, next_sequence=0, pending=[1]

# 序号 2 再到：仍然暂存
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e2","sequence":2,"delta":50}'
# 201 -> balance=0, next_sequence=0, pending=[1,2]

# 序号 0 到达：同一事务内自动追赶 0 -> 1 -> 2
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 201 -> balance=350, next_sequence=3, pending=[]

# 相同 event_id 重放：幂等，返回 200，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 200 -> replayed=true, balance=350, next_sequence=3

# 同一 event_id 内容不一致：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":999}'
# HTTP/1.1 409
# {"detail":{"code":"replay_conflict","message":"event_id already exists with different sequence/delta"}}

# 序号 0 被另一个 event_id 占用：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"other","sequence":0,"delta":100}'
# HTTP/1.1 409, code=sequence_conflict

# 查询
curl -s localhost:8000/streams/acct-1
```

## 消费游标示例（claim / ack）

```bash
# 先准备三个已应用事件（接上文，acct-1 的 next_sequence=3）
# 消费者 worker-a 领取最多 2 个事件
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' \
  -d '{"max_events":2}'
# {
#   "stream_id":"acct-1","consumer_id":"worker-a",
#   "lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi",
#   "expires_at":1789676192280,"from_sequence":0,"to_sequence":2,
#   "events":[{"event_id":"e0","sequence":0,"delta":100},
#             {"event_id":"e1","sequence":1,"delta":200}]
# }

# 租约未到期重复领取：同一批次、同一 token、同一 expires_at
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":99}'
# -> lease_token 不变，events 仍是 [e0, e1]

# 用错误/旧 token 确认：409
curl -i -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' -d '{"lease_token":"bogus"}'
# HTTP/1.1 409 -> {"detail":{"code":"lease_token_mismatch", ...}}

# 用有效 token 确认：游标原子推进到 2
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"dJQ8DuW6F_uuBYrN1OeG2TtsVCuLivSi"}'
# {"status":"ok","stream_id":"acct-1","consumer_id":"worker-a","cursor":2}

# 再次领取：从游标 2 继续
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events=[e2]，from_sequence=2, to_sequence=3

# 租约到期后再用旧 token 确认：409 lease_expired；
# 重新 claim 可再次拿到同批事件（新 token）。
```

## 消费拒绝与死信示例（nack / dead letters）

```bash
# 准备 3 个已应用事件：e0(0,+10) e1(1,+20) e2(2,+30)
for i in 0 1 2; do
  curl -s -o /dev/null -X POST localhost:8000/streams/acct-dl/events \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"e$i\",\"sequence\":$i,\"delta\":$(( (i+1)*10 ))}"
done

# 第 1 次 nack：游标不动，租约立即释放，attempts=1
TOKEN=$(curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/nack \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$TOKEN\",\"reason\":\"attempt 1 failed\"}"
# {"status":"ok",...,"cursor":0,"attempts":1,"max_attempts":3,
#  "dead_lettered":false,"reason":"attempt 1 failed","replayed":false}

# 同一 token 立即重试：返回原结果，replayed=true，不重复计数
curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/nack \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$TOKEN\",\"reason\":\"ignored retry\"}"
# -> "replayed":true, "attempts":1, "reason":"attempt 1 failed"

# 下次 claim 仍从原游标返回首事件 e0（新 token）；再 nack 两次达到阈值
for n in 2 3; do
  T=$(curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/claim \
    -H 'Content-Type: application/json' -d '{"max_events":10}' \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["lease_token"])')
  curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/nack \
    -H 'Content-Type: application/json' \
    -d "{\"lease_token\":\"$T\",\"reason\":\"attempt $n failed\"}"
done
# 第 3 次："attempts":3,"dead_lettered":true,"cursor":1 —— 死信写入且游标越过 e0

# 后续 claim 从序号 1 继续（e0 永不返回）
curl -s -X POST localhost:8000/streams/acct-dl/consumers/worker/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events=[e1, e2]，from_sequence=1

# 查询死信：按 sequence 升序，含事件内容、失败次数、最后原因、隔离时间
curl -s localhost:8000/streams/acct-dl/consumers/worker/dead-letters
# {"stream_id":"acct-dl","consumer_id":"worker","dead_letters":[
#   {"stream_id":"acct-dl","consumer_id":"worker","sequence":0,"event_id":"e0",
#    "delta":10,"attempts":3,"last_reason":"attempt 3 failed",
#    "dead_lettered_at":1789676192280}]}

# 空批次不可 nack：409 empty_batch_not_nackable（租约仍可 ack）
curl -s -X POST localhost:8000/streams/acct-empty/consumers/worker/claim \
  -H 'Content-Type: application/json' -d '{"max_events":5}'   # 无已应用事件
curl -s -i -X POST localhost:8000/streams/acct-empty/consumers/worker/nack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"<空批次 token>","reason":"nothing"}'
# HTTP/1.1 409 -> {"detail":{"code":"empty_batch_not_nackable", ...}}

# 伪造/旧 token：409 lease_token_mismatch；过期 token：409 lease_expired
curl -s -i -X POST localhost:8000/streams/acct-dl/consumers/worker/nack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"bogus","reason":"x"}'
# HTTP/1.1 409 -> {"detail":{"code":"lease_token_mismatch", ...}}

# 阈值可由环境变量调整：CONSUMER_MAX_ATTEMPTS=1 时首次 nack 即隔离
```

## 流分叉示例（fork）

```bash
# 接上文：acct-1 已有 3 个已应用事件（balance=350, next_sequence=3）。
# 把 [0, 2) 的已应用事件复制到新流 acct-1-backup：
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":2}'
# 201:
# {"source_stream_id":"acct-1","target_stream_id":"acct-1-backup",
#  "through_sequence":2,"balance":300,"next_sequence":2}

# 目标流可以继续追加事件（序号从 next_sequence=2 开始）
curl -s -X POST localhost:8000/streams/acct-1-backup/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"b-e2","sequence":2,"delta":10}'
# 201 -> balance=310, next_sequence=3

# 相同 fork_id + 相同参数重试：幂等，返回 200 与既有分叉（不会重复复制事件）
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":2}'
# HTTP/1.1 200 OK

# 相同 fork_id 但参数不同：409 fork_replay_conflict
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"daily-backup","target_stream_id":"acct-1-backup","through_sequence":3}'
# HTTP/1.1 409

# 目标流已存在：409 target_stream_exists
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"another","target_stream_id":"acct-1-backup","through_sequence":1}'
# HTTP/1.1 409

# 上界超过源流的 next_sequence：422 through_sequence_out_of_range
curl -s -i -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"oob","target_stream_id":"acct-1-oob","through_sequence":99}'
# HTTP/1.1 422

# 源流不存在：404，且不会创建目标流
curl -s -i -X POST localhost:8000/streams/unknown/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"f","target_stream_id":"ghost","through_sequence":0}'
# HTTP/1.1 404

# through_sequence=0：创建一个空的、可从头追加的目标流
curl -s -X POST localhost:8000/streams/acct-1/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"empty","target_stream_id":"acct-1-empty","through_sequence":0}'
# {"...","through_sequence":0,"balance":0,"next_sequence":0}
```

## 跨事件流转账示例（transfer）

```bash
# 准备两个已存在的流（沿用前文 acct-1：balance=350, next_sequence=3）
curl -s -X POST localhost:8000/streams/acct-2/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":0}'
# 201 -> acct-2 balance=0, next_sequence=1

# 从 acct-1 向 acct-2 转账 50：两侧各自在当前 next_sequence 追加事件
curl -s -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":50}'
# 201:
# {"status":"ok","replayed":false,"transfer_id":"txn-1","amount":50,
#  "source":{"stream_id":"acct-1","event_id":"transfer:txn-1:debit",
#            "sequence":3,
#            "state":{"stream_id":"acct-1","balance":300,"next_sequence":4,"pending_sequences":[]}},
#  "target":{"stream_id":"acct-2","event_id":"transfer:txn-1:credit",
#            "sequence":1,
#            "state":{"stream_id":"acct-2","balance":50,"next_sequence":2,"pending_sequences":[]}}}

# 相同 transfer_id + 相同参数重试：幂等返回 200 与原事件序号，不重复扣款
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":50}'
# HTTP/1.1 200 OK -> replayed=true, source.sequence 仍为 3, target.sequence 仍为 1

# 相同 transfer_id 但金额/流不一致：409 transfer_replay_conflict
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-1","source_stream_id":"acct-1","target_stream_id":"acct-2","amount":99}'
# HTTP/1.1 409

# 余额不足：422 insufficient_funds（事务整体回滚，两侧均无变化）
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-poor","source_stream_id":"acct-2","target_stream_id":"acct-1","amount":51}'
# HTTP/1.1 422

# 流不存在：404（转账不会自动建流）
curl -s -i -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"txn-404","source_stream_id":"acct-1","target_stream_id":"ghost","amount":1}'
# HTTP/1.1 404 -> {"detail":{"code":"target_stream_not_found", ...}}

# 源流与目标流相同 / amount 非正整数：422 参数校验失败
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"bad","source_stream_id":"acct-1","target_stream_id":"acct-1","amount":1}'
# 422

# 转账事件与普通事件无异：消费者可以领取到扣款/入账事件
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-a/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events 最后一项为 {"event_id":"transfer:txn-1:debit","sequence":3,"delta":-50}
```

## 事件撤销示例（reversal）

```bash
# 接上文：acct-1 当前 balance=300, next_sequence=4
# （e0=100, e1=200, e2=50 已应用，transfer:txn-1:debit=-50 已应用）。
# 撤销事件 e2（delta=50）：在 next_sequence=4 追加补偿事件 reversal:rev-1（delta=-50）
curl -s -X POST localhost:8000/streams/acct-1/events/e2/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-1"}'
# 201:
# {"status":"ok","replayed":false,"reversal_id":"rev-1","stream_id":"acct-1",
#  "event_id":"e2","reversal_event_id":"reversal:rev-1","sequence":4,"delta":-50,
#  "stream":{"stream_id":"acct-1","balance":250,"next_sequence":5,"pending_sequences":[]}}

# 相同 reversal_id + 相同目标重试：幂等返回 200 与最新状态，不重复补偿
curl -s -i -X POST localhost:8000/streams/acct-1/events/e2/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-1"}'
# HTTP/1.1 200 OK -> replayed=true, sequence 仍为 4, balance 仍为 250

# 相同 reversal_id 但目标不同：409 reversal_replay_conflict
curl -s -i -X POST localhost:8000/streams/acct-1/events/e1/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-1"}'
# HTTP/1.1 409

# 同一事件换一个 reversal_id 再次撤销：409 event_already_reversed
curl -s -i -X POST localhost:8000/streams/acct-1/events/e2/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-2"}'
# HTTP/1.1 409

# 撤销因缺号而暂存、尚未应用的事件：409 event_not_applied
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e7","sequence":7,"delta":1}'   # 缺 5、6，e7 暂存
curl -s -i -X POST localhost:8000/streams/acct-1/events/e7/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-3"}'
# HTTP/1.1 409 -> {"detail":{"code":"event_not_applied", ...}}

# 流或事件不存在：404
curl -s -i -X POST localhost:8000/streams/ghost/events/e0/reversals \
  -H 'Content-Type: application/json' \
  -d '{"reversal_id":"rev-4"}'
# HTTP/1.1 404 -> {"detail":{"code":"stream_not_found", ...}}

# 补偿事件与普通事件无异：消费者可以领取到 reversal:rev-1
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-b/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
# -> events 中包含 {"event_id":"reversal:rev-1","sequence":4,"delta":-50}
```

## 流封存示例（seal）

```bash
# 准备一个无缺号、无暂存事件的流（balance=130, next_sequence=3）
curl -s -X POST localhost:8000/streams/acct-9/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
curl -s -X POST localhost:8000/streams/acct-9/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","sequence":1,"delta":-30}'
curl -s -X POST localhost:8000/streams/acct-9/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e2","sequence":2,"delta":60}'

# 在版本 3 封存：对 [0,"e0",100]\n[1,"e1",-30]\n[2,"e2",60] 计算 SHA-256
curl -s -X POST localhost:8000/streams/acct-9/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"seal-1","expected_next_sequence":3}'
# 201:
# {"status":"ok","replayed":false,"stream_id":"acct-9","seal_id":"seal-1",
#  "next_sequence":3,"event_count":3,
#  "sha256":"<按规范 JSON 行拼接得到的 SHA-256>"}

# 相同请求重试：幂等返回 200 与同一 SHA-256
curl -s -i -X POST localhost:8000/streams/acct-9/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"seal-1","expected_next_sequence":3}'
# HTTP/1.1 200 OK

# 换 seal_id 或版本号：409 seal_replay_conflict
curl -s -i -X POST localhost:8000/streams/acct-9/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"seal-2","expected_next_sequence":3}'
# HTTP/1.1 409

# 封存后新增普通事件：409 stream_sealed
curl -s -i -X POST localhost:8000/streams/acct-9/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e3","sequence":3,"delta":1}'
# HTTP/1.1 409 -> {"detail":{"code":"stream_sealed", ...}}

# 封存前已提交事件的幂等重放：仍返回 200 原结果
curl -s -i -X POST localhost:8000/streams/acct-9/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# HTTP/1.1 200 OK -> replayed=true

# 查询、消费、fork 源一切正常
curl -s localhost:8000/streams/acct-9
curl -s -X POST localhost:8000/streams/acct-9/consumers/w/claim \
  -H 'Content-Type: application/json' -d '{"max_events":10}'
curl -s -X POST localhost:8000/streams/acct-9/forks \
  -H 'Content-Type: application/json' \
  -d '{"fork_id":"f1","target_stream_id":"acct-9-copy","through_sequence":3}'
# fork 目标是独立的新流，不继承封存，可继续追加

# 版本过期：实际 next_sequence 与 expected_next_sequence 不符 -> 409 version_mismatch
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"s0","sequence":0,"delta":1}'
curl -s -i -X POST localhost:8000/streams/acct-1/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"seal-x","expected_next_sequence":99}'
# HTTP/1.1 409

# 仍有缺号暂存事件：409 stream_has_pending_events
curl -s -X POST localhost:8000/streams/acct-g/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"g1","sequence":1,"delta":1}'   # 缺 0，暂存
curl -s -i -X POST localhost:8000/streams/acct-g/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"seal-g","expected_next_sequence":0}'
# HTTP/1.1 409

# 流不存在：404（封存不自动建流）
curl -s -i -X POST localhost:8000/streams/ghost/seal \
  -H 'Content-Type: application/json' \
  -d '{"seal_id":"s","expected_next_sequence":0}'
# HTTP/1.1 404
```

## 事件历史快照导出示例（snapshot export）

```bash
# 准备 5 个已应用事件（balance=150, next_sequence=5）
for i in 0 1 2 3 4; do
  curl -s -o /dev/null -X POST localhost:8000/streams/acct-5/events \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"e$i\",\"sequence\":$i,\"delta\":$((i+1))0}"
done

# 首请求（不带 cursor）：固定快照上界 snapshot_next_sequence=5，返回第一页
curl -s 'localhost:8000/streams/acct-5/events?limit=2'
# {"stream_id":"acct-5","snapshot_next_sequence":5,"balance":150,
#  "events":[{"event_id":"e0","sequence":0,"delta":10},
#            {"event_id":"e1","sequence":1,"delta":20}],
#  "next_cursor":"eyJuZXh0X3NlcXVlbmNlIjoyLCJzbmFwc2hvdF9uZXh0X3NlcXVlbmNlIjo1LCJzdHJlYW1faWQiOiJhY2N0LTUifQ==.<签名>"}

# 快照固定后继续追加新事件（不影响进行中的导出）
curl -s -o /dev/null -X POST localhost:8000/streams/acct-5/events \
  -H 'Content-Type: application/json' -d '{"event_id":"e5","sequence":5,"delta":60}'

# 携带 next_cursor 翻页：仍是同一快照（上界 5、余额 150），新事件不会混入
curl -s -G 'localhost:8000/streams/acct-5/events' \
  --data-urlencode 'limit=2' \
  --data-urlencode 'cursor=eyJuZXh0X3NlcXVlbmNlIjoyLCJzbmFwc2hvdF9uZXh0X3NlcXVlbmNlIjo1LCJzdHJlYW1faWQiOiJhY2N0LTUifQ==.<签名>'
# -> events=[e2, e3]，next_cursor 指向下页
# 再翻一页 -> events=[e4]，next_cursor=null，导出结束（共 5 条，不漏不重）

# 重新发起不带 cursor 的请求：固定新快照，包含刚追加的事件
curl -s 'localhost:8000/streams/acct-5/events?limit=100'
# -> snapshot_next_sequence=6, balance=210, events=[e0..e5], next_cursor=null

# 篡改游标签名 / 跨流使用：400
curl -s -i -G 'localhost:8000/streams/acct-5/events' \
  --data-urlencode 'cursor=AAAA.<篡改的签名>'
# HTTP/1.1 400 -> {"detail":{"code":"invalid_cursor", ...}}
# 把 acct-5 的游标用到别的流：400 cursor_stream_mismatch

# 流不存在：404
curl -s -i 'localhost:8000/streams/ghost/events'
# HTTP/1.1 404 -> {"detail":{"code":"stream_not_found", ...}}
```

## 数据模型

```text
streams(stream_id PK, balance, next_sequence)
events(stream_id, event_id, sequence, delta, applied,
       PK(stream_id, event_id), UNIQUE(stream_id, sequence))
forks(source_stream_id, fork_id, target_stream_id, through_sequence, balance,
      PK(source_stream_id, fork_id), UNIQUE(target_stream_id))
transfers(transfer_id PK, source_stream_id, target_stream_id, amount,
          source_sequence, target_sequence,
          CHECK(amount > 0), CHECK(source_stream_id <> target_stream_id))
reversals(reversal_id PK, stream_id, event_id, reversal_event_id,
          reversal_sequence, delta,
          UNIQUE(stream_id, event_id),
          FK(stream_id, event_id) -> events(stream_id, event_id))
seals(stream_id PK, seal_id, next_sequence, event_count, sha256,
      FK(stream_id) -> streams(stream_id))
consumers(stream_id, consumer_id, cursor_seq,
          lease_token, lease_expires_at, lease_from_seq, lease_to_seq,
          PK(stream_id, consumer_id))
consumer_failures(stream_id, consumer_id, sequence, attempts, last_reason, updated_at,
                  PK(stream_id, consumer_id, sequence),
                  FK(stream_id, consumer_id) -> consumers(stream_id, consumer_id))
dead_letters(stream_id, consumer_id, sequence, event_id, delta,
             attempts, last_reason, dead_lettered_at,
             PK(stream_id, consumer_id, sequence),
             FK(stream_id, consumer_id) -> consumers(stream_id, consumer_id),
             FK(stream_id, event_id) -> events(stream_id, event_id))
nack_outcomes(lease_token PK, stream_id, consumer_id, cursor_seq, attempts,
              max_attempts, dead_lettered, reason, settled_at,
              FK(stream_id, consumer_id) -> consumers(stream_id, consumer_id))
```

- `UNIQUE(stream_id, event_id)` 保证重放幂等；
- `UNIQUE(stream_id, sequence)` 保证序号唯一；
- `applied=0` 表示暂存，`applied=1` 表示已计入余额；
- `transfers` 以全局 `transfer_id` 为主键记录每笔已提交转账的参数与两侧事件序号：
  重试按 `transfer_id` 比对参数，`source_sequence`/`target_sequence` 即原扣款/入账
  事件的追加位置；转账事件本身仍是 `events` 表中的普通行，事件 id 形如
  `transfer:<transfer_id>:debit|credit`，`events` 的两个唯一约束为其兜底；
- `reversals` 以全局 `reversal_id` 为主键记录每笔已提交撤销的目标事件、补偿事件
  id 与追加序号：`UNIQUE(stream_id, event_id)` 保证每个事件至多被撤销一次，重试
  按 `reversal_id` 比对目标；补偿事件本身仍是 `events` 表中的普通行，事件 id 形如
  `reversal:<reversal_id>`，`delta` 为原事件 `delta` 的相反数；
- 每个 `(stream_id, consumer_id)` 一行：`cursor_seq` 为消费游标（下一个待领取
  的序号），租约四列要么整体为 NULL（无有效租约），要么整体非空。一行只持有一个
  租约，因此领取与确认都在 `BEGIN IMMEDIATE` 事务内串行执行——即使多个应用实例
  同时领取，数据库中也只会存在一个有效租约。
- `consumer_failures` 为仍在重试的批次首事件记录**连续 nack 次数**：nack 时
  upsert（`attempts = attempts + 1`），ack 越过该序号或事件被死信隔离时删除；
  它与 `dead_letters` 对同一 `(stream, consumer, sequence)` 永不共存。
- `dead_letters` 是死信（隔离）记录：达到 `CONSUMER_MAX_ATTEMPTS` 的那次 nack 在
  推进游标的同一事务内插入，复制当时不可变事件的 `event_id`/`delta`，并保存失败
  次数、最后原因与 POSIX 毫秒隔离时间；每个 `(stream, consumer, sequence)` 至多
  一条、只追加，查询按 `sequence` 升序。
- `nack_outcomes` 以 `lease_token` 为主键保存每个已结算 nack 的原结果，供同 token
  重试幂等回放（不重复计数）；每个消费者只保留最近一次已结算 token 的结果——一旦
  claim 到更新的租约，旧 token 一律按 `lease_token_mismatch` 拒绝。
- `seals` 以 `stream_id` 为主键记录每流至多一条的不可逆封存：`seal_id` 为流内
  幂等键，`next_sequence` 为封存时的版本（连续前缀长度），`event_count`/`sha256`
  为按 sequence 升序的规范 JSON 行（`ensure_ascii=False`、紧凑分隔符、UTF-8、
  换行拼接、无末尾换行）摘要。另外 `BEFORE INSERT` 触发器
  `trg_events_sealed_insert` 在存储层兜底：任何向已封存流插入 `events` 行的尝试
  都会被数据库直接中止，与应用层的 `stream_sealed` 检查共同保证封存不可逆。
- 快照导出**不新增任何表**：游标是无状态的签名令牌（规范 JSON + HMAC-SHA256），
  快照上界与翻页位置全部封装在游标内；快照一致性由"已应用事件不可变"这一事实
  保证，而非由服务端保存的快照副本保证。

租约生命周期：

1. claim 时若存在未到期租约（`lease_expires_at > now`），直接返回该租约覆盖的
   原有批次（`lease_from_seq..lease_to_seq`）、原 token、原到期时间；
2. 否则从 `cursor_seq` 起取连续已应用事件（序号上界为流的 `next_sequence`），
   至多 `max_events` 条，生成新 token（`secrets.token_urlsafe`）和
   `expires_at = now + LEASE_DURATION_SECONDS`，写入租约四列并提交；
3. ack 在单个 `BEGIN IMMEDIATE` 事务内校验：存在流（否则 `404`）→ 存在租约
   （否则 `lease_not_active`）→ token 一致（否则 `lease_token_mismatch`）→
   未过期（否则 `lease_expired`）；全部通过后把游标推进到 `lease_to_seq` 并清空
   租约列，原子提交；
4. 租约到期不会自动删除：旧 token 无法 ack（`409`），下一次 claim 会发放新
   token，同一批事件可被重新消费。

nack 与死信的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 先确认流存在（否则 `404`，事务内无写入）；读取消费者行：
   - 当前**没有**租约时，先按 `lease_token` 查 `nack_outcomes`：命中则回放该
     token 首次结算时的原结果（`200`、`replayed=true`、不计数）；未命中即
     `409 lease_not_active`；
   - 当前**有**租约时（先于回放判断）：token 不一致即
     `409 lease_token_mismatch`（覆盖伪造 token、旧租约 token、已 nack 后又
     claim 到新租约的 token），已过期即 `409 lease_expired`（不计数、不释放）；
2. 空批次（`lease_from_seq >= lease_to_seq`）返回
   `409 empty_batch_not_nackable`，租约原样保留，仍可 ack；
3. 对批次首事件（`lease_from_seq`）在 `consumer_failures` 上 upsert：
   `attempts = attempts + 1`、更新 `last_reason`/`updated_at`；
4. **立即清空租约列**释放租约（无需等待到期）；
5. 若 `attempts < CONSUMER_MAX_ATTEMPTS`：游标保持 `cursor_seq` 不动，写入
   nack 结果后提交——下次 claim 仍从原游标返回同一首事件；
6. 若 `attempts >= CONSUMER_MAX_ATTEMPTS`：删除该首事件的失败计数行，读取事件的
   `event_id`/`delta`，向 `dead_letters` 插入死信记录，并把 `cursor_seq` 更新为
   `head_sequence + 1`（只越过首事件一个序号），全部与结果行在同一事务提交；
7. 写入结果前删除该消费者更早的 `nack_outcomes` 行（每个消费者只留最近一次）。

ack 成功时除推进游标、清空租约外，还会删除 `sequence < 新游标` 的
`consumer_failures` 行——一次成功确认即重置被越过事件的连续失败计数。并发 nack
在写锁上串行：同一 token 的并发请求只有一个完成首次结算，其余读到 `nack_outcomes`
后回放原结果，因此失败只计一次、死信只写一条。

提交事件的处理在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 若流不存在则创建（`balance=0, next_sequence=0`）；
2. 若 `event_id` 已存在：比对 `sequence`/`delta`，不一致即 `409`，一致则直接
   返回当前状态（幂等）；
3. 否则检查 `sequence` 是否已被占用，占用则 `409`，未占用则写入
   `applied=0` 的事件；
4. 从 `next_sequence` 开始循环取 `applied=0` 的连续事件累加 `delta`、标记
   `applied=1`，直到遇到缺号；
5. 更新 `balance` 与 `next_sequence`，提交事务。

分叉的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 锁定并读取源流：不存在直接返回 `404`（事务内不做任何写入）；
2. 按 `(source_stream_id, fork_id)` 查 `forks`：已存在则比对
   `target_stream_id`/`through_sequence`，不一致返回 `409 fork_replay_conflict`，
   一致则返回既有分叉（幂等重试，HTTP `200`）；
3. 校验 `0 <= through_sequence <= 源流 next_sequence`，越界返回
   `422 through_sequence_out_of_range`；
4. 确认目标流不存在（存在则 `409 target_stream_exists`，`streams.stream_id` 的
   主键与 `forks.target_stream_id` 的唯一约束共同兜底并发情况）；
5. 取源流 `applied=1` 且序号在 `[0, through_sequence)` 的事件（按序号升序），
   求和得到目标流初始 `balance`；插入目标流
   `(balance=和, next_sequence=through_sequence)`，把事件以 `applied=1` 原样复制，
   再写入 `forks` 分叉记录，统一提交——任一步失败整体回滚，不留半成品。

转账的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 按 `transfer_id` 查 `transfers`：已存在则比对源流、目标流、`amount`，不一致
   返回 `409 transfer_replay_conflict`，一致则回放原结果（`200`）——返回记录中的
   原始两侧事件序号，并重新读取两侧**当前**的流状态一并返回，不做第二次扣款；
2. 首次请求：读取并锁定源流与目标流行，任一不存在即 `404`
   （`source_stream_not_found` / `target_stream_not_found`），事务内无任何写入；
3. 校验源流当前 `balance >= amount`（余额只统计连续已应用前缀），不足返回
   `422 insufficient_funds`；
4. 在两侧当前 `next_sequence` 分别插入扣款（`-amount`）与入账（`+amount`）事件
   （`applied=0`，确定性事件 id `transfer:<transfer_id>:debit|credit`）；
5. 两侧分别从各自 `next_sequence` 起执行连续追赶：新事件（以及因此解除缺号的
   暂存事件链）依次累加并标记 `applied=1`，直到再次遇到缺号；
6. 更新两侧 `balance`/`next_sequence`，写入 `transfers` 记录
   （含两侧事件序号），统一提交——任一步失败整体回滚，事件与余额都不会残留。

并发重试时，多个相同 `transfer_id` 请求在 `BEGIN IMMEDIATE` 写锁上排队：第一个
拿到锁的请求完成插入并提交，其余请求随后读到既有记录，按幂等回放返回，因此
`transfers` 中只有一行、两侧各只有一条转账事件。

撤销的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 按 `reversal_id` 查 `reversals`：已存在则比对目标 `(stream_id, event_id)`，
   不一致返回 `409 reversal_replay_conflict`，一致则回放原结果（`200`）——返回
   记录中的原补偿事件序号与 `delta`，并重新读取流的**当前**状态一并返回，不做
   第二次补偿；
2. 首次请求：读取并锁定流行，不存在即 `404 stream_not_found`；再查目标事件，
   不存在即 `404 event_not_found`，事务内无任何写入；
3. 校验事件已应用（`applied=1`），仍因缺号暂存的事件返回
   `409 event_not_applied`；再按 `(stream_id, event_id)` 查 `reversals`，已被
   其他 `reversal_id` 撤销过则返回 `409 event_already_reversed`；
4. 在流当前 `next_sequence` 插入补偿事件（`applied=0`，确定性事件 id
   `reversal:<reversal_id>`，`delta=-原 delta`）；
5. 从 `next_sequence` 起执行连续追赶：补偿事件（以及因此解除缺号的暂存事件链）
   依次累加并标记 `applied=1`，直到再次遇到缺号；
6. 更新 `balance`/`next_sequence`，写入 `reversals` 记录，统一提交——任一步
   失败整体回滚，事件、余额与撤销记录都不会残留。

并发撤销时，多个请求在 `BEGIN IMMEDIATE` 写锁上排队：相同 `reversal_id` 的重试
读到既有记录按幂等回放返回；不同 `reversal_id` 争抢同一事件时，只有第一个提交
的请求成功，其余读到撤销记录后得到 `409 event_already_reversed`——补偿事件
恰好追加一次。

封存的处理同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 锁定并读取流：不存在直接返回 `404 stream_not_found`（事务内不做任何写入）；
2. 按 `stream_id` 查 `seals`：已存在则比对 `seal_id`/`expected_next_sequence`，
   任一不一致返回 `409 seal_replay_conflict`，完全一致则回放原封存记录
   （HTTP `200`，原 SHA-256 不变）；
3. 首次请求：比对流当前 `next_sequence` 与 `expected_next_sequence`，不一致返回
   `409 version_mismatch`（先于暂存检查）；
4. 检查是否存在 `applied=0` 的暂存事件，存在则返回
   `409 stream_has_pending_events`；
5. 按 sequence 升序读取全部已应用事件，逐条生成规范 JSON 行
   （`[sequence,event_id,delta]`，`ensure_ascii=False` 保留 Unicode、紧凑分隔符
   无空白、UTF-8），以 `\n` 拼接（无末尾换行）后计算 SHA-256；
6. 写入 `seals` 记录并统一提交——任一步失败整体回滚，不留封存记录。

封存后，普通提交、转账、撤销的写事务在**追加任何事件之前**先查 `seals`：命中即
返回 `409 stream_sealed`；幂等重放路径不写事件（命中既有 `event_id` /
`transfers` / `reversals` 记录后直接返回原坐标），因此封存前完成的操作仍可安全
重试。查询、claim/ack 与 fork 不追加源流事件，行为完全不变。

快照导出的读路径：

1. 首请求（无 `cursor`）在**一次读事务**（WAL 快照读，不阻塞写入）内读取流的
   当前 `next_sequence` 作为 `snapshot_next_sequence`，并在同一事务内取回第一页
   事件——上界与首页数据天然一致；
2. 每一页查询都是 `applied = 1 AND sequence >= 起点 AND sequence <
   snapshot_next_sequence ORDER BY sequence ASC LIMIT limit`：已应用事件不可变、
   序号不复用，所以并发追加（序号只会落在 `[snapshot_next_sequence, ...)`）永远
   不会进入快照范围，翻页不重不漏；
3. 每页的 `balance` 按 `SUM(delta) WHERE sequence < snapshot_next_sequence`
   现算——该区间的事件集合冻结不变，因此每页都精确等于快照版本的余额；
4. 页未满快照末尾时签发下一游标：把 `{stream_id, snapshot_next_sequence,
   next_sequence}` 编码为规范 JSON（`sort_keys`、紧凑分隔符、UTF-8、非 ASCII
   原样），以 `SNAPSHOT_CURSOR_SECRET` 为密钥计算 HMAC-SHA256，两段分别做
   URL-safe Base64 后以 `.` 拼接；到达上界时 `next_cursor = null`；
5. 携带游标的请求先校验流存在（否则 `404`），再依次校验游标结构、Base64、
   HMAC 签名（`hmac.compare_digest` 常量时间比较）、payload 字段（恰好三个字段、
   类型与取值合法、`next_sequence <= snapshot_next_sequence`）与流绑定——任一
   失败返回 `400`，签名密钥轮换后旧游标自然失效（`400 invalid_cursor`）。

## 配置

| 环境变量                | 默认值                | 说明                          |
| ----------------------- | --------------------- | ----------------------------- |
| `APP_PORT`              | `8000`                | 服务监听端口                  |
| `DATABASE_PATH`         | `/data/events.db`     | SQLite 文件路径               |
| `LEASE_DURATION_SECONDS` | `30`                 | claim 租约有效时长（秒，> 0） |
| `CONSUMER_MAX_ATTEMPTS`  | `3`                  | 批次首事件连续 nack 达到此次数后被送入死信、游标越过该事件（正整数；非整数或 `< 1` 时回退默认值）。Compose 通过 `${CONSUMER_MAX_ATTEMPTS:-3}` 提供默认值，可用环境或 `.env` 覆盖，如 `CONSUMER_MAX_ATTEMPTS=5 docker compose up --build` |
| `SNAPSHOT_CURSOR_SECRET` | `dev-snapshot-cursor-secret` | 快照导出游标的 HMAC-SHA256 签名密钥；Compose 通过 `${SNAPSHOT_CURSOR_SECRET:-dev-snapshot-cursor-secret}` 提供可用默认值，生产环境务必覆盖为随机长密钥。轮换密钥会使既有游标全部失效（`400`） |

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_api.py` 覆盖：顺序累加、缺号暂存与自动追赶、幂等重放、两种 409、
404、422 参数校验、大批量追赶以及 20 线程并发；消费者游标部分覆盖：领取连续批次、
缺号时只返回已应用事件、租约内重复领取同批同 token、ack 推进游标、错误/旧/过期
token 的三种 409、租约到期后同批重新领取、多消费者独立游标、空批次以及 20 线程
并发只产生一个有效租约；消费拒绝（nack）部分覆盖：阈值内 nack 不推进游标且重领
返回首事件、达到 `CONSUMER_MAX_ATTEMPTS` 时同一事务写死信并恰好越过一个序号、
后续 claim 从下一序号继续、同一 token 重试返回原结果且不重复计数（含阈值时刻）、
伪造 token `lease_token_mismatch` 且租约不受影响、未 claim `lease_not_active`、
已 nack 后重新 claim 再用旧 token `409`、过期 token `lease_expired` 且不计数、
空批次 `empty_batch_not_nackable` 且租约仍可 ack、`lease_token`/`reason` 缺失或
空白 `422`、流不存在 `404`、ack 重置连续失败次数、死信不阻塞后续事件、多个毒消息
按序隔离且按 `sequence` 升序返回、死信按消费者隔离、死信查询未知流 `404`/未知
消费者返回空列表，以及 20 线程并发 nack 同一 token 只结算一次；流分叉部分覆盖：整段/部分/空前缀复制、暂存事件不复制、
目标流可继续追加、源流 404、目标已存在 409、上界越界 422、相同参数重试 200 幂等、
参数漂移 409、请求体 422，以及 20 线程并发（同一 `fork_id` 只产生一个分叉；
不同 `fork_id` 争抢同一目标流时不留半成品）；跨流转账部分覆盖：正常扣款/入账与
响应字段、事件可被 claim 消费、恰好足额、余额不足 `422` 且无副作用、源流/目标流
缺失 `404`、相同参数重试 `200` 返回原序号且不重复扣款、重试时返回最新流状态、
参数漂移 `409`、请求体 `422`、暂存事件在转账时一并追赶、与 fork/消费者接口共存，
以及 20 线程并发（同一 `transfer_id` 只提交一次；50 笔不同转账串行提交且总额守恒）；
事件撤销部分覆盖：正/负 `delta` 事件的补偿追加与响应字段、相同目标重试 `200`
返回最新状态且不重复补偿、相同 `reversal_id` 不同目标 `409`、同一事件换 id 再撤销
`409`（且失败的 `reversal_id` 不作废）、流/事件缺失 `404`、未应用事件 `409` 且
缺号补齐后可撤销、请求体 `422`、补偿事件关闭缺号一并追赶暂存事件、补偿事件可被
claim/fork 观察、撤销转账事件、撤销的撤销，以及 20 线程并发（同一 `reversal_id`
只补偿一次；不同 `reversal_id` 争抢同一事件时恰好一个成功）；
流封存部分覆盖：正常封存与规范 SHA-256（含 Unicode 事件名按字面量保留）、空流
封存（空输入 SHA-256）、流不存在 `404`、版本不符 `409 version_mismatch` 且不消耗
`seal_id`、存在暂存事件 `409 stream_has_pending_events` 且补齐后可封存、相同请求
重试 `200` 返回同一摘要、`seal_id`/版本漂移 `409 seal_replay_conflict`、封存后
新提交 `409 stream_sealed` 但旧事件幂等重放 `200`、封存流作为转账源/目标均被拒
但封存前转账可重放、封存后新撤销被拒但封存前撤销可重放、封存流仍可查询/消费/
ack/作为 fork 源且 fork 目标可追加、请求体 `422`，以及 20 线程并发（同一封存
请求恰好一次 `201`，其余 `200`）；快照导出部分覆盖：按 sequence 升序分页、整页
边界末页 `next_cursor=null`、快照固定后并发追加不混入/不漏项/不重复且余额保持
快照版本、旧游标幂等重放同一页、暂存事件不导出、空流与缺省 `limit=100`、流不
存在 `404`（含携带游标）、`limit` 非法 `422`、游标为规范 JSON + HMAC-SHA256 且
可验签、篡改 payload/签名/结构非法 `400 invalid_cursor`、跨流使用
`400 cursor_stream_mismatch`、合法签名但字段非法（缺字段/多字段/越界/负数/布尔/
类型错误）`400`、密钥轮换后旧游标失效、Unicode 流名游标往返。

仅用 Python 标准库验证核心归并算法（无需安装 FastAPI）：

```bash
python3 verify_core.py
python3 verify_forks.py
python3 verify_transfers.py
python3 verify_reversals.py
python3 verify_seals.py
python3 verify_snapshots.py
python3 verify_nacks.py
```

## 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── main.py        # FastAPI 路由与请求/响应模型
│   ├── repo.py        # 归并/追赶算法（纯 sqlite3，事务边界清晰）
│   ├── forks.py       # 流分叉（fork）幂等事务
│   ├── transfers.py   # 跨事件流原子转账（transfer）幂等事务
│   ├── reversals.py   # 事件撤销（reversal）幂等事务
│   ├── seals.py       # 不可逆封存（seal）与规范 SHA-256 摘要
│   ├── snapshots.py   # 事件历史快照导出与 HMAC 签名游标
│   ├── consumers.py   # 消费游标：claim/ack 租约、nack 重试与死信
│   ├── db.py          # 连接管理、WAL、schema 初始化
│   └── config.py      # 环境变量配置
├── tests/test_api.py
├── verify_core.py
├── verify_forks.py
├── verify_transfers.py
├── verify_reversals.py
├── verify_seals.py
├── verify_snapshots.py
├── verify_nacks.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
