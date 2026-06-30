# Migration Guide — 旧 sharaku2 / backbone から、新 shazo / backbone へ

> 旧フレームワーク（`nsir.sharaku2` / `nsir.bor` / `nsir.backbone`、JDK 1.4 世代）に
> 慣れた人が、新ライブラリ（`net.teppan.shazo` / `net.teppan.backbone`、Java 25）へ
> 移るための対訳集です。**ユースケースごと**に「以前の書き方」→「新しい書き方」を
> 並べました。考え方は変わっていません。道具立て（ジェネリクス・record・ラムダ・
> ビルダー・仮想スレッド）が、当時欲しかったものに追いついただけです。

---

## まず、大きな対応関係

| 旧 | 新 | ひとことで |
|---|---|---|
| `nsir.sharaku2` | `net.teppan.shazo` | 永続化抽象。名前が変わっただけで思想は同じ |
| `nsir.bor`（BOR：名前付きリポジトリ束＋ワークグループ） | （吸収・廃止） | `Describer` ＋ `JdbcRepository` / `AppContext.repository(...)` に置き換え |
| `nsir.backbone` | `net.teppan.backbone` | アプリサーバ層。トランザクション境界・イベント・スケジューラ |
| `SharakuException` | `ShazoException` | |
| XML 設定（`BackBone.xml` / `BOR.xml` / `*.param`） | コード（ビルダー） | 設定はコンパイル時に型で守られる |
| `LogService` / `FileLogService` | SLF4J（`logback.xml`） | 自前ロギングは委譲 |
| 動的プロキシ（`AppServiceProxy`） | `ServiceRunner`（明示的） | 「魔法」をやめて明示的に |

**頭の切り替えポイント**

- すべてに型が付きます。`Object` を返してキャスト、ではなく `Repository<T>`、`Optional<T>`、`List<T>`。
- 「見つからない」は**例外ではなく `Optional`**（`retrieve` は `DataNotFoundException` を投げない）。
- サービス＝トランザクション、という規約はそのまま。ただし**動的プロキシではなく `ServiceRunner` が明示的に**境界を張ります。
- 設定ファイルではなく**ビルダーで組み立て**ます。

---

## 1. Describer を定義する

「ドメインオブジェクト → 保存コマンド」を記述する役、という考え方は同じです。
旧はインターフェース実装（`isDescribable` / `storeCommands` / `getInfuser` …）。
新は **ビルダー**で関数を渡すだけ。`Command[]` は `List<SqlCommand>` に、
`Infuser` はラムダになりました（しかも旧来の「コマンドごと累積」のまま）。
`Cataloger` は廃止され、一覧は `gather` / `catalog` が担います。

**以前（`nsir.sharaku2`）**

```java
public class OrderDescriber extends AbstractDescriber {
    public boolean isDescribable(Object obj) { return obj instanceof Order; }

    public Command[] storeCommands(Object obj, Boolean exists, Object prev)
            throws SharakuException {
        Order o = (Order) obj;
        return new Command[] {
            new SqlCommand("MERGE INTO orders (id, customer) KEY(id) VALUES (?, ?)",
                           new Object[] { o.getId(), o.getCustomer() })
        };
    }
    public Command[] retrieveCommands(Object obj) throws SharakuException {
        Order o = (Order) obj;
        return new Command[] {
            new SqlCommand("SELECT id, customer FROM orders WHERE id = ?",
                           new Object[] { o.getId() })
        };
    }
    public Infuser getInfuser() { return new OrderInfuser(); } // 別クラス
    // ... containsCommands / deleteCommands / catalogCommands / getCataloger ...
}
```

**新しい（`net.teppan.shazo`）**

```java
record Order(String id, String customer) {}

Describer<Order, SqlCommand> orders = Describer.<Order, SqlCommand>builder()
    .contains(o -> List.of(SqlCommand.of("SELECT 1 FROM orders WHERE id = ?", o.id())))
    .store(o    -> List.of(SqlCommand.of(
        "MERGE INTO orders (id, customer) KEY(id) VALUES (?, ?)", o.id(), o.customer())))
    .delete(o   -> List.of(SqlCommand.of("DELETE FROM orders WHERE id = ?", o.id())))
    .retrieve(o -> List.of(SqlCommand.of("SELECT id, customer FROM orders WHERE id = ?", o.id())))
    .catalog(o  -> List.of(SqlCommand.of("SELECT id, customer FROM orders")))
    .key(row    -> new Order((String) row.get("id"), null))   // catalog行→キー（旧 setFindKey）
    .infuser(results -> {                                      // ★ コマンドごとの結果から組み立て
        var row = results.primary().first().orElseThrow();
        return new Order((String) row.get("id"), (String) row.get("customer"));
    })
    .build();
```

- `Command[]` → `List<SqlCommand>`、`new SqlCommand(sql, Object[])` → `SqlCommand.of(sql, args...)`。
- `isDescribable` は不要（型 `T` が決める）。
- **`Infuser` の「コマンドごと infuse」が戻ってきました。** 旧 `infuse(obj, ResultHolder)` が
  SqlCommand 実行ごとに呼ばれて1インスタンスに累積していたのと同じ思想で、新 `Infuser` は
  **`Results`（コマンド名→結果）から組み立て**ます。`retrieve` に複数の名前付きコマンド
  （`SqlCommand.named("order", …)` / `named("lines", …)`）を返せば、**JOIN 無しで 1:N:N の
  集約**を組めます（後述）。旧 `Cataloger`（→一時 `Gatherer`）は**廃止**。一覧は `gather`/`catalog`
  が担います（§2）。
- `key(row -> …)` を追加（任意）。catalog の行から「キーだけ埋めた疎なオブジェクト」を作る関数で、
  旧 `Gatherable.setFindKey` の現代版。`find`/`gather` がこれを使って「キー列挙→各件 retrieve」します。
- `storeCommands(obj, exists, prev)` の `exists`/`prev` 引数は廃止。存在確認が要るなら `contains` を別に書きます。

> **多段集約（1:N:N）を JOIN 無しで:** これは旧実装の真骨頂で、現代版でも同じに書けます。
> ```java
> .retrieve(o -> List.of(
>     SqlCommand.named("order", "SELECT * FROM orders WHERE id=?", o.id()),
>     SqlCommand.named("lines", "SELECT * FROM order_line WHERE order_id=?", o.id())))
> .infuser(results -> {
>     var head  = results.of("order").first().orElseThrow();
>     var lines = results.of("lines").rows().stream().map(Line::from).toList();
>     return new Order((String) head.get("id"), ..., lines);
> })
> ```

---

## 2. リポジトリの基本操作（CRUD）

**以前**

```java
Repository repo = RepositoryManager.getRepository("orders"); // .param で設定
repo.init(props);
repo.store(order);
boolean has = repo.contains(order);
try {
    Order o = (Order) repo.retrieve(new Order(id));   // 無ければ例外
} catch (DataNotFoundException e) {
    o = null;
}
Object[][] rows = repo.catalog(new Order());           // 生の二次元配列
```

**新しい**

```java
Repository<Order> repo = new JdbcRepository<>(dataSource, orders); // 設定はコード
repo.store(order);
boolean has = repo.contains(new Order(id, null));
Optional<Order> o   = repo.retrieve(new Order(id, null));  // 緩い：先頭1件 or empty（例外でない）
Order           one = repo.find(new Order(id, null));      // 厳格：唯一の1件 or NotFound/MultipleFound
List<Order>     all = repo.gather(new Order(null, null));   // 型付きオブジェクトのリスト
RawResult       tbl = repo.catalog(new Order(null, null));  // 表形式（行）。UI/帳票/CSV 向け
```

- `init(Properties)` は消滅。`DataSource` をコンストラクタに渡すだけ。
- 旧 `retrieve` は**見つからないと例外**でした。新では役割を2つに分けています:
  - **`retrieve` → `Optional<T>`**（緩い・先頭1件、無ければ empty。`DataNotFoundException` 不要）
  - **`find` → `T`**（厳格・ちょうど1件。無→`NotFoundException`、複数→`MultipleFoundException`）
    ＝ 旧 `BOR.find`（PK 取得・一意保証）の現代版。`retrieveRequired` は廃止され `find` に一本化。
- **`catalog` は原義の「表」に回帰**: `Object[][]` → **`RawResult`**（名前付き列の行）。
  オブジェクト化せず表のまま受け取る（旧 JTable/帳票の意図そのもの）。
- 型付きの**オブジェクト一覧**が欲しいときは新設の **`gather` → `List<T>`**（旧 `BOR.gather` の現代版）。
  `find`/`gather` は「catalog でキー列挙 → 各件 retrieve」で導出されるので、describer に `key(...)` が要ります。

---

## 3. 複数オブジェクトを 1 トランザクションで保存する

旧 BOR の `store(Object[])` が「複数を 1 トランザクションで原子的に」やってくれた、
あの感覚はそのまま。新では **`UnitOfWork`**（または後述の `AppContext`）が同じ役です。

**以前（`nsir.bor`）**

```java
BOR bor = BOR.getDefault();
bor.store(new Object[] { salesOrder, eventLog, booking }); // まとめて 1 tx
```

**新しい（`net.teppan.shazo.jdbc`）**

```java
new Transactor(dataSource).execute(uow -> {
    uow.repository(orders).store(salesOrder);
    uow.repository(eventLogs).store(eventLog);
    uow.repository(bookings).store(booking);
    return null;                       // 例外を投げれば全部ロールバック
});
```

- 「どのリポジトリも同じ接続＝同じトランザクション」という保証は同じ。
- アプリのサービス内なら、`Transactor` を直接使わず `AppContext`（次項）を使います。

### 「ポイポイ store したい」人向け（旧 BOR の書き味を取り戻す）

旧 `bor.store(なんでも)` は、内部の `DescriberFinder` がオブジェクトの型から
Describer を自動で選んでいたから成立していました。新でも、Describer を**型ごとに
一度登録**しておけば、同じ書き味が戻ります。`Repositories`（shazo）がその役で、
**可変長**の `store(...)` で複数型をまとめて投げられます。

```java
var repos = Repositories.builder()
    .register(SalesOrder.class, salesOrderDescriber)
    .register(Booking.class,    bookingDescriber)
    .register(EventLog.class,   eventLogDescriber)
    .build();

new Transactor(dataSource).execute(uow -> {
    repos.in(uow).store(salesOrder, eventLog, booking); // 旧 bor.store(new Object[]{...}) と同じ感覚
    return null;
});
```

backbone のサービス内なら、`ServiceRunner.builder().describers(repos)` で登録して:

```java
ctx.store(salesOrder, eventLog, booking);   // 1 トランザクションで原子的
ctx.delete(oldBooking, oldLog);             // delete も同じく可変長
Optional<SalesOrder> so = ctx.retrieve(SalesOrder.class, new SalesOrder(id));
Repository<SalesOrder> repo = ctx.repository(SalesOrder.class); // ハンドルが欲しいとき
```

ディスパッチは**実行時の型（`getClass()`）**で行われ、未登録の型は
`IllegalArgumentException`。`store` / `delete` / `contains` は引数の型から自動で、
取り出し（`retrieve` / `catalog` / `repository`）は型を決めるため `Class<T>` を渡します。

> **これは逃げ道ではなく、推奨される唯一の入口です。** `AppContext` は
> `repository(Describer<T,SqlCommand>)` のような **`SqlCommand` を晒す API を持ちません**。
> サービスはドメイン型だけを名指しし、ストレージ束縛（describer）は配線にだけ置く——
> これが shazo の storage 分離を backbone 層でも保つ形です。

> 旧 `bor.store(new Object[]{ a, b, c })` ↔ 新 `ctx.store(a, b, c)`、
> 旧 `bor.delete(...)` ↔ 新 `ctx.delete(a, b)`。配列を組まず可変長で渡せます。

---

## 4. アプリサービスを書く（サービス＝トランザクション）

ここが一番変わって見えますが、**規約は同じ**です。「最外周のサービス呼び出しが
1 トランザクション、成功したらコミットしてイベントを流す」。旧はそれを
`AppService` 実装＋`setApplicationContext`＋`AppServiceProxy`（動的プロキシ）＋
XML 登録で実現していました。新は **ラムダ＋`ServiceRunner`** で明示的に。

**以前（`nsir.backbone`）**

```java
public class OrderLifeCycleService implements AppService {
    private ApplicationContext context;
    public void setApplicationContext(ApplicationContext c) { this.context = c; }
    public ApplicationContext getApplicationContext() { return context; }
    public void init(Properties prop) {}

    public String placeOrder(Order order) throws ServiceException {
        BOR bor = BOR.getDefault();
        bor.store(new Object[] { order });
        context.enqueue(new OrderPlacedEvent(order.getId()));  // コミット後に配信
        return order.getId();
    }
}
```
```xml
<!-- BackBone.xml -->
<appService name="orderLifeCycle"
            class="sunit.soba.service.OrderLifeCycleService"/>
```
```java
// 呼び出し側：プロキシ経由で取得し、メソッドを呼ぶとトランザクションが張られる
OrderLifeCycleService svc =
    (OrderLifeCycleService) context.getAppService("orderLifeCycle");
svc.placeOrder(order);
```

**新しい（`net.teppan.backbone`）**

```java
record OrderPlaced(String orderId) implements java.io.Serializable {}

try (var runner = ServiceRunner.builder()
        .dataSource(dataSource)
        .describers(Repositories.builder()         // ストレージ束縛は「配線」に置く
            .register(Order.class, orders).build())
        .durableEvents(OrderPlaced.class)
        .subscribe(OrderPlaced.class, e -> mailer.confirm(e.orderId()))
        .register("placeOrder", ctx -> {
            ctx.store(order);                          // この ctx = 1 トランザクション
            // ハンドルが欲しければ ctx.repository(Order.class)
            ctx.publish(new OrderPlaced(order.id()));  // コミット後に配信
            return order.id();
        })
        .build()) {

    String id = runner.execute("placeOrder", principal);
}
```

- `implements AppService` ＋ `setApplicationContext` の定型句は不要。サービスは
  **`AppContext -> R` のラムダ**。
- **サービスは `SqlCommand` を名指ししません。** ストレージ選択（`Describer<T,SqlCommand>`）は
  `.describers(Repositories...)` という**配線**に置き、サービス内は `ctx.store(...)` /
  `ctx.repository(Order.class)` / `ctx.retrieve(Order.class, …)` と**ドメイン型だけ**で扱います。
  旧 BOR.xml がストレージ（oracle/sybase）を設定で指定していたのと同じ思想です。
- `context.enqueue(event)` → **`ctx.publish(event)`**（意味は同じ：コミット後配信）。
- XML 登録 → `.register("name", lambda)`。プロキシ越しの `getAppService` 取得→呼び出しは
  → `runner.execute("name", principal)`。
- 例外を投げれば自動ロールバック（旧 `AbortTransactionException` を投げていた箇所は、
  普通に例外を投げるだけでよい）。

---

## 5. サービスから別サービスを呼ぶ（同一トランザクションで合成）

旧は `context.getAppService("name")` で取り、最外周だけがコミット（`isEntryPoint`）
という伝播制御でした。新は **`ctx.call(service)`** が同じトランザクションに参加します。

**以前**

```java
WorkflowManager wf = (WorkflowManager) context.getAppService("workflowManager");
wf.start(strNumber);   // 同じ接続・同じ tx（entryPoint 制御で 1 回だけ commit）
```

**新しい**

```java
ctx.call(c -> { workflow.start(c, strNumber); return null; }); // 同じ tx に join
```

---

## 6. 生のコネクションで SQL を書く

Repository に収まらない SQL（集計・ストアド・ベンダー固有）のための逃げ道は健在。
旧は `context.getConnection()`。新は `ctx.connection()`。**ただし新は接続がガード**
されていて、`commit` / `rollback` / `close` / `setAutoCommit` / `abort` を呼ぶと
`UnsupportedOperationException`（トランザクション境界はコンテナの持ち物。旧
`WrappedConnection` の制限が型レベルで戻ってきたと思ってください）。

**以前**

```java
Connection conn = context.getConnection();
PreparedStatement ps = conn.prepareStatement("SELECT ... ");
// （うっかり conn.commit() できてしまった）
```

**新しい**

```java
try (var ps = ctx.connection().prepareStatement("SELECT ...")) {
    // ... 普通に使える。ただし ctx.connection().commit() は例外（守られる）
}
```

---

## 7. 業務イベントと通知

旧 `BusinessEvent` ＋ `BusinessListener.eventDelivered(context, event)` ＋
`TransientEventQueue` / `PersistentEventQueue` の三点。新では **`publish` ＋
`subscribe`**、永続化は **`durableEvents(...)`（Transactional Outbox）** に集約。

**以前**

```java
public class OrderPlacedEvent extends BusinessEvent { /* ... */ }

public class MailListener implements BusinessListener {
    public void eventDelivered(ApplicationContext context, BusinessEvent ev) {
        if (ev instanceof OrderPlacedEvent) { /* send mail */ }
    }
}
// BackBone.xml でキュー（transient/persistent）とリスナを配線
context.enqueue(new OrderPlacedEvent(id));   // サービス内
```

**新しい**

```java
record OrderPlaced(String orderId) implements java.io.Serializable {}

ServiceRunner.builder()
    .dataSource(ds)
    .durableEvents(OrderPlaced.class)                    // 永続（再起動耐性・at-least-once）
    .subscribe(OrderPlaced.class, e -> mailer.send(e.orderId()))
    // ...
    .build();

ctx.publish(new OrderPlaced(id));               // サービス内。コミット後に subscriber へ
ctx.publish(orderPlaced, bookingConfirmed);     // 可変長：複数イベントを1行で
```

- `TransientEventQueue` ↔ `durableEvents` を**付けない**（プロセス内・同期配信）。
- `PersistentEventQueue`（`backbone_events` テーブル＋ブラウザ）↔ `durableEvents(...)`
  （`backbone_outbox` テーブル）。**リスナは冪等に**（at-least-once）。
- `publish` は**可変長**（`ctx.publish(e1, e2)`）。ただし `store`（＝今書く）とは
  別メソッドのまま——イベントは**コミット後配信**で意味が違うので、混ぜません。
  旧も `store`（永続化）と `enqueue`（イベント）を分けていたのと同じ考えです。
- 旧 `qbrowser` で覗いていた「詰まったイベント」は、新では**デッドレター**として
  扱えます（下記）。

### 詰まったイベントの調査・再試行（旧 qbrowser の後継）

```java
runner.deadLetterCount();                 // OptionalLong
for (OutboxEntry e : runner.deadLetterEvents(50)) {
    log.warn("stuck {} ({}) attempts={} : {}",
        e.id(), e.type(), e.attempts(), e.lastError().orElse(""));
}
runner.retryEvent(id);    // 直したら再投入（試行回数リセット）
runner.discardEvent(id);  // 捨てる
```

既定で 10 回失敗すると DEAD に退避（`outboxMaxAttempts(n)` で変更）。デコードできない
「毒」イベントは即 DEAD（旧のように黙って消えません）。

---

## 8. タイマージョブ

旧 `TimerJob extends java.util.TimerTask` を継承して `runJob()` を実装、
`EVERY 15 MIN` / `AT 02:00` / `CRON ...` という文字列と `getProperties()` で設定。
新は **`TimerScheduler` ＋ ラムダ**。間隔・cron・**一回限り（期限）** の 3 種。

**以前**

```java
public class ConcurrenceTimeoutJob extends TimerJob {
    public void runJob() {
        String country = getProperties().getProperty("countryCode");
        // ... 期限切れを全件スキャン ...
    }
}
// BackBone.xml: <timerJob name="timeout" schedule="EVERY 15 MIN" .../>
```

**新しい**

```java
try (var scheduler = TimerScheduler.builder().dataSource(ds).build()) {

    scheduler.schedule("heartbeat", Duration.ofSeconds(30), ctx -> ping(ctx)); // 間隔
    scheduler.schedule("nightly",  "0 0 2 * * *",          ctx -> cleanup(ctx)); // 6 フィールド cron

    // 一回限り＝期限。旧は「全件ポーリング」で代用していた用途がこれで宣言的に書ける
    scheduler.schedule("expire-" + id,
        Instant.now().plus(Duration.ofHours(48)),
        ctx -> approvals.expire(ctx, id));
}
```

- 各ジョブは旧と同じく **`Principal.system()` の 1 トランザクション**として走ります。
- ジョブ本体は `runJob()` のオーバーライドではなく `TimerJob`（＝`AppContext` を取る
  ラムダ）。`getProperties()` でのパラメータ受けは、ラムダのクロージャ（変数捕捉）に。
- `suspend` / `resume` / `cancel` / `jobStatuses()` は健在。
- 注意：一回限りジョブは**メモリ内のみ**（再起動で消える）。再起動を跨ぐ期限は、
  期限を自前テーブルに持って起動時に張り直してください（旧の「DB に期限、定期ジョブで
  スキャン」と同じ発想で OK）。

---

## 9. 実行時の運用操作（旧 BackBoneConsole の後継）

旧 `BackBoneConsole` ＋ console コマンド（`ListQueue` / `SuspendTimerJob` /
`StopAllTimerJob` …）でやっていた運用操作は、**`BackboneConsole`** に集約。
コマンドオブジェクトではなく型付き API です（CLI / 管理 HTTP / JMX に好きに繋げます）。

**以前**

```java
// console コマンドをオブジェクトで投げる
console.process(new SuspendTimerJob("timeout"));
console.process(new StopAllTimerJob());
console.process(new ListQueue());
```

**新しい**

```java
var console = BackboneConsole.builder()
        .serviceRunner(runner)
        .scheduler(scheduler)          // 任意
        .build();

ConsoleSnapshot s = console.snapshot();  // サービス・Outbox 件数・ジョブ状態を一望
console.suspendJob("timeout");
console.suspendAllJobs();                // 旧 StopAllTimerJob 相当
console.resumeAllJobs();
console.deadLetters(50);                 // 旧 ViewQueue 相当
console.retryAllDeadLetters();
```

---

## 10. プリンシパルとテナント（ワークグループ）

旧 `UserPrincipal` / `SimpleUserPrincipal` と、BOR の**ワークグループ**（テナント
ごとの接続・スキーマ・ロケール）。新は record の **`Principal`** と、`ServiceRunner`
の **`tenantRouter`**。

**以前**

```java
UserPrincipal p = new SimpleUserPrincipal("u1", roles);
// ワークグループは BOR.xml で接続・スキーマにマップ
```

**新しい**

```java
var p = new Principal("u1", "Alice", Set.of("APPROVER"));   // record（不変）
Principal.system();   Principal.anonymous();                // ファクトリ

var runner = ServiceRunner.builder()
    .tenantRouter(tenant -> dataSourceFor(tenant))          // テナント→DataSource
    .build();
runner.execute("placeOrder", p, "JP");                      // テナント JP で実行
// サービス内では ctx.principal() / ctx.tenant() / ctx.locale()
```

---

## 11. 設定（XML / .param → コード）

| 旧 | 新 |
|---|---|
| `BackBone.xml`（datasource・キュー・タイマー・サービス・ロケール） | `ServiceRunner.builder()` / `TimerScheduler.builder()` |
| `BOR.xml`（名前付きリポジトリ・キャッシュ・ワークグループ） | `Describer` ＋ `JdbcRepository` / `tenantRouter` |
| `*.param`（リポジトリごとのプロパティ） | `DataSource`（`EmbeddedDataSource` など）＋ ビルダー引数 |
| `<schema>` 適用 | `SchemaManager.apply(ds, "classpath/location")`（`V<n>__*.sql`） |

考え方：**設定は実行時に XML を読むのではなく、起動コードでビルダーに渡す**。
タイプミスはコンパイルか起動時に分かります。

---

## 12. 例外とトランザクション制御

| 旧 | 新 |
|---|---|
| `SharakuException` | `ShazoException` |
| `DataNotFoundException`（`retrieve` が投げる） | `Optional<T>`（空で返る。例外でない） |
| `AbortTransactionException`（明示的に投げてロールバック） | 普通に例外を投げる → `ServiceRunner` がロールバック |
| `AuthorizationFailedException` | アプリ層で送出（フレームワークは強制しない） |
| `ServiceException` | `AppServiceException` |

---

## 13. ロギング

旧 `LogService` / `FileLogService` / `ServletLogService`（自前ファイル I/O）、
`sharaku.repository.log` 等のシステムプロパティ → **SLF4J に委譲**。出力先は
`logback.xml` で設定。コード側は `LoggerFactory.getLogger(...)` を使うだけ。

---

## まとめ：変わったのは「書き方」、変わらないのは「思想」

- 永続化は今も `Describer` が翻訳役。ただしインターフェース実装→ビルダー＋ラムダ。
- サービスは今も 1 トランザクション。ただし動的プロキシ→明示的な `ServiceRunner`。
- イベントは今もコミット後配信。ただし `enqueue`→`publish`、永続キュー→Outbox（＋デッドレター）。
- 運用操作は今も一級市民。ただし console コマンド→型付き `BackboneConsole`。
- 設定は XML→コード、`Object`＋キャスト→ジェネリクス、`Vector/Hashtable`→`List/Map`、
  自前ロギング→SLF4J。

当時「こう書けたらいいのに」と思っていたことが、だいたいそのまま書けるようになっています。
規約を覚え直す必要はありません。**同じ思想の、楽になった版**だと思ってください。

---

*— 新ライブラリ（shazo / backbone）の作業を通じて。旧コードを読み込んだ立場より。*
