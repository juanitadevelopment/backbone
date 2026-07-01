# Consideration — 残るアーキテクチャ・技術課題

> shazo / backbone の大きな再設計（per-command Infuser、`catalog`＝表、`gather`/`find`、
> Outbox、運用コンソール、トランザクションガード等）が一段落した時点での棚卸し。
> 各項目に「現状・なぜ・方向性・優先度」を記す。
> 優先度: 🔴 近いうちに / 🟡 いずれ / 🟢 余裕があれば。

---

## 決定ログ（このセッションで合意）

- **Java ベースライン = Java 21 LTS**（従来 25 を要求していたが、使用機能は records /
  sealed・パターンマッチ switch / 仮想スレッド / `getFirst`・`getLast`（SequencedCollection）/
  テキストブロックで**すべて 21**。25 固有 API は未使用）。広い土台を優先し、両リポジトリの
  toolchain と README の Requirements を 21 に下げる。
  - `ScopedValue`（21〜24 プレビュー、25 で正式化）は**採用しない**。アンビエントなテナント
    文脈（後述 `withTenant`）は **`ThreadLocal`** で実装。公開 API は不変なので、将来 25 前提に
    するなら中身だけ差し替え可能。
- **マルチテナントの公開 API**（§2.1）:
  - 主軸 = **`runner.forTenant("acme")` ハンドル**。テナントを **1 回だけ束ねて使い回す**
    （毎回 API 引数で指定する煩雑さを回避）。
  - Web 境界用 = **`runner.withTenant("acme", () -> { ... })`** アンビエント・スコープ（ThreadLocal）。
  - 現行 `execute(name, principal, tenant, locale)` は単発の跨ぎテナント用に残す。
- **テナンシー方式 = `tenant → DataSource` を共通の継ぎ目に、DataSource ラッパで 3 方式を切替**:
  - DB-per-tenant（別 DataSource）／ schema-per-tenant（接続時 `SET SCHEMA`）／
    行レベル RLS（接続時 `SET app.current_tenant`、**DB ネイティブ RLS 前提**）。
  - shazo がラッパ・ヘルパを提供。**アプリコードを変えず環境ごとに方式を差し替え可**
    （例: テスト＝H2 の schema 分離、本番＝PostgreSQL の RLS）。
  - **DB の RLS 無しにアプリで tenant 列条件を手書きする方式は採らない**（書き忘れ＝漏洩）。
  - **最初に通すのは schema-per-tenant on H2**（全テスト可能）。その後ラッパで他方式へ広げる。
- **`contains` は残す**（§1.5）。「主キー/条件での軽量な存在チェック（materialize しない）」と再定義。

---

## 1. 永続化層（shazo）

### 1.1 `gather` の N+1 と、大量データの扱い 🔴
- **現状**: `gather` = `catalog`（キー列挙）＋ 各キー `retrieve`。N 件で N+1 クエリ。
  `catalog`/`gather` は結果を**全件メモリに載せる**（ストリーミング/カーソルなし）。
- **なぜ**: 大きな一覧で N+1 とメモリ消費が効く。旧 BOR は `maxLimit` で抑えていた。
- **方向性**: (a) `gather(query, limit)` ／ ページング（offset / keyset）、(b) 子を
  `WHERE id IN (...)` でバッチ取得して 1〜数クエリで多数構築する一括経路、(c) `Stream<T>` /
  カーソルでの遅延読み出し。まず (a)（ページング＋上限）を入れるのが安全。

### 1.2 並び順が backend 依存 🟡
- **現状**: `gather` の順序は `catalog`（ストレージ）順。JDBC は SQL の `ORDER BY` で揃うが、
  file store は内容フィールド（updatedAt 等）でソートできない。
- **方向性**: `catalog` のソートを backend に持たせる（例: `FileCommand.List` に Comparator）。
  あるいは「順序保証は catalog クエリ次第」と契約として明文化。

### 1.3 HTTP トランスポートが Repository 契約を完全に満たしていない 🔴
- **原則**: sharaku は「ストレージだけでなく**トランスポートも置換可能**」を志向した
  （RMI / HTTP の存在理由）。契約メソッドが特定トランスポートで動かないのは抽象の破綻＝不可。
- **現状（精査）**:
  - `contains` / `store` / `delete` / `retrieve` / `gather` … ✅ HTTP 越しで動作。
    （`gather` は `OP_CATALOG` → サーバの `gather` → エンコード済みオブジェクト列）。
  - `find` … ⚠️ **不完全**。アダプタが**クライアント側で `retrieve().orElseThrow(NotFound)`** に
    化けており、サーバの `find` を呼ばない＝ **MultipleFound を検査しない**。
  - `catalog`（RawResult） … ❌ **未対応**（`UnsupportedOperationException`）。
- **直し方（プロトコル拡張。例外搬送 `STATUS_EXCEPTION` は既存）**:
  1. **OP_FIND**: サーバで `find()` を実行し、T か NotFound / MultipleFound を返す。
     アダプタはサーバ find に委譲（クライアント偽装を廃止）。
  2. **OP_CATALOG（生の表）**: `catalog()` → RawResult を**型付き行フォーマット**で転送
     （列名＋セルの型タグ＋値）。**Java シリアライズの全 Object 許可は避け**、スカラ型
     （String / 数値 / Boolean / 日時 / byte[] 等）限定の安全なエンコードにする（gadget chain 回避）。
  3. 現 `OP_CATALOG`（中身は gather）を **OP_GATHER** に改名し、生 catalog 用に OP_CATALOG を分離。
- **補足**: 「現代なら REST でも」はトランスポート実装の選択肢の話。まず契約の透過性
  （全メソッドが動く）を満たすのが先。

### 1.4 bare `retrieve` の接続一貫性 🟡
- **現状**: `executeEach` の既定は**コマンドごとに `execute` を呼ぶ**。UoW 内では同一接続だが、
  トランザクション外の素の `JdbcRepository.retrieve` は**コマンドごとに接続を借り直す**
  （スナップショット非一貫・借用回数増）。
- **方向性**: `JdbcRepository` で `executeEach` を 1 接続実行にオーバーライド。

### 1.5 `contains` / `verifier` の位置づけ再定義 🟢
- **現状**: `contains` は元々 `store` の exists フラグ用だったが、その用途は廃止された。
  ただし `contains` は `containsCommands`（例 `SELECT 1 WHERE id=?`）＋ `verifier` で、
  **infuse しない**＝オブジェクトを作らない軽量チェック。`retrieve` は infuse を伴い重い。
- **判断**: `contains` は残す。意義を「**主キー/条件での軽量な存在チェック（materialize しない）**」
  に書き直す。`verifier` はその「found 判定基準」として contains に紐づけて残す（既定 nonEmpty）。
- **方向性**: ドキュメント上の再定義のみ（実装変更不要）。

### 1.6 `Infuser` の二次クエリ（多段・多枝の取得戦略）🟢
- **現状**: `Infuser` は実行済みコマンドの `Results` しか受け取らず、自分で追加クエリを発行できない。
- **判断**: 現場で必要になった実感がない（優先度低）。多段・多枝のバッチ取得は §1.1 と併せて
  いずれ検討。

### 1.7 `RawResult` の値型安全と `Producer` 活用 🟢
- **現状**: 行は `Map<String,Object>`。infuser 内のキャストで実行時型エラーになりうる。
  `Producer`（型変換器）はあるが活用は任意。
- **方向性**: infuser ヘルパ（`row.string("id")` 等）や Producer の標準化。

---

## 2. アプリ層（backbone）

### 2.1 マルチテナント 🔴
- **現状（動作の実態）**:
  - **サービス実行**は tenant→DataSource ルーティング済み（`transactorFor(tenant)` /
    `ctx.tenant()`）。リポジトリ操作はテナントの DB に正しく走る。✅
  - **Durable events（Outbox）は単一 DataSource 固定**＝実質シングルテナント。
    `tenantRouter` だけ＋`durableEvents` は build 例外。`dataSource`＋`tenantRouter`＋
    `durableEvents` 併用時は「**テナント接続に write／単一 DS を poll**」でズレ、配信されない
    （テナント DB に `backbone_outbox` 表が無い可能性も）。⚠️
  - **TimerScheduler は単一 DataSource**・テナント文脈なし。⚠️
  - **schema-per-tenant 未対応**（DataSource 分離のみ。旧 `<S>` 相当なし）。❌
- **なぜ重要**: 実アプリでは Outbox / Timer もテナント単位で正しく回る必要がある。
- **決定した設計**（→ 冒頭「決定ログ」参照）:
  - 公開 API: **`forTenant` ハンドル ＋ `withTenant`（ThreadLocal）**。方式非依存。
  - 方式: **`tenant → DataSource` を継ぎ目に、DataSource ラッパで DB-per-tenant /
    schema-per-tenant / RLS を切替**（shazo の `SessionInitDataSource`）。
- **実装済み（shazo 0.1.6 / backbone 0.1.9）**:
  - ✅ `SessionInitDataSource`（接続時 init SQL）＝ schema-per-tenant / RLS の継ぎ目。
  - ✅ `ServiceRunner.forTenant(...)` / `withTenant(...)`、`ctx.tenant()`。
  - ✅ **テナントごとの Outbox**: publish はテナントの接続へ書き、テナントの DataSource
    （自前 `backbone_outbox`）で poll。遅延生成。`TenantRunner` に per-tenant カウント。
  - ✅ **TimerJob のテナント実行**（backbone 0.1.10）: `TimerScheduler.tenantRouter(...)`、
    テナント束縛ジョブ（`schedule(name, cron/interval, tenant, job)`）＋全テナント fan-out
    （`scheduleForEachTenant(...)`、発火時にテナント集合を評価）。
  - ✅ E2E テスト（schema-per-tenant on H2: データ分離 / ambient / テナント別 durable 配信 /
    テナント別タイマー / fan-out）。
- **残るフォローアップ**:
  1. `TenantRunner` の管理メソッド拡充（retry/discard/一覧。今は count のみ。runner レベルは
     default テナント）。
  2. Timer 内部 runner に describer レジストリを渡す（現状ジョブは `ctx.connection()` の生 SQL、
     または要 registry）。

### 2.2 認可（Authorization）フックがない 🔴
- **現状**: `ServiceRunner` は `Principal` を運ぶが、**ロール / 権限の検査機構がない**。
  旧フレームワークには `AuthorizationFailedException` があった。
- **方向性**: サービス登録 / 実行時の認可フック（必要ロール宣言、または `ServiceRunner` の
  認可コールバック）。横断的関心事として上層で挟む。

### 2.3 トランザクション伝播が `REQUIRED` のみ 🟡
- **現状**: `ctx.call` は同一トランザクションに join（＝REQUIRED）だけ。
- **方向性**: `REQUIRES_NEW`（別トランザクション）等の伝播オプション。要否を見極めて。

### 2.4 可観測性（metrics / tracing）🟡
- **現状**: `BackboneConsole` の introspection と SQL トレースはあるが、構造化メトリクス
  （Micrometer 等）や分散トレーシング（OpenTelemetry）のフックは無い。
- **方向性**: サービス実行・Outbox 配信・タイマー実行の計測フック。

### 2.5 ワークフロー / サーガ 🟢
- **現状**: 旧 soba / tailwalk には承認ワークフロー・状態機械があった。backbone は意図的に
  持たない（汎用化が重い）。長時間処理・補償（saga）プリミティブも無い。
- **方向性**: 別ライブラリとして切り出す候補（backbone コアには入れない）。

---

## 3. 横断・運用・配布

### 3.1 CI（GitHub Actions）🔴
- **現状**: JitPack 依存。push 時の自動ビルド / テスト（CI ワークフロー）が無い（要確認）。
- **方向性**: 両リポジトリに build + test の GitHub Actions。退行検知の土台。

### 3.2 Maven Central 公開 🟡
- **現状**: JitPack のみ。署名付きアーティファクト / 正式 POM での Central 公開は未。
- **方向性**: 広く使うなら Central（OSSRH、署名、メタデータ）。

### 3.3 1.0 への道 🟡
- **現状**: 0.1.x で破壊的変更を多数実施。API がまだ流動的。
- **方向性**: API 凍結基準、非互換 / 非推奨ポリシー、1.0 のスコープ定義。

### 3.4 ベンチマーク（JMH）🟢
- **現状**: 性能計測なし。N+1 gather、`doCacheSQL`（PreparedStatement キャッシュ）等の効果が未測定。
- **方向性**: 代表操作の JMH ベンチ。

### 3.5 エラー型の細分化 🟢
- **現状**: `ShazoException` が広い。制約違反 / 接続喪失 / タイムアウト等が一括り
  （autoReconnect は既存）。
- **方向性**: 典型失敗の型付け。

---

## 当面の推奨着手順（私見）

1. **CI（3.1）** — 退行検知の土台。最優先。
2. **HTTP 契約の透過化（1.3）** — トランスポート置換可能という中核思想の回復。
3. **マルチテナント（2.1）** — アプリで現に必要。Outbox / Timer / スキーマ分離まで。
4. **認可フック（2.2）** — 実用アプリで必須。旧にもあった欠落。
5. **gather のページング / 上限（1.1）** — N+1・メモリの実害対策。

その後、可観測性（2.4）・トランザクション伝播（2.3）・Maven Central（3.2）・1.0 への道（3.3）。

---

*— 2026年、ReturnOfSharaku の作業時点での棚卸し。*
