# Event-Driven Notification System: Uygulama Yol Haritası (v2)

> Insider One Software Engineer Assessment (Golang) için tasarım ve uygulama planı.
> Bu sürüm zaman kısıtı olmadan hazırlandı. Önceliğimiz bir MVP'den çok, üretime yakın kalitede bir sistem.
> Her kararın gerekçesi yanında yazılı.

## v1'e göre ne değişti?

| Alan | v1 | v2 |
|---|---|---|
| Event-driven omurga | Redis pub/sub ile status yayını (fire-and-forget) | **Transactional outbox + Redis Streams.** Domain event'leri kaybolmaz, consumer group'larla işlenir |
| Provider | Sadece webhook.site | webhook.site (varsayılan, PDF gereği) + **docker-compose içinde fault-injection'lı mock provider**. webhook.site'ın ücretsiz istek limiti yük testine yetmiyor |
| API sözleşmesi | Elle yazılmış OpenAPI | **Spec-first:** `openapi.yaml` dosyasından `oapi-codegen` ile tip ve interface üretimi. Spec ile kod birbirinden kopamaz |
| SQL | Elle yazılmış pgx | Statik sorgular için `sqlc` (tip güvenli), dinamik liste filtresi için elle yazılmış builder |
| Redis arızası | Tanımsız | Yerel limiter'a **güvenli düşüş** (degraded mode), readiness'ta raporlanır |
| Müşteriye görünürlük | Sorgu + WebSocket | Ek olarak bildirim başına **status callback (webhook)**, HMAC imzalı |
| Güvenlik | Yok | API key, body limitleri, timeout'lar, callback için SSRF koruması |
| Test | Unit + integration + e2e | Ek olarak OpenAPI contract testleri, fuzz, chaos (worker öldürme), k6 yük testi ve sonuçların README'de yayımlanması |
| Operasyon | Prometheus | Ek olarak provision edilmiş **Grafana dashboard'u**, alert kuralları, veri saklama (retention) job'u |
| Dokümantasyon | README | Ek olarak **ADR'ler** (mimari karar kayıtları), runbook, `.http` örnek dosyası |
| CI | Lint + test + build | Ek olarak `govulncheck`, Trivy image taraması, coverage raporu, Dependabot |

---

## 1. Teknoloji Kararları

| Konu | Seçim | Gerekçe |
|---|---|---|
| Dil | Go 1.23 | PDF "1.2x" istiyor |
| HTTP router | `go-chi/chi` | Hafif, `net/http` uyumlu, oapi-codegen'in chi desteği var |
| API kodu | `oapi-codegen` (spec-first) | Request ve response tipleri spec'ten üretilir, el ile senkron tutmak gerekmez |
| Veritabanı | PostgreSQL 16 + `pgx/v5` | Asıl kayıt yeri; transaction, `SKIP LOCKED` ve `LISTEN/NOTIFY` desteği var |
| Sorgular | `sqlc` + dinamik liste için küçük bir builder | Derleme zamanında tip güvenliği |
| Migration | `golang-migrate` | Versiyonlu up/down SQL dosyaları |
| İş kuyruğu | **PostgreSQL tabanlı** (`FOR UPDATE SKIP LOCKED`), `Queue` interface'inin arkasında | §1.1 |
| Event bus | **Transactional outbox → Redis Streams** | §1.2 |
| Rate limit | Redis üzerinde GCRA / token bucket (Lua) | Tüm instance'larda ortak limit |
| Log | `log/slog` (JSON) | Standart kütüphane |
| Metrik | Prometheus client + Grafana | |
| Tracing | OpenTelemetry + Jaeger | |
| Circuit breaker | `sony/gobreaker` | |
| WebSocket | `coder/websocket` | Bakımı aktif, context destekli |
| Test | `testify`, `testcontainers-go`, `kin-openapi` (contract), Go fuzzing, k6 | |
| Kalite | `golangci-lint`, `govulncheck`, Trivy | |

### 1.1 İş kuyruğu neden Postgres? (ADR-001)

Bildirimi DB'ye yazıp ardından RabbitMQ veya Kafka'ya ayrıca publish etmek *dual-write* problemi yaratır: biri başarılı olup diğeri başarısız olabilir. Postgres kuyruğu tek transaction içinde şunları sağlar:

- kalıcılık,
- öncelik,
- gecikmeli retry,
- zamanlanmış gönderim,
- atomik iptal.

Bunun için ek bir altyapı gerekmez.

**Kapasite:** Kanal başına 100/s limitle sistemin tavanı 300 msg/s, yani günde yaklaşık 26 milyon mesaj. Bu, "günde milyonlarca" hedefini karşılar. Postgres `SKIP LOCKED` kuyruğu bu hacmin çok üzerine çıkabilir; yük testiyle (§13) ölçüp README'ye yazacağız.

**Geçiş yolu:** `Queue` interface'i sayesinde ölçek büyürse Kafka'ya geçilebilir. Bunun için kanal başına topic, öncelik başına topic ve retry için gecikmeli topic'ler gerekir. README'de ve ADR'de anlatılacak.

### 1.2 Event-driven omurga: Transactional Outbox (ADR-002)

Her durum değişikliği, aynı transaction içinde `outbox_events` tablosuna bir domain event'i olarak yazılır. Event tipleri:

- `notification.created`
- `notification.status_changed`
- `batch.completed`

Bir **outbox relay** goroutine'i bu event'leri Redis Streams'e (`stream:notification-events`) taşır ve gönderilenleri işaretler. Consumer'lar şunlardır:

- **WebSocket hub'ları:** Her API instance'ı kendi consumer'ı ile okur ve bağlı client'lara yayınlar.
- **Callback dispatcher:** Müşterinin verdiği `callback_url` adresine imzalı POST atar.
- **Metrik toplayıcı:** Gerekirse.

Redis pub/sub'dan farkı: event'ler kaybolmaz (stream kalıcıdır, consumer group'ta ACK vardır), consumer yeniden başlarsa kaldığı yerden devam eder. "Event-driven" başlığını gerçek anlamda karşılayan kısım burası.

---

## 2. Klasör Yapısı

```
insider-one/
├── cmd/
│   ├── api/                   # HTTP API + WebSocket + outbox relay
│   ├── worker/                # Kuyruk worker'ları + reaper + retention + callback dispatcher
│   └── mockprovider/          # Fault-injection'lı sahte provider (lokal ve yük testi için)
├── internal/
│   ├── config/                # env → struct, doğrulama (başlangıçta fail-fast)
│   ├── domain/                # Entity'ler, value object'ler, state machine, domain hataları, event'ler
│   ├── validation/            # Kanal bazlı içerik ve alıcı kuralları, SMS segment hesabı
│   ├── template/              # Şablon render
│   ├── service/               # Use-case'ler (create, batch, cancel, list, retry, template)
│   ├── repository/postgres/   # sqlc çıktıları + dinamik sorgular + tx yöneticisi
│   ├── queue/                 # Queue interface + postgres implementasyonu
│   ├── outbox/                # Outbox writer + relay
│   ├── events/                # Redis Streams publisher/consumer
│   ├── ratelimit/             # Redis GCRA (Lua) + yerel fallback + in-memory (test)
│   ├── provider/              # HTTP client, hata sınıflandırma, circuit breaker sarmalayıcı
│   ├── retry/                 # Backoff politikası
│   ├── worker/                # Dispatcher, kanal havuzları, işleme döngüsü, reaper
│   ├── callback/              # Müşteri webhook'ları, HMAC imzası, SSRF koruması
│   ├── http/
│   │   ├── api/               # oapi-codegen çıktısı (generated)
│   │   ├── handler/           # Strict server implementasyonu
│   │   ├── middleware/        # auth, correlation-id, logging, recovery, tracing, metrics, body limit
│   │   └── ws/                # WebSocket hub
│   ├── observability/         # logger, metrics, tracer kurulumu
│   └── platform/              # graceful shutdown, health check kayıtları, clock (test edilebilir zaman)
├── migrations/
├── api/openapi.yaml
├── deploy/
│   ├── prometheus/            # prometheus.yml, alert kuralları
│   └── grafana/               # datasource + dashboard provisioning
├── docs/
│   ├── adr/                   # Mimari karar kayıtları
│   ├── architecture.md        # Diyagramlar (Mermaid): bileşen, sequence, state
│   └── runbook.md             # Operasyon: DLQ boşaltma, devre açık kalırsa ne yapılır vb.
├── test/
│   ├── integration/
│   ├── e2e/
│   └── load/                  # k6 senaryoları
├── examples/requests.http     # VS Code / JetBrains HTTP client örnekleri
├── .github/workflows/ci.yml
├── .github/dependabot.yml
├── .golangci.yml
├── Dockerfile                 # Multi-stage, distroless, non-root, tek image + üç binary
├── docker-compose.yml
├── Makefile
├── .env.example
└── README.md
```

Projede saf bir hexagonal mimari dayatmıyoruz, ama bağımlılık yönü tektir: `handler → service → (repository, queue, provider interface'leri)`. `domain` hiçbir şeye bağımlı değildir. Zaman (`clock`) ve rastgelelik (jitter) enjekte edilir, böylece testlerde deterministik çalışır.

---

## 3. Veri Modeli (Migration'lar)

### `notifications`
| Kolon | Tip | Not |
|---|---|---|
| `id` | UUID v7 PK | Zamana göre sıralı, indeks dostu |
| `batch_id` | UUID NULL FK | |
| `channel` | enum `sms, email, push` | |
| `priority` | smallint | 0 = high, 1 = normal, 2 = low |
| `recipient` | text | |
| `subject` | text NULL | Email için |
| `content` | text | Render edilmiş son içerik |
| `template_id`, `template_vars` | UUID NULL, JSONB | İz sürmek için |
| `status` | enum `pending, scheduled, processing, sent, failed, cancelled` | |
| `idempotency_key` | text NULL | `UNIQUE` (kısmi, NULL hariç) |
| `attempts`, `max_attempts` | int | |
| `next_attempt_at` | timestamptz | Kuyruk sırası, retry zamanı ve schedule için tek alan |
| `locked_until`, `locked_by` | timestamptz, text | Worker lease'i |
| `scheduled_at`, `sent_at`, `failed_at`, `cancelled_at` | timestamptz NULL | |
| `provider_message_id` | text NULL | Provider'ın döndürdüğü `messageId` |
| `last_error`, `last_error_code` | text | |
| `callback_url` | text NULL | Opsiyonel status webhook'u |
| `metadata` | JSONB | Müşterinin serbest etiketleri (kampanya ID'si vb.) |
| `correlation_id`, `trace_parent` | text | Log ve trace zinciri |
| `created_at`, `updated_at` | timestamptz | |

İndeksler:
- Kuyruk için kısmi indeks: `(channel, priority, next_attempt_at) WHERE status IN ('pending','scheduled')`
- Lease süresi dolan kayıtlar için kısmi indeks: `(locked_until) WHERE status = 'processing'`
- Listeleme için: `(created_at DESC, id DESC)`, `(status, created_at)`, `(channel, created_at)`, `(batch_id)`

### `batches`
`id`, `total`, `metadata`, `correlation_id`, `created_at`. Durum özeti sorgu anında `GROUP BY status` ile hesaplanır. Tüm bildirimler son duruma gelince `batch.completed` event'i üretilir.

### `notification_attempts`
`id`, `notification_id`, `attempt_no`, `status_code`, `error_class` (transient/permanent), `error`, `provider_message_id`, `duration_ms`, `created_at`. Audit ve debug amaçlı tutulur; `GET /notifications/{id}` yanıtında gösterilir.

### `templates`
`id`, `name` UNIQUE, `channel`, `subject`, `body`, `variables` (beklenen değişken listesi, JSON), `version`, `created_at`, `updated_at`. Şablon güncellenince versiyon artar; bildirim hangi versiyonla render edildiğini saklar.

### `idempotency_keys`
`key` PK, `request_hash`, `response_status`, `response_body` JSONB, `locked_at`, `created_at`. TTL 24 saat; temizliği retention job yapar. `locked_at` alanı, aynı key ile eşzamanlı gelen iki isteğin ikisinin de işlenmesini engeller (§7).

### `outbox_events`
`id` BIGSERIAL, `aggregate_id`, `event_type`, `payload` JSONB, `created_at`, `published_at` NULL. Relay `published_at IS NULL` kayıtları sırayla gönderir.

### `callback_deliveries`
Müşteri webhook denemelerinin kaydı: `id`, `notification_id`, `event_id`, `attempt_no`, `status_code`, `next_attempt_at`, `status`.

### Veri saklama
Retention job son durumdaki (`sent/failed/cancelled`) ve `RETENTION_DAYS` (varsayılan 30) günden eski kayıtları siler. Ayrıca yayımlanmış outbox event'lerini ve süresi dolmuş idempotency key'lerini temizler. README'de, ölçek büyüdüğünde `created_at` ile aylık **partitioning** yapmanın kayıtları silmeyi `DROP PARTITION` kadar ucuz hale getireceği not edilir.

---

## 4. Durum Makinesi

```
           ┌──────────► cancelled (sadece pending/scheduled'dan)
           │
pending/scheduled ──claim──► processing ──202──► sent
           ▲                     │
           │   geçici hata,      │ kalıcı hata veya deneme hakkı bitti
           └── attempts<max ─────┤
               (next_attempt_at  ▼
                ileri alınır)   failed  ──(manuel /retry)──► pending
   lease süresi dolarsa (reaper):
   processing ──────────────────► pending
```

- Geçişler `domain` paketinde tek bir tabloda tanımlanır. Geçersiz geçiş `ErrInvalidTransition` döner ve tüm geçişler table-driven testlerle korunur.
- DB güncellemeleri koşullu yapılır (`UPDATE ... WHERE id=$1 AND status='processing' AND locked_by=$me`). Etkilenen satır sayısı 0 ise başka bir aktör kaydı değiştirmiş demektir ve işlem güvenle bırakılır.
- **İptal ve claim yarışı:** İptal `WHERE status IN ('pending','scheduled')` koşuluyla yapılır. Worker kaydı claim ettiyse iptal 409 döner ("zaten gönderiliyor"). İki aktör aynı anda kazanamaz.
- Her geçiş aynı transaction içinde bir outbox event'i yazar.

---

## 5. API (`/api/v1`)

### 5.1 Endpoint'ler

| Method | Path | Açıklama |
|---|---|---|
| POST | `/notifications` | Tekli oluşturma. Yanıt 201. `Idempotency-Key` header'ı desteklenir |
| POST | `/notifications/batch` | 1–1000 bildirim. Davranış aşağıda |
| GET | `/notifications/{id}` | Detay + deneme geçmişi |
| GET | `/notifications` | Filtreler: `status, channel, priority, batch_id, created_from, created_to`. Cursor pagination (`limit` ≤ 100, `cursor`) |
| POST | `/notifications/{id}/cancel` | Sadece pending/scheduled. Değilse 409 |
| POST | `/notifications/{id}/retry` | `failed` durumundakini yeniden kuyruğa alır (DLQ'dan çıkarma). `attempts` sıfırlanır |
| GET | `/batches/{id}` | Özet (status başına sayılar, ilerleme yüzdesi) |
| GET | `/batches/{id}/notifications` | Batch içeriği, sayfalı |
| POST | `/batches/{id}/cancel` | Batch'teki bekleyenleri iptal eder, kaç tane iptal edildiğini döner |
| POST | `/templates` | Şablon oluşturma (render ön-doğrulamasıyla) |
| GET | `/templates`, `/templates/{id}` | |
| PUT | `/templates/{id}` | Yeni versiyon oluşturur |
| DELETE | `/templates/{id}` | Soft delete |
| POST | `/templates/{id}/preview` | Değişkenlerle render edip sonucu döner, kaydetmez |
| GET | `/ws` | WebSocket. Abonelik mesajıyla `notification_id` / `batch_id` seçilir |
| GET | `/health` | Liveness |
| GET | `/ready` | DB + Redis ping, bileşen bazlı durum (`ok/degraded/down`) |
| GET | `/metrics` | Prometheus |
| GET | `/api/v1/metrics` | JSON özet (kuyruk derinliği, başarı/hata oranları, p50/p95/p99 latency) |
| GET | `/docs`, `/openapi.yaml` | Swagger UI ve spec |

### 5.2 Batch semantiği
- **Varsayılan davranış "hepsi ya da hiçbiri":** Tek bir item geçersizse 422 döner ve hata detayında item index'i ile alan adı yer alır. Hiçbir kayıt yazılmaz.
- Bu tercih, kampanya gönderen bir müşteri için "yarısı gitti, yarısı gitmedi" belirsizliğini ortadan kaldırır.
- Yazma işlemi `COPY` ile yapılır, böylece 1000 satır tek round-trip ile yazılır.
- Batch içindeki her item'ın kendi `idempotency_key` alanı olabilir.

### 5.3 Ortak kurallar
- **Kimlik doğrulama:** `X-API-Key` header'ı. Anahtarlar env'den okunur. `/health`, `/ready` ve `/metrics` hariç her yerde zorunludur.
- **Hata formatı:** RFC 7807 tarzı, tek tip:
  ```json
  {"error": {"code": "validation_failed", "message": "...", "details": [{"field": "items[3].recipient", "reason": "..."}]}, "correlation_id": "..."}
  ```
- **Pagination:** Cursor, `(created_at, id)` çiftinin base64 kodlanmış hali. Milyonlarca satırda `OFFSET` yavaşladığı için cursor kullanılır. Yanıtta `next_cursor` ve `has_more` döner.
- **Limitler:** Request body en fazla 5 MB (1000 email için yeterli), handler timeout 10 sn, `ReadHeaderTimeout` tanımlı (Slowloris'e karşı).
- **Tarih formatı:** Her yerde RFC 3339 / UTC.

---

## 6. Validation Kuralları

| Kanal | Alıcı | İçerik |
|---|---|---|
| SMS | E.164: `^\+[1-9]\d{7,14}$` | GSM-7 karakter setinde 160, UCS-2'de (Türkçe karakter veya emoji) 70 karakter tek segment. **Çok parçalı SMS desteklenir:** en fazla 6 segment (GSM-7: 153×6, UCS-2: 67×6). Segment sayısı hesaplanır ve kayda yazılır |
| Email | RFC 5322 (`net/mail`), en fazla 254 karakter | `subject` zorunlu, en fazla 255 karakter; gövde en fazla 100 KB |
| Push | Device token: boş olmayan, en fazla 4096 karakter (FCM token'ları uzun olabilir) | En fazla 1024 karakter; `subject` opsiyonel (başlık olarak kullanılır) |

Ortak kurallar:
- `priority` varsayılan olarak `normal`.
- `content` ile `template_id` birbirini dışlar; ikisinden biri zorunludur.
- `scheduled_at` geçmişte olamaz ve en fazla 30 gün ileride olabilir.
- `callback_url` sadece `https` olabilir ve SSRF koruması uygulanır (§9.6).
- `metadata` en fazla 4 KB.
- Template kullanılıyorsa önce render edilir, limitler render edilmiş içerik üzerinden kontrol edilir.
- Validation hataları tek seferde döner (ilk hatada durmaz), her hata alan yoluyla gösterilir.
- Validation fonksiyonları fuzz testinden geçirilir: hiçbir girdi panic üretmemeli.

---

## 7. Idempotency (İki Katman)

### 7.1 API katmanı (`Idempotency-Key` header'ı)
1. İstek gelince key için `INSERT ... ON CONFLICT DO NOTHING` ile "işleniyor" kaydı oluşturulur (`locked_at`).
2. Kayıt zaten varsa:
   - yanıtı kaydedilmişse ve request hash'i (SHA-256) aynıysa, ilk yanıt aynen döner (`Idempotent-Replayed: true` header'ıyla);
   - hash farklıysa 422 (`idempotency_key_reused`) döner;
   - hâlâ işleniyorsa 409 (`request_in_progress`) döner.
3. İşlem bitince yanıt kaydedilir. İşlem hata ile biterse, istemcinin tekrar deneyebilmesi için kayıt silinir.

Bu akış, eşzamanlı aynı-key istekleri de doğru yönetir. Integration testiyle kanıtlanır.

### 7.2 Bildirim katmanı
- `idempotency_key` alanında kısmi unique constraint vardır. Çakışan item mevcut kaydı döndürür ve yeni gönderim olmaz.
- Worker sadece `processing` durumundaki ve lease'i kendisine ait kaydı gönderir. `sent` olan bir kayıt asla tekrar gönderilmez.
- Provider'a `X-Idempotency-Key: <notification_id>` header'ı gönderilir.

### 7.3 Teslim garantisi
Sistem **at-least-once** çalışır. Worker provider'dan 202 aldıktan sonra, DB'yi güncelleyemeden çökerse lease dolar ve kayıt tekrar gönderilebilir. Bu pencere iki şekilde daraltılır:

- Provider çağrısından hemen sonra yapılan güncelleme kısa ve tek bir sorgudur.
- Provider tarafında idempotency key bulunur. Exactly-once garantisini yalnızca provider sağlayabilir.

Bu durum README'de açıkça yazılır.

---

## 8. İşleme Motoru (Worker)

### 8.1 Yapı
```
                 ┌─────────── Dispatcher (kanal başına) ───────────┐
LISTEN/NOTIFY ──►│  claim(batch=N) → yerel buffer → worker goroutine'leri │──► rate limiter ──► circuit breaker ──► provider
   + poll        └──────────────────────────────────────────────────┘
Reaper (lease dolanları geri alır) · Retention job · Queue depth sampler · Outbox relay
```

- **Kanal başına ayrı havuz:** SMS'in rate limit'e takılması ya da circuit breaker'ının açılması Email'i etkilemez.
- **Claim sorgusu:**
  ```sql
  UPDATE notifications
  SET status='processing', locked_until=now()+$lease, locked_by=$worker, attempts=attempts+1
  WHERE id IN (
    SELECT id FROM notifications
    WHERE channel=$1 AND status IN ('pending','scheduled') AND next_attempt_at <= now()
    ORDER BY priority, next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT $n)
  RETURNING ...;
  ```
  `SKIP LOCKED` sayesinde N worker instance'ı aynı kaydı almadan paralel çalışabilir.
- **Claim miktarı rate limit'e bağlıdır:** Bir seferde en fazla o anda harcanabilecek token kadar kayıt claim edilir. Böylece kayıtlar uzun süre `processing` durumunda bekleyip lease'i doldurmaz.
- **Öncelik ve aging:** Katı öncelik uygulanır. Ancak low önceliğin hiç işlenmeme (starvation) riskine karşı, 5 dakikadan uzun bekleyen kayıtların efektif önceliği bir kademe yükselir: `ORDER BY GREATEST(priority - floor(bekleme/5dk), 0), next_attempt_at`. Aging eşiği ayarlanabilir ve testle doğrulanır.
- **Lease ve reaper:** Lease 30 sn sürer; uzun süren provider çağrılarında lease uzatılır (heartbeat). Reaper, lease'i dolmuş `processing` kayıtlarını `pending` durumuna geri çeker ve `worker_lease_expired_total` metriğini artırır.
- **Uyanma stratejisi:** Insert sonrası `NOTIFY notifications_<channel>` gönderilir, worker hemen uyanır. Bağlantı koparsa ya da NOTIFY kaçarsa, yedek olarak 1 sn'lik poll devreye girer. Böylece gecikme düşük kalır ve boşta DB yükü olmaz.
- **Graceful shutdown:** SIGTERM gelince yeni claim yapılmaz, eldeki işler bitirilir (yapılandırılabilir timeout ile). Bitmeyenler lease süresi sonunda reaper tarafından geri alınır, yani hiçbir kayıt kaybolmaz.

### 8.2 Rate limiting
- Redis üzerinde Lua script ile atomik çalışan **GCRA** (token bucket'ın tek anahtarlı, bellek dostu türevi). Anahtar `ratelimit:{channel}`, limit 100/s, burst 100 (ayarlanabilir).
- Limit **tüm worker instance'ları genelinde** geçerlidir. Script izin verilmediğinde bekleme süresini döner, worker o kadar uyur. Kayıt kaybolmaz.
- **Redis erişilemezse:** Worker, `limit / beklenen_instance_sayısı` değeriyle çalışan yerel bir token bucket'a düşer. Bu muhafazakâr bir tercih: limiti aşmaktansa yavaş göndermek daha iyi. `/ready` "degraded" döner ve `rate_limiter_fallback_active` metriği 1 olur.
- Test: Birden çok goroutine ve instance simülasyonuyla 10 saniyelik pencerede hiçbir 1 saniyelik dilimde 100'ün aşılmadığı ölçülür.

### 8.3 Burst trafiği
API sadece DB'ye yazar ve hemen döner. Kuyruk tamponlama işini üstlenir, worker sabit hızla (100/s/kanal) eritir. Flash sale anında sistem çökmez, sadece kuyruk derinliği artar. Bu metriklerde ve Grafana'da görünür. Yük testi tam olarak bu senaryoyu ölçer (§13).

---

## 9. Teslimat ve Retry Tasarımı

### 9.1 Provider client
- Provider URL'i kanal başına yapılandırılabilir (`PROVIDER_URL_SMS`, `_EMAIL`, `_PUSH`). Ayrı tanımlanmamışsa ortak `PROVIDER_URL` kullanılır.
- Gönderilen istek PDF formatındadır: `{"to": "...", "channel": "sms", "content": "..."}`. Ek olarak header'larda `X-Idempotency-Key`, `X-Correlation-ID` ve `traceparent` gönderilir.
- Yanıt `{messageId, status, timestamp}` olarak parse edilir. `status != "accepted"` ya da `messageId` boşsa yanıt geçersiz sayılır.
- 10 sn timeout, connection pooling (`MaxIdleConnsPerHost` ayarlı), `context` ile iptal edilebilir, yanıt gövdesi en fazla 1 MB okunur.

### 9.2 Hata sınıflandırması

| Durum | Sınıf | Davranış |
|---|---|---|
| 202 + geçerli body | Başarılı | `sent`, `provider_message_id` kaydedilir |
| 2xx ama beklenmeyen body | Geçici | Retry, `error_code=invalid_provider_response` |
| 408, 429 | Geçici | `Retry-After` varsa ona uyulur (en fazla max delay kadar) |
| 5xx | Geçici | Backoff ile retry |
| Timeout, connection refused/reset, DNS hatası | Geçici | Backoff ile retry |
| 400, 401, 403, 404, 410, 413, 422 | Kalıcı | Retry yapılmaz, doğrudan `failed` |
| Diğer 4xx | Kalıcı | Aynı |

### 9.3 Backoff
- Formül: `delay = rand(0, min(base · 2^attempt, max))` (AWS "full jitter").
- Varsayılanlar: base 2 sn, max 5 dk, `max_attempts` 5. Hepsi env ile ayarlanabilir; `max_attempts` bildirim başına da verilebilir (1–10).
- Jitter, provider toparlandığında bütün retry'ların aynı anda saldırmasını (thundering herd) önler.
- Retry, kaydın `next_attempt_at` alanını ileri almaktan ibarettir. Worker içinde `sleep` yoktur; kayıt kuyruğa geri döner ve başka bir worker alabilir.
- **Retry kaydı öncelik kaybetmez:** high öncelikli bir bildirim retry'da da high kalır.

### 9.4 Circuit breaker (kanal başına)
- Devre, 10 saniyelik pencerede en az 20 istek varsa ve hata oranı %50'yi geçerse açılır.
- **Açıkken** dispatcher o kanaldan claim yapmaz. Kayıtlar kuyrukta bekler, retry hakları **harcanmaz**. Bu kritik bir nokta: provider kesintisi, bildirimlerin boşuna `failed` olmasına yol açmamalı.
- 30 sn sonra **half-open** durumuna geçilir; birkaç deneme isteği başarılı olursa devre kapanır.
- Devre durumu metrik olarak yayınlanır ve `/ready` sonucuna "degraded" olarak yansır.

### 9.5 DLQ
- Deneme hakkı biten ya da kalıcı hata alan kayıt `failed` olur; `last_error` ve `last_error_code` doldurulur.
- `failed` durumu DLQ görevi görür: `?status=failed` ile listelenir, `/retry` ile tekil olarak tekrar kuyruğa alınır.
- Runbook'ta "provider kesintisinden sonra DLQ'yu toplu boşaltma" prosedürü anlatılır (filtreli toplu retry SQL'i ve endpoint örneği).

### 9.6 Status callback'leri (müşteri webhook'u)
- `callback_url` verilmişse, son durum değişikliklerinde (`sent`, `failed`, `cancelled`) müşteriye POST atılır.
- Gövde event payload'ıdır. `X-Signature: sha256=HMAC(secret, timestamp + "." + body)` ve `X-Timestamp` header'ları eklenir, böylece müşteri isteğin bize ait olduğunu ve tekrar oynatılmadığını doğrulayabilir.
- Kendi retry politikası vardır (backoff, en fazla 5 deneme) ve `callback_deliveries` tablosuna kaydedilir.
- **SSRF koruması:** Sadece `https` kabul edilir. Hostname çözüldükten sonra private, loopback ve link-local IP'ler reddedilir; kontrol, DNS rebinding'e karşı bağlantı anında (custom dialer'da) tekrar yapılır. Lokal geliştirme için env ile kapatılabilir.

---

## 10. Gözlemlenebilirlik

### 10.1 Metrikler (Prometheus)
| Metrik | Tip | Label'lar |
|---|---|---|
| `notifications_created_total` | counter | channel, priority |
| `notifications_sent_total` | counter | channel |
| `notifications_failed_total` | counter | channel, reason |
| `notifications_cancelled_total` | counter | channel |
| `notification_retries_total` | counter | channel |
| `notification_queue_depth` | gauge | channel, priority, status (pending/scheduled/processing) |
| `notification_queue_oldest_age_seconds` | gauge | channel |
| `notification_delivery_latency_seconds` | histogram | channel, priority |
| `provider_request_duration_seconds` | histogram | channel, status_class |
| `rate_limit_wait_seconds` | histogram | channel |
| `rate_limiter_fallback_active` | gauge | |
| `circuit_breaker_state` | gauge | channel |
| `worker_lease_expired_total` | counter | |
| `outbox_lag_seconds` | gauge | |
| `callback_deliveries_total` | counter | result |
| `http_requests_total`, `http_request_duration_seconds` | counter, histogram | route, method, status |

Notlar:
- `notification_delivery_latency_seconds`, oluşturmadan `sent` olana kadar geçen süredir. Zamanlanmış bildirimlerde başlangıç noktası `scheduled_at` alınır.
- Kuyruk derinliği her istekte `COUNT(*)` ile hesaplanmaz. Arka planda 5 sn'de bir örnekleme yapılır, böylece DB'ye yük bindirilmez.
- `/api/v1/metrics` uç noktası aynı verilerin JSON özetini döner: kanal başına kuyruk derinliği, son 1/5/15 dakikanın başarı/hata oranları, p50/p95/p99 latency. PDF'teki "real-time metrics endpoint" maddesi doğrudan bununla karşılanır.

### 10.2 Grafana ve alert'ler
- Provision edilmiş bir dashboard docker-compose ile otomatik gelir. Panelleri: kanal başına throughput, kuyruk derinliği, hata oranı, latency yüzdelikleri, devre durumu, rate limit bekleme süreleri.
- Örnek alert kuralları:
  - kuyruktaki en eski kaydın yaşı 5 dk'yı geçerse,
  - hata oranı 5 dk boyunca %10'u geçerse,
  - devre 2 dk'dan uzun açık kalırsa,
  - outbox gecikmesi 30 sn'yi geçerse.

### 10.3 Loglar
- Format JSON (`slog`). Ortak alanlar: `time, level, msg, service, version, correlation_id, trace_id, span_id`; bağlama göre `notification_id, batch_id, channel, attempt` eklenir.
- Middleware `X-Correlation-ID` header'ını okur, yoksa üretir ve yanıta ekler. ID `context` içinde taşınır, DB'ye yazılır. Worker kaydı alınca aynı ID ile log basar ve provider'a da header olarak iletir. Böylece **API isteğinden provider çağrısına kadar tek bir ID ile iz sürülebilir**.
- PII koruması: Telefon numarası ve email loglarda maskelenir (`+90555***4567`). İçerik hiç loglanmaz.
- Log seviyesi env ile ayarlanır.

### 10.4 Tracing
- OpenTelemetry ile şunlar span olarak kaydedilir: HTTP (otelhttp), DB (pgx tracer), Redis, provider çağrısı ve worker işleme adımları.
- API'deki `traceparent` DB'ye yazılır. Worker span'ı bu değere **link** ile bağlanır; asenkron sınırda doğru model parent değil link'tir.
- Sonuçlar Jaeger UI'da görülebilir. Sampling oranı env ile ayarlanır.

### 10.5 Health
- `/health`: Process ayaktaysa her zaman 200 döner (liveness).
- `/ready`: DB ve Redis'i 1 sn timeout ile ping'ler. Bileşen bazlı JSON döner. DB yoksa 503; Redis yoksa veya bir devre açıksa 200 + `degraded`.

---

## 11. Bonus Özellikler (hepsi uygulanacak)

| Bonus | Uygulama |
|---|---|
| Failure handling | Circuit breaker, DLQ + manuel retry, lease/reaper, graceful shutdown, Redis fallback, outbox ile kayıpsız event'ler (§8, §9) |
| Scheduled notifications | `scheduled_at` dolu kayıt `scheduled` statüsüne girer, `next_attempt_at = scheduled_at` olur. Claim sorgusu doğru zamanda yakalar, ayrı bir scheduler gerekmez. Zamanı gelene kadar iptal edilebilir |
| Template system | Go `text/template` (`missingkey=error`). Şablon kaydında değişkenler ve kanal uyumluluğu doğrulanır. Versiyonlama ve `preview` endpoint'i var. Güvenlik için şablonlarda fonksiyon seti kısıtlıdır; sadece `upper`, `lower`, `default` gibi güvenli yardımcılar kullanılabilir |
| WebSocket updates | Client bağlanınca `{"action":"subscribe","notification_ids":[...],"batch_ids":[...]}` gönderir. Hub, Redis Streams'ten gelen event'leri filtreleyip iletir. Ping/pong ile canlılık kontrol edilir, yavaş client'ın tamponu dolarsa bağlantı kesilir (backpressure). Kimlik doğrulama query param veya ilk mesajdaki API key ile yapılır |
| Distributed tracing | OpenTelemetry + Jaeger, asenkron sınırda span link'leri (§10.4) |
| GitHub Actions CI/CD | §14 |

---

## 12. Güvenlik ve Sağlamlık

- API key doğrulaması `crypto/subtle` ile sabit zamanlı karşılaştırmayla yapılır.
- Request body limiti, JSON'da bilinmeyen alanlar reddedilir (`DisallowUnknownFields`), sunucu timeout'ları tanımlı.
- Panic recovery middleware'i var; stack trace loga yazılır, istemciye dönmez.
- SQL injection yok: tüm sorgular parametrelidir. Dinamik liste filtresi de whitelist'li builder ile kurulur.
- Callback'ler için SSRF koruması (§9.6).
- Container: distroless base image, non-root kullanıcı, read-only filesystem.
- Secret'lar sadece env'den okunur. `.env.example` dosyasında gerçek değer yoktur.
- Config başlangıçta doğrulanır; eksik veya hatalı değerde servis anlaşılır bir hata mesajıyla hiç ayağa kalkmaz (fail-fast).

---

## 13. Test Stratejisi

| Katman | Kapsam | Araç |
|---|---|---|
| Unit | Validation tabloları (SMS segment hesabı dahil), state machine, backoff (seed'li jitter, enjekte edilmiş clock), hata sınıflandırması, template render, cursor encode/decode, HMAC imzası, SSRF IP filtresi | `testing`, `testify` |
| Fuzz | Validation ve cursor decode: hiçbir girdi panic üretmemeli | Go native fuzzing |
| Handler | Her endpoint için başarılı ve hatalı akışlar (mock servis ile) | `httptest` |
| Contract | Tüm handler yanıtları OpenAPI spec'e karşı doğrulanır | `kin-openapi` |
| Integration | Repository sorguları; `SKIP LOCKED` altında eşzamanlı claim (aynı kayıt iki kez alınmamalı); eşzamanlı aynı-key idempotency; iptal-claim yarışı; outbox relay; Redis rate limiter'ın 100/s'ı aşmadığı | `testcontainers-go` |
| E2E | Gerçek API + worker + mock provider ile: 503→503→202 (retry ve attempts kaydı), 400 (doğrudan failed), provider kesintisinde devrenin açılması ve retry hakkının korunması, scheduled gönderim, batch iptali, WebSocket'e event gelmesi, callback imzası | `testcontainers-go`, mock provider |
| Chaos | Worker işlerin ortasında öldürülür; lease dolduktan sonra kayıtların başka worker tarafından tamamlandığı ve kayıp olmadığı doğrulanır | E2E içinde |
| Load | k6 senaryoları: (a) sürekli yük, (b) flash sale burst'ü (60 sn'de 50.000 bildirim), (c) provider %20 hata verirken. Ölçülenler: API p99 latency, kanal başına gerçekleşen gönderim hızının 100/s'ı aşmadığı, kuyruk erime süresi. **Sonuçlar README'de tablo olarak yayımlanır** | k6 + mock provider |

Komutlar:
- `make test`: unit + integration + e2e (Docker gerekir), `-race` açık.
- `make test-unit`: Docker gerektirmez, saniyeler içinde biter.
- `make test-load`: k6 senaryolarını çalıştırır.
- `make cover`: Coverage raporu üretir. Hedef: domain, service ve worker paketlerinde %80+ (sayı tek başına amaç değil; kritik yollar mutlaka test edilecek).

---

## 14. CI/CD (GitHub Actions)

- **Tetikleyiciler:** push ve pull request.
- **Job'lar** (paralel):
  1. `lint`: `golangci-lint` (errcheck, govet, staticcheck, gosec, revive, gofumpt…).
  2. `generate-check`: `oapi-codegen` ve `sqlc` çıktıları yeniden üretilir ve git diff boş olmalı. Böylece spec ile kod arasında kopukluk olamaz.
  3. `test-unit`: race detector ile çalışır, coverage artifact olarak yüklenir.
  4. `test-integration`: testcontainers ile çalışır (GitHub runner'da Docker mevcut).
  5. `security`: `govulncheck` ve Trivy ile image taraması.
  6. `build`: Docker image build edilir. Main branch'te GHCR'ye push edilir; bu CD kısmıdır.
- **Dependabot:** Go modülleri, GitHub Actions ve Docker base image'ları için.

---

## 15. Docker Compose

`docker-compose up` ile ayağa kalkan servisler:

| Servis | Port | Not |
|---|---|---|
| `postgres` | 5432 | healthcheck'li, volume'lu |
| `redis` | 6379 | healthcheck'li |
| `migrate` | — | Bir kere çalışır ve çıkar; `api` ve `worker` buna bağımlı (`service_completed_successfully`) |
| `api` | 8080 | |
| `worker` | — | `replicas: 2` ile yatay ölçeklenmeyi ve ortak rate limit'i gösterir |
| `mockprovider` | 9000 | Varsayılan provider. Hata oranı, gecikme ve 429 oranı env ile ayarlanır |
| `jaeger` | 16686 | |
| `prometheus` | 9090 | Alert kurallarıyla birlikte |
| `grafana` | 3000 | Dashboard provision edilmiş, anonim okuma açık |

- **webhook.site'a geçiş:** `.env` dosyasında `PROVIDER_URL=https://webhook.site/<uuid>` ayarlamak yeterli. Neden mock varsayılan? webhook.site'ın ücretsiz planında istek ve süre limitleri var; yük testi ve tekrarlanabilir hata senaryoları için kendi kontrolümüzdeki mock gerekiyor. README'de iki kullanım da adım adım anlatılır.
- `make up`, `make down`, `make logs`, `make seed` (örnek şablon ve bildirimler) komutları sağlanır.

---

## 16. Dokümantasyon

- **README.md:**
  - proje özeti,
  - 30 saniyede başlatma,
  - mimari diyagramı (Mermaid),
  - tasarım kararları ve trade-off'lar,
  - API örnekleri (curl),
  - konfigürasyon tablosu (tüm env değişkenleri),
  - test komutları,
  - yük testi sonuçları,
  - bilinen sınırlamalar ve "ölçek büyürse ne yapardık" bölümü.
- **docs/architecture.md:** Bileşen diyagramı, "bildirimin yaşamı" sequence diyagramı (create → claim → send → retry → event → WebSocket/callback), durum diyagramı.
- **docs/adr/:** Her mimari karar için kısa bir kayıt (bağlam, karar, sonuçlar):
  - 001 Postgres kuyruğu,
  - 002 Transactional outbox + Redis Streams,
  - 003 Redis GCRA rate limit + fallback,
  - 004 Retry ve circuit breaker politikası,
  - 005 At-least-once teslim garantisi,
  - 006 Spec-first API.
- **docs/runbook.md:** DLQ boşaltma, açık kalan devre, büyüyen kuyruk, Redis kesintisi durumunda ne yapılacağı.
- **api/openapi.yaml:** Tüm endpoint'ler, şemalar, örnekler ve hata yanıtları. `/docs` adresinde Swagger UI ile sunulur.
- **examples/requests.http:** IDE içinden tek tıkla çalıştırılabilir örnek istekler.

---

## 17. Adım Adım Uygulama Sırası

Commit'ler **Conventional Commits** formatında atılır (`feat:`, `fix:`, `test:`, `docs:`, `chore:`). Her commit derlenir ve testleri geçer. Her fazın sonunda "bitti" kriteri (DoD) kontrol edilir.

**İlerleme:** `[x]` tamamlandı, `[ ]` bekliyor. Her adım bitince işaretlenir ve o adımın commit'ine dahil edilir.

### Faz 0: Hazırlık
- [ ] 0. Go 1.27, Docker Desktop kurulumu ✅ · GitHub repo ✅ · webhook.site URL'i ⏳ (Adım 11'den önce gerekli).
   - webhook.site'ta URL açılır: "Edit" → status 202, content-type `application/json`, body `{"messageId":"$request.uuid$","status":"accepted","timestamp":"$time$"}` (webhook.site'ın değişken desteğiyle).
   - GitHub'da boş bir repo açılır.
   - **DoD:** `go version`, `docker compose version` çalışıyor; webhook.site'a atılan curl isteği 202 dönüyor.

### Faz 1: Temel
- [x] 1. **İskelet:** `go mod init`, klasör yapısı, config (fail-fast), logger, Makefile, `.golangci.yml`, `.gitignore`, `.env.example`, `git init`. İlk CI iskeleti (lint + build).
- [x] 2. **Altyapı:** Dockerfile ve docker-compose'un ilk hali (postgres, redis, migrate), graceful shutdown iskeleti, `/health` ve `/ready`.
- [x] 3. **Migration'lar ve domain:** Tüm tablolar; entity'ler, state machine ve testleri.
- [ ] 4. **Validation ve template motoru:** SMS segment hesabı dahil, table-driven testler ve fuzz.
   - **DoD:** `docker compose up` DB'yi migrate ediyor, `make test-unit` geçiyor.

### Faz 2: API
- [ ] 5. **OpenAPI spec (ilk tam hali)** ve `oapi-codegen` kurulumu.
- [ ] 6. **Repository katmanı:** sqlc sorguları, dinamik liste builder'ı, tx yöneticisi, `COPY` ile batch insert, integration testleri.
- [ ] 7. **Servis ve handler'lar:** create, batch, get, list, cancel, retry, template CRUD ve preview.
- [ ] 8. **Middleware'ler:** auth, correlation ID, request log, recovery, body limit; idempotency (eşzamanlılık testiyle birlikte).
   - **DoD:** Tüm endpoint'ler curl ile çalışıyor, contract testleri geçiyor.

### Faz 3: İşleme motoru
- [ ] 9. **Kuyruk:** Claim, lease, heartbeat, reaper, `LISTEN/NOTIFY` + poll.
- [ ] 10. **Rate limiter:** Redis GCRA Lua, yerel fallback, doğruluk testi.
- [ ] 11. **Provider client ve mock provider:** Hata sınıflandırması, fault injection.
- [ ] 12. **Worker:** Kanal havuzları, öncelik ve aging, claim miktarının limite bağlanması, graceful shutdown.
- [ ] 13. **Retry ve failure handling:** Backoff, `Retry-After`, circuit breaker, DLQ, attempts kaydı.
    - **DoD:** E2E testleri (retry, kalıcı hata, devre açılması, chaos) geçiyor; uçtan uca bir bildirim webhook.site'a ulaşıyor.

### Faz 4: Event'ler ve gerçek zamanlılık
- [ ] 14. **Outbox ve relay:** Redis Streams publisher, consumer group yönetimi.
- [ ] 15. **WebSocket hub:** Abonelik, filtreleme, ping/pong, backpressure.
- [ ] 16. **Status callback'leri:** HMAC imzası, retry, SSRF koruması.
- [ ] 17. **Scheduled bildirimler:** Uçtan uca test (scheduled → pending → sent ve iptal).
    - **DoD:** Bir batch gönderildiğinde WebSocket client'ı tüm durum değişikliklerini sırayla görüyor; callback alıcısı imzayı doğrulayabiliyor.

### Faz 5: Gözlemlenebilirlik
- [ ] 18. **Prometheus metrikleri** ve `/api/v1/metrics` JSON özeti, queue depth sampler.
- [ ] 19. **OpenTelemetry + Jaeger:** Span link'leri, log'larda `trace_id`.
- [ ] 20. **Grafana dashboard'u ve alert kuralları:** Provisioning dahil.
- [ ] 21. **Log'larda PII maskeleme.**
    - **DoD:** Tek bir bildirim, correlation ID ile loglardan ve trace ID ile Jaeger'dan uçtan uca takip edilebiliyor; dashboard canlı veri gösteriyor.

### Faz 6: Kalite ve kanıt
- [ ] 22. **E2E test setinin tamamlanması**; `make test` tek komutla her şeyi çalıştırıyor.
- [ ] 23. **k6 yük testleri:** Sonuçların ölçülmesi; gerekirse indeks ve batch boyutu ayarı (bulgular ADR'ye yazılır).
- [ ] 24. **CI'ın tamamlanması:** Generate-check, integration, govulncheck, Trivy, coverage, Dependabot, GHCR push.
- [ ] 25. **Güvenlik gözden geçirmesi:** gosec bulguları, container sertleştirme, secret taraması.
    - **DoD:** CI yeşil; yük testi kanal başına 100/s limitinin aşılmadığını ve burst'ün kayıpsız eridiğini gösteriyor.

### Faz 7: Teslim
- [ ] 26. **OpenAPI'nin son hali:** Örnekler ve hata yanıtları.
- [ ] 27. **Dokümantasyon:** README, architecture.md, ADR'ler, runbook, `requests.http`.
- [ ] 28. **Temiz kurulum provası:** Repo sıfırdan klonlanır, sadece `docker-compose up` ve README'deki adımlarla her şeyin çalıştığı doğrulanır. Commit geçmişi gözden geçirilir.
- [ ] 29. **Teslim:** Repo linki paylaşılır.

---

## 18. Gereksinim Karşılama Tablosu

| PDF gereksinimi | Karşılayan bölüm |
|---|---|
| Create (recipient, channel, content, priority) | §5, §6 |
| Batch (≤1000) | §5.2 |
| Status by ID / batch ID | §5.1 |
| Cancel pending | §4, §5 |
| List + filter + pagination | §5.1, §5.3 |
| Async queue workers | §8 |
| 100 msg/s/kanal rate limit | §8.2 |
| Priority queue (high/normal/low) | §8.1 |
| Content validation | §6 |
| Idempotency | §7 |
| Delivery & retry (aday tasarımı) | §9 |
| Metrics endpoint (queue depth, success/failure, latency) | §10.1 |
| Structured logging + correlation ID | §10.3 |
| Health check | §10.5 |
| webhook.site provider | §9.1, §15, §17 Faz 0 |
| Burst trafiği | §8.3, §13 (yük testi) |
| İç ekipler ve API kullanıcıları için görünürlük | §5, §9.6, §10, §11 (WebSocket) |
| Bonus: failure handling | §8, §9, §11 |
| Bonus: scheduled | §11 |
| Bonus: template | §11 |
| Bonus: WebSocket | §11 |
| Bonus: distributed tracing | §10.4 |
| Bonus: GitHub Actions CI/CD | §14 |
| Repo + temiz commit geçmişi | §17 |
| README | §16 |
| docker-compose up | §15 |
| Swagger/OpenAPI | §5, §16 |
| Versiyonlu migration'lar | §3 |
| Tek komutla test | §13 |
