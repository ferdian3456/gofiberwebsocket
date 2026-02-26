# Go Fiber WebSocket Chat Server

Server WebSocket real-time berbasis [Go Fiber](https://gofiber.io/) dengan arsitektur production-grade: Hub pattern, direct messaging, ping/pong heartbeat, graceful shutdown, dan telah diuji hingga **10.000 concurrent client** dengan **45.000 msg/sec** dan **0 errors**.

---

## Struktur Project

```
gofiberwebsocket/
├── server/
│   └── main.go       # WebSocket server
├── client/
│   └── main.go       # Interactive chat client (CLI)
└── benchmark/
    └── main.go       # Load testing tool
```

---

## Arsitektur

### Hub Pattern

Semua koneksi dikelola oleh sebuah `Hub` terpusat menggunakan `sync.Map` untuk concurrent-safe routing tanpa bottleneck lock.

```
Client A ──► ReadPump ──► Hub.route() ──► target.send channel ──► WritePump ──► Client B
```

### Per-Client Goroutines

Setiap client menjalankan 2 goroutine terpisah:

- **ReadPump** — membaca pesan masuk secara blocking, meneruskan ke Hub
- **WritePump** — menulis pesan keluar + mengirim ping heartbeat periodik

### Direct Messaging

Pesan diroute ke client tertentu berdasarkan field `to` di payload JSON — bukan broadcast ke semua client.

```json
{
  "to": "clientB",
  "msg": "halo!",
  "sent_at": 1772149376000000
}
```

---

## Teknologi & Library

| Library | Fungsi |
|---|---|
| `gofiber/fiber/v3` | HTTP/WebSocket framework |
| `gofiber/contrib/v3/websocket` | WebSocket middleware (server-side) |
| `fasthttp/websocket` | WebSocket client & konstanta |
| `bytedance/sonic` | JSON marshal/unmarshal (3x lebih cepat dari `encoding/json`) |
| `go.uber.org/zap` | Structured logging performa tinggi |

---

## Fitur

- ✅ Direct messaging (pesan ke client tertentu, bukan broadcast)
- ✅ Hub pattern dengan `sync.Map` (zero lock contention)
- ✅ Ping/Pong heartbeat otomatis (deteksi koneksi zombie)
- ✅ Read/Write deadline per koneksi (mencegah goroutine leak)
- ✅ Graceful shutdown (drain koneksi aktif sebelum mati)
- ✅ Message size limit (512 KB per pesan)
- ✅ Buffered send channel per client (1024 slots)
- ✅ Structured logging dengan `zap`
- ✅ Client CLI interaktif dengan reconnect & exponential backoff
- ✅ Support multiple target per client (`-to=clientA,clientB`)

---

## Cara Menjalankan

### Prerequisites

```bash
# Install dependencies
cd server && go mod tidy
cd client && go mod tidy
cd benchmark && go mod tidy
```

### 1. Jalankan Server

```bash
cd server
ulimit -n 200000   # naikkan file descriptor limit
go run main.go
```

### 2. Jalankan Client (CLI Chat)

Buka beberapa terminal, masing-masing jalankan client berbeda:

```bash
# Terminal 1 — Client A, chat ke B
go run main.go -name=clientA -to=clientB

# Terminal 2 — Client B, chat ke A
go run main.go -name=clientB -to=clientA

# Terminal 3 — Client C, hanya kirim ke B
go run main.go -name=clientC -to=clientB
```

**Flag client:**

| Flag | Keterangan | Contoh |
|---|---|---|
| `-name` | Nama client (unik) | `-name=clientA` |
| `-to` | Target penerima (bisa multiple, pisah koma) | `-to=clientB,clientC` |

Ketik pesan lalu Enter untuk mengirim. Ketik `/quit` untuk keluar dengan graceful.

---

## Format Pesan (JSON)

```json
{
  "from": "clientA",
  "to":   "clientB",
  "msg":  "halo bro!",
  "sent_at": 1772149376000000
}
```

Field `from` selalu di-set oleh server (tidak bisa di-spoof oleh client).

---

## Benchmark / Load Testing

### Setup

```bash
# Naikkan OS limits (lakukan sekali per sesi)
ulimit -n 200000

sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### Menjalankan Benchmark

```bash
cd benchmark

# Standard test: 1.000 client
go run main.go -clients=1000 -interval=200ms -duration=60s

# Load test: 5.000 client
go run main.go -clients=5000 -interval=200ms -duration=120s -ramp=15s

# Stress test: 10.000 client
go run main.go -clients=10000 -interval=200ms -duration=120s -ramp=30s

# Preview sample pesan
go run main.go -clients=100 -interval=200ms -duration=30s -verbose
```

### Flag Benchmark

| Flag | Default | Keterangan |
|---|---|---|
| `-clients` | 100 | Jumlah concurrent client |
| `-interval` | 500ms | Interval kirim pesan per client (±20% jitter) |
| `-duration` | 30s | Durasi benchmark |
| `-ramp` | 3s | Ramp-up time (client connect bertahap) |
| `-url` | ws://localhost:3000/ws | WebSocket server URL |
| `-verbose` | false | Tampilkan preview sample pesan |

### Simulasi Pesan Realistis

Benchmark menggunakan pool 80+ pesan realistis bahasa Indonesia yang terbagi dalam beberapa tier:

- **Singkat:** `"iya"`, `"ok"`, `"👍"`, `"wkwk"`
- **1 kalimat:** `"PRnya udah di-review belum?"`, `"ada bug di production nih bro"`
- **Beberapa kalimat:** diskusi teknis, update progress, pertanyaan
- **Paragraf panjang:** cerita bug production, proposal arsitektur, meeting reminder
- **Typo/salah kirim:** `"eh salah kirim, sorry"`, `"lol salah chat"` (realistis!)

Setiap client mendapat jitter ±20% dari interval yang ditentukan sehingga traffic tidak uniform — lebih mirip pola chat asli.

---

## Hasil Benchmark

Benchmark dijalankan di laptop (server dan benchmark di mesin yang sama):

| Clients | Msg/sec Sent | Msg/sec Recv | Avg Latency | Errors | Status |
|---|---|---|---|---|---|
| 1.000 | 4.892 | 4.865 | ~0ms | 0 | ✅ Santai |
| 5.000 | 23.812 | 23.296 | ~0ms | 0 | ✅ Optimal |
| **10.000** | **45.064** | **43.236** | **8ms** | **0** | **✅ Sweet spot** |
| 20.000 | 64.476 | 56.149 | 1.036ms | 499 | ⚠️ Mendekati batas |

**Sweet spot: ~10.000–15.000 concurrent client** di environment lokal.

> ⚠️ **Catatan:** Angka di atas adalah hasil benchmark lokal (server + benchmark di mesin yang sama, traffic via loopback). Untuk klaim kapasitas production, diperlukan benchmark dengan mesin terpisah, network asli, database, dan autentikasi.

---

## Optimasi yang Diterapkan

| Optimasi | Sebelum | Sesudah | Impact |
|---|---|---|---|
| JSON library | `encoding/json` | `sonic` | ~3x lebih cepat |
| Hub concurrency | `map + RWMutex` | `sync.Map` | Hilangkan lock contention |
| Logging | `log.Printf` | `zap` | Non-blocking, structured |
| Send buffer | 256 slots | 1024 slots | Toleransi burst traffic |
| OS file descriptor | 1.024 (default) | 200.000 | Support lebih banyak koneksi |
| OS socket buffer | Default | 16 MB | Kurangi packet drop |
| Go scheduler | Default (auto) | Default (GOMAXPROCS = NumCPU sejak Go 1.5) | Semua core dipakai |

---

## Keterbatasan & Next Steps

### Keterbatasan Saat Ini

- Tidak ada autentikasi (JWT, session)
- Tidak ada persistensi pesan (database)
- Single instance — belum support horizontal scaling

### Untuk Production / Kubernetes

Ketika di-deploy ke Kubernetes dengan HPA (Horizontal Pod Autoscaler), Hub lokal tiap pod perlu dikoordinasikan menggunakan message broker:

```
Pod 1 (Hub A) ──┐
Pod 2 (Hub B) ──┼──► Redis Pub/Sub / NATS / Kafka ──► semua pod
Pod 3 (Hub C) ──┘
```

Setiap pod subscribe ke broker. Pesan yang masuk di Pod 1 di-publish ke broker, lalu semua pod menerima dan forward ke client masing-masing.

Selain itu perlu:
- **Sticky session** di Ingress (WebSocket butuh koneksi persistent)
- **Autentikasi** (JWT validation sebelum upgrade ke WebSocket)
- **Database** untuk history pesan
- **Benchmark staging** dengan mesin terpisah sebelum go-live

---

## Konfigurasi Server

```go
const (
    writeWait      = 10 * time.Second  // timeout write per operasi
    pongWait       = 60 * time.Second  // timeout tunggu pong dari client
    pingPeriod     = 54 * time.Second  // interval kirim ping (90% dari pongWait)
    maxMessageSize = 512 * 1024        // max ukuran pesan (512 KB)
    sendBufSize    = 1024              // ukuran buffer channel per client
)
```

---

## Dependencies

```
server:
  github.com/gofiber/fiber/v3
  github.com/gofiber/contrib/v3/websocket
  github.com/fasthttp/websocket
  github.com/bytedance/sonic
  go.uber.org/zap

client:
  github.com/fasthttp/websocket
  github.com/bytedance/sonic

benchmark:
  github.com/fasthttp/websocket
  github.com/bytedance/sonic
```