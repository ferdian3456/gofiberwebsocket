package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/fasthttp/websocket"
)

// ─── Realistic Chat Messages ─────────────────────────────────────────────────

var chatMessages = []string{
	// ── Sapaan & buka percakapan ──
	"halo", "hai", "hey", "oi", "woy", "hei",
	"halo bro", "bro ada?", "bang ada?", "gan online?",
	"permisi boleh tanya?", "bisa minta tolong sebentar?",
	"lagi sibuk ga?", "free ga sekarang?", "ada waktu?",

	// ── Jawaban singkat / filler ──
	"iya", "ok", "siap", "oke", "bisa", "boleh",
	"ga bisa nih", "nanti ya", "sebentar", "tunggu dulu",
	"iya iya", "oh oke", "hmm", "hm oke deh",
	"wah", "oh gitu", "ooh", "haha", "wkwk", "lol",
	"😂", "👍", "🙏", "ok sip", "noted", "oke noted",
	"mantap", "gas", "siap bos", "oke bos",

	// ── Pertanyaan sehari-hari ──
	"udah makan belum?", "lagi ngapain?", "gimana kabarnya?",
	"udah sampe mana?", "jadi ga hari ini?", "kapan bisa ketemuan?",
	"lo lagi di mana?", "masih di kantor?", "udah pulang belum?",
	"hari ini masuk ga?", "besok ada acara?",

	// ── Chat kerja / project ──
	"PRnya udah di-review belum?", "ada bug di production nih bro",
	"meeting jam berapa tadi?", "tolong cek log servernya dong",
	"deploy kapan rencananya?", "ada error ga dari tadi?",
	"test udah pass semua?", "branch mana yang dipake buat ini?",
	"minta akses repositorynya dong", "dokumentasinya udah diupdate belum?",
	"ticketnya udah di-close?", "gimana progress featurenya?",
	"masih nunggu approval dari siapa?", "designnya udah final belum?",
	"QA udah ngecek belum?", "hotfix kapan mau dinaikin?",

	// ── Beberapa kalimat / medium ──
	"eh btw tadi gua cek lagi kodenya, kayaknya ada yang aneh di bagian query database. nanti gua ping ya kalau udah ketemu masalahnya.",
	"iya tadi sempet liat PRnya, tapi masih ada beberapa bagian yang perlu direfactor dulu sih. gua kasih comment ya nanti.",
	"gua lagi ngerjain ini dulu, estimasi sejam lagi kelar. habis itu langsung gua push ke staging.",
	"wah iya baru notice juga ada issue-nya. gua coba reproduce dulu di local, kalau udah ketemu root cause-nya gua update di ticket.",
	"kemarin emang servernya agak lambat, udah gua restart tadi pagi. sekarang harusnya udah normal lagi.",
	"oke nanti gua ping kamu kalau udah selesai review. mungkin sore atau besok pagi paling lambat.",
	"ini lagi stuck di bagian autentikasi JWT-nya, error terus pas di-refresh token. ada yang pernah ketemu kasus sama?",
	"tadi habis meeting sama PM, katanya deadline dimajuin jadi minggu depan. jadi kita perlu prioritasin dulu fitur utamanya.",
	"gua udah push fixnya, tolong di-pull dan test lagi ya. kalau udah oke langsung merge aja.",
	"btw infrastrukturnya lagi gua migrasi ke kubernetes, masih proses setup. minggu depan harusnya udah bisa dicoba.",

	// ── Chat santai / personal ──
	"nonton apa kemarin? ada rekomendasi film bagus ga?",
	"makan siang di mana? pengen nyobain tempat baru nih.",
	"weekend kemarin kemana? gua malah rebahan doang di rumah haha.",
	"btw tadi meeting panjang banget, 3 jam gaada hasil konkritnya.",
	"ngopi dulu yuk, gua butuh kafein nih udah ngantuk banget dari tadi.",
	"eh lo udah nyobain restoran baru yang di sebelah kantor belum? katanya enak tuh.",
	"gua mau ambil cuti minggu depan, ada yang bisa handle kalau ada apa-apa?",

	// ── Paragraf panjang realistis ──
	"bro gua mau cerita dikit. jadi tadi pagi pas lagi deploy, tiba-tiba database connection-nya timeout semua. panik banget gua. ternyata setelah dicek, ada query yang ga pake index jadi full table scan terus. udah gua fix sih sekarang, tapi sempet down sekitar 10 menit. udah gua masukin ke postmortem report.",
	"eh soal fitur yang kemarin kita diskusi, gua udah mikirin solusinya. kayaknya lebih baik kita pisahin jadi dua service sekalian daripada masukin semua ke monolith. emang butuh effort lebih di awal, tapi jangka panjangnya lebih gampang di-maintain. gimana menurut lo?",
	"update progress: gua udah selesaikan bagian backend-nya, tinggal nunggu desain dari UI/UX buat frontend. estimasi kalau desain udah fix, butuh 3-4 hari lagi buat integrasi dan testing. jadi total masih on track sih sama deadline yang disepakatin.",
	"tadi gua ada call sama client, mereka minta ada fitur export ke Excel. awalnya pikir simpel, tapi ternyata data yang mau di-export bisa sampai ratusan ribu rows. jadi harus pake background job biar ga timeout. gua udah buat estimasinya, kira-kira butuh 2 sprint.",
	"hei, gua mau kasih heads up. kayaknya ada memory leak di service payment. gua notice dari grafana, memory usage-nya naik terus ga pernah turun meski traffic udah sepi. gua udah scale up dulu sementara, tapi perlu dicari root cause-nya. ada yang mau bantu investigate bareng?",
	"btw gua abis baca artikel menarik soal optimasi query PostgreSQL. ternyata ada beberapa teknik yang belum kita pake, kayak partial index sama covering index. kalau kita implement ini buat tabel transaksi yang paling sering di-query, bisa hemat signifikan tuh response time-nya. nanti gua share artikelnya.",
	"reminder buat semua: sprint review besok jam 2 siang ya. tolong semua yang punya task selesai segera update statusnya di Jira. kalau ada yang belum kelar dan mau dipindah ke sprint berikutnya, bilang ke gua dulu sekarang biar bisa dikomunikasiin ke PM.",

	// ── Typo / salah kirim (realistis) ──
	"eh salah kirim, sorry", "ups bukan di sini harusnya", "lol salah chat",
	"maaf typo tadi", "maksudnya*", "eh yang tadi abaikan ya hehe",
}

func randomMessage() string {
	return chatMessages[rand.Intn(len(chatMessages))]
}

// ─── Stats ───────────────────────────────────────────────────────────────────

type Stats struct {
	Connected    atomic.Int64
	Disconnected atomic.Int64
	MsgSent      atomic.Int64
	MsgReceived  atomic.Int64
	Errors       atomic.Int64
	TotalLatency atomic.Int64
	LatencyCount atomic.Int64
}

func (s *Stats) Print(elapsed time.Duration) {
	connected := s.Connected.Load()
	disconnected := s.Disconnected.Load()
	sent := s.MsgSent.Load()
	received := s.MsgReceived.Load()
	errors := s.Errors.Load()
	latCount := s.LatencyCount.Load()

	var avgLatency float64
	if latCount > 0 {
		avgLatency = float64(s.TotalLatency.Load()) / float64(latCount) / 1000
	}

	secs := elapsed.Seconds()

	fmt.Println("\n╔══════════════════════════════════════╗")
	fmt.Println("║         BENCHMARK RESULTS            ║")
	fmt.Println("╠══════════════════════════════════════╣")
	fmt.Printf("║ Duration          : %-16s ║\n", elapsed.Round(time.Millisecond))
	fmt.Printf("║ Connected         : %-16d ║\n", connected)
	fmt.Printf("║ Disconnected      : %-16d ║\n", disconnected)
	fmt.Printf("║ Active            : %-16d ║\n", connected-disconnected)
	fmt.Println("╠══════════════════════════════════════╣")
	fmt.Printf("║ Messages Sent     : %-16d ║\n", sent)
	fmt.Printf("║ Messages Received : %-16d ║\n", received)
	fmt.Printf("║ Msg/sec (sent)    : %-16.2f ║\n", float64(sent)/secs)
	fmt.Printf("║ Msg/sec (recv)    : %-16.2f ║\n", float64(received)/secs)
	fmt.Printf("║ Avg Latency       : %-13.3f ms ║\n", avgLatency)
	fmt.Println("╠══════════════════════════════════════╣")
	fmt.Printf("║ Errors            : %-16d ║\n", errors)
	fmt.Println("╚══════════════════════════════════════╝")
}

// ─── Message ─────────────────────────────────────────────────────────────────

type Message struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Msg    string `json:"msg"`
	SentAt int64  `json:"sent_at"`
}

// ─── Client ──────────────────────────────────────────────────────────────────

func runClient(id int, numClients int, url string, interval time.Duration, stats *Stats, wg *sync.WaitGroup, stop <-chan struct{}) {
	defer wg.Done()

	name := fmt.Sprintf("bench-%d", id)
	fullURL := fmt.Sprintf("%s?name=%s", url, name)

	conn, _, err := websocket.DefaultDialer.Dial(fullURL, nil)
	if err != nil {
		stats.Errors.Add(1)
		return
	}
	defer conn.Close()

	stats.Connected.Add(1)

	// Baca pesan masuk
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg Message
			if err := sonic.Unmarshal(data, &msg); err == nil && msg.SentAt > 0 {
				latency := time.Now().UnixMicro() - msg.SentAt
				stats.TotalLatency.Add(latency)
				stats.LatencyCount.Add(1)
			}
			stats.MsgReceived.Add(1)
		}
	}()

	// Jitter per-client: ±20% dari interval supaya tidak semua kirim barengan
	jitter := time.Duration(float64(interval) * (0.8 + rand.Float64()*0.4))
	ticker := time.NewTicker(jitter)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"),
			)
			stats.Disconnected.Add(1)
			return

		case <-ticker.C:
			targetID := rand.Intn(numClients)
			if targetID == id {
				targetID = (targetID + 1) % numClients
			}

			msg := Message{
				To:     fmt.Sprintf("bench-%d", targetID),
				Msg:    randomMessage(),
				SentAt: time.Now().UnixMicro(),
			}
			data, _ := sonic.Marshal(msg)

			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				stats.Errors.Add(1)
				stats.Disconnected.Add(1)
				return
			}
			stats.MsgSent.Add(1)
		}
	}
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	numClients := flag.Int("clients", 100, "jumlah concurrent client")
	msgInterval := flag.Duration("interval", 500*time.Millisecond, "interval kirim pesan per client")
	duration := flag.Duration("duration", 30*time.Second, "durasi benchmark")
	url := flag.String("url", "ws://localhost:3000/ws", "WebSocket server URL")
	rampUp := flag.Duration("ramp", 3*time.Second, "ramp-up time")
	verbose := flag.Bool("verbose", false, "preview sample pesan")
	flag.Parse()

	fmt.Printf("🚀 Realistic Chat Benchmark\n")
	fmt.Printf("   Clients  : %d\n", *numClients)
	fmt.Printf("   Interval : %s (±20%% jitter per client)\n", *msgInterval)
	fmt.Printf("   Duration : %s\n", *duration)
	fmt.Printf("   Ramp-up  : %s\n\n", *rampUp)

	if *verbose {
		fmt.Println("📨 Sample pesan:")
		for i := 0; i < 8; i++ {
			fmt.Printf("   • %q\n", randomMessage())
		}
		fmt.Println()
	}

	stats := &Stats{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	start := time.Now()

	rampDelay := *rampUp / time.Duration(*numClients)
	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		go runClient(i, *numClients, *url, *msgInterval, stats, &wg, stop)
		time.Sleep(rampDelay)

		if *numClients >= 10 && (i+1)%(*numClients/10) == 0 {
			fmt.Printf("   [ramp-up] %d/%d clients...\n", i+1, *numClients)
		}
	}

	fmt.Println("   ✓ All clients connected!\n")

	liveTicker := time.NewTicker(5 * time.Second)
	go func() {
		for range liveTicker.C {
			fmt.Printf("   [%.0fs] sent: %d | recv: %d | errors: %d\n",
				time.Since(start).Seconds(),
				stats.MsgSent.Load(),
				stats.MsgReceived.Load(),
				stats.Errors.Load(),
			)
		}
	}()

	time.Sleep(*duration)
	close(stop)
	liveTicker.Stop()

	wg.Wait()
	stats.Print(time.Since(start))
}
