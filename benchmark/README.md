# Cek limit saat ini
ulimit -n

# Naikan sementara (per session)
ulimit -n 100000

# Naikan permanent
echo "* soft nofile 100000" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 100000" | sudo tee -a /etc/security/limits.conf
```

---

## Cara Baca Hasil
```
╔══════════════════════════════════════╗
║         BENCHMARK RESULTS            ║
╠══════════════════════════════════════╣
║ Duration          : 30s             ║
║ Connected         : 1000            ║
║ Disconnected      : 1000            ║
║ Active            : 0               ║
╠══════════════════════════════════════╣
║ Messages Sent     : 58420           ║
║ Messages Received : 57891           ║  ← kalau jauh dari sent = ada bottleneck
║ Msg/sec (sent)    : 1947            ║
║ Msg/sec (recv)    : 1929            ║
║ Avg Latency       : 2.341 ms        ║  ← makin kecil makin bagus
╠══════════════════════════════════════╣
║ Errors            : 0               ║  ← idealnya 0
╚══════════════════════════════════════╝