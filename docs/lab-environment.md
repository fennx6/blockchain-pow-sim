# Lingkungan eksperimen final

Isi file ini **setelah** eksperimen final dijalankan, dari mesin yang benar-benar
dipakai. Angka speedup bergantung pada CPU host karena semua container berbagi
CPU yang sama (AGENTS.md Bagian 4).

Status: **belum diisi** - belum ada eksperimen final.

## Template

| Item | Nilai | Cara mengisi (Windows PowerShell) |
|---|---|---|
| CPU | TODO | `Get-CimInstance Win32_Processor \| Select-Object Name,NumberOfCores,NumberOfLogicalProcessors` |
| RAM | TODO | `Get-CimInstance Win32_ComputerSystem \| Select-Object TotalPhysicalMemory` |
| OS | TODO | `Get-CimInstance Win32_OperatingSystem \| Select-Object Caption,Version` |
| Python | TODO | `python --version` |
| Docker | TODO | `docker --version` |
| Docker Compose | TODO | `docker compose version` |
| Tanggal eksperimen | TODO | tanggal run final |
| Batas `cpus` per node | 1.0 | dari `docker-compose.yml` |
| Difficulty | TODO | hasil kalibrasi, lihat `docs/decisions.md` D5 |

## Catatan

- Jumlah core fisik menjadi batas atas speedup yang realistis; hasil di atas
  jumlah core harus dilaporkan apa adanya, bukan disembunyikan.
- Semua container memakai jam host yang sama, sehingga selisih `ts_ms` antar
  node sah untuk mengukur delay propagasi.
