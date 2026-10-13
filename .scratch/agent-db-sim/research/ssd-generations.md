# Research: NVMe SSD Gen3/4/5 for StorageBackend presets

Source lane: lib-6 (librarian), 2026-10-13. Manufacturer spec sheets + independent measurements (StorageReview, TechPowerUp, Tom's Hardware, TheSSDReview, PyNVMe3). Intended consumers: `STORAGE_BACKEND_REGISTRY` presets and scenario profile authoring.

## 1. PCIe Gen3 NVMe class (Samsung 970 EVO Plus, WD SN750 class)

| Metric | 970 EVO Plus 1TB (rated) | Measured | Source |
|---|---|---|---|
| Seq. read | 3,500 MB/s | ~3,200–3,400 MB/s | techpowerup SSD DB, PC Perspective |
| Seq. write | 3,300 MB/s rated; **sustained post-SLC ~1.5–2 GB/s** | | TechPowerUp, TweakTown |
| 4K random read QD32 | 600K IOPS | 431K IOPS @ **295 µs** loaded peak; sub-1 ms only to ~129K IOPS | StorageReview VDBench |
| 4K random write QD32 | 550K IOPS | 365K IOPS @ **348 µs** | StorageReview |
| QD1 4K latency | ~50–80 µs typical | 82.1 µs @59K IOPS (983 DCT) | StorageReview 983 DCT, TweakTown |

**Gen3 enterprise anchor (Samsung 983 DCT / PM983, 3-DWPD):** rated 440–540K read IOPS, 45–55K write IOPS, 3.0–3.4 GB/s; measured 592K read @215 µs, 53K write @2.42 ms; spec latency 85/50 µs R/W. Massive steady-state read/write asymmetry (≥10×).

## 2. PCIe Gen4 NVMe (980 PRO / SN850 / FireCuda 530)

| Metric | Rated | Measured | Source |
|---|---|---|---|
| Seq. read | 7,000 MB/s | 5.57 GB/s (64K) | Samsung PR, TechPowerUp, StorageReview |
| Seq. write | 5,000–5,300 MB/s | 1.52 GB/s (64K) | same |
| 4K rnd read QD32 | 1,000K IOPS | 980 PRO 545.7K @232 µs; SN850 790K @160 µs | StorageReview |
| 4K rnd write QD32 | 720K–1,000K | 980 PRO 383K @329 µs; SN850 172.9K @734 µs | StorageReview |
| QD1 latency | ~35–45 µs | 980 PRO inherently low QD1 latency | AnandTech |

Write cliff: SN850 → ~1.0 GB/s sustained after 277 GB SLC cache (TechPowerUp).

## 3. PCIe Gen5 NVMe (2025–2026)

⚠️ 990 EVO is NOT a Gen5 anchor (dual-mode Gen4×4/Gen5×2). Real Gen5×4: Crucial T700/T705/T710, WD SN8100; enterprise: Micron 9550, Kioxia CM7/V, Samsung PM1743.

| Metric | T700 (consumer) | Micron 9550 MAX (ent.) | Kioxia CM7-V (ent.) |
|---|---|---|---|
| Seq. read | 12,400 MB/s | 14,000 MB/s | 14,000 MB/s |
| Seq. write | 11,800 MB/s (SLC window) | 10,000 MB/s | 7,000 MB/s |
| 4K rnd read QD32 | 1,500K IOPS | 3,300K IOPS | 2,700K IOPS |
| 4K rnd write QD32 | ~1,300K IOPS | ~720K IOPS | 550–600K IOPS |
| QD1 latency | ~30 µs class; no improvement over Gen4, bandwidth only | TYP ~80 µs read | 78 µs @715K IOPS loaded |

Key caveat: Gen5 consumer throttles on power (~25 W sustained); latency does not improve Gen4→Gen5.

## 4. Enterprise steady-state anchors

| Gen | Drive | 4K rr IOPS | 4K rw IOPS | TYP latency | Seq R/W | DWPD |
|---|---|---|---|---|---|---|
| Gen3 | Samsung 983 DCT | 592K | 53K | 82 µs | 3.3/2.2 GB/s | 1.3–3 |
| Gen4 | Micron 7450 PRO | 1,000K | 180–250K | 80 µs | 6.8/5.6 GB/s | 1–3 |
| Gen5 | Micron 9550 | 3,300K | 720K | ~80 µs | 14/10 GB/s | 1–3 |
| Gen5 | Kioxia CM7-V 12.8TB | 2,700K | 600K | 78 µs | 14/7 GB/s | 3 |

## 5. Modeling caveats (ranked)

1. **Queue-depth dependence** — biggest gap in `base_latency + ceilings` model: latency at QD1 (~80 µs) vs loaded (295–734 µs) is 3–9×; real drives ≈ `base_latency × (1 + f(QD))` until ceiling. For our in-flight range (1–32) curve shape matters more than peak ceilings.
2. **Ceilings are block-size dependent** — IOPS at 4K, bandwidth at ≥128K; mixed sizes over-approximate effective ceiling ~1.5–2×.
3. **Read/write asymmetry not representable in 4 fields** — enterprise Gen5: 4.6:1 read/write IOPS. Recommendation: read-based preset values + document write asymmetry (or later add `write_per_gb_time`).
4. **SLC cache cliffs** — consumer writes time-varying (fill %); steady-state DB sim should assume post-cache speeds (0.4–1.0 s/GB effective consumer steady writes).
5. **Enterprise vs consumer** — enterprise flat across fill (PM9A3: 685K→604K 10%→90% fill), consumer highly fill-dependent; enterprise preset is the "honest" DB choice.

## YAML-ready presets (proposed)

```yaml
# per_gb_time = 1 / sustained_GB_s (rated 128K seq read)
# iops_ceiling = conservative 4K random read (rated vs measured blend)
# base_latency = typical QD1 4K read service latency
ssd_gen3:        # 970 EVO Plus / SN750 class
  base_latency: 0.00008          # 80 µs
  per_gb_time: 0.294             # 1 / 3.4 GB/s
  iops_ceiling: 550000
  bandwidth_ceiling: 3.4         # GB/s
ssd_gen4:        # 980 PRO / SN850 / FireCuda 530
  base_latency: 0.00004          # 40 µs
  per_gb_time: 0.1471            # 1 / 6.8 GB/s
  iops_ceiling: 800000
  bandwidth_ceiling: 6.8
ssd_gen5:        # T700 / T705 / SN8100 consumer
  base_latency: 0.00003          # 30 µs
  per_gb_time: 0.0806            # 1 / 12.4 GB/s
  iops_ceiling: 1450000
  bandwidth_ceiling: 12.4
ssd_enterprise:  # Gen5 DC: Micron 9550 MAX / Kioxia CM7-V (RECOMMENDED default for DB sim)
  base_latency: 0.00008          # 80 µs (TYP; CM7-R measured 78 µs loaded)
  per_gb_time: 0.0714            # 1 / 14 GB/s
  iops_ceiling: 2400000          # conservative (3.3M / 2.7M available)
  bandwidth_ceiling: 14.0
  # write path 3–8× worse; consider 70/30 mixed effective rates (9550: ~1300K IOPS)
```

Tier scaling: Gen3→Gen4 ≈ 2× all axes; Gen4→Gen5 ≈ 1.8× bandwidth, 1.5× IOPS, no latency gain; consumer→enterprise Gen5 ≈ 2.7× IOPS.

Model refinement suggestions: treat ceilings as `max(concurrent_iops, bytes/block)` combined limit; consider future `write_per_gb_time` field; in-flight-count range makes curve shape the dominant error source.
