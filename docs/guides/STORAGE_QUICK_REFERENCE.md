# Weather Data Storage - Quick Reference Guide

## 📊 Storage Requirements at a Glance

### Sorted by Size (Smallest to Largest)

| Rank | Scenario | Storage | Best For |
|------|----------|---------|----------|
| 1 | **US Subset** (1K stations) | **1.75 GB** | Testing, small research projects |
| 2 | **US Comprehensive** (10K stations) | **17.5 GB** | Complete US weather history |
| 3 | **Major Cities** (1K cities, daily+hourly) | **60 GB** | Urban climate analysis, ML training |
| 4 | **Recent Global** (30 years, 0.5° grid) | **554 GB** | Modern climate analysis |
| 5 | **North America Grid** (85 years, 0.5°) | **654 GB** | Regional studies |
| 6 | **Global Coarse** (85 years, 1° grid) | **3.83 TB** | Global climate research |
| 7 | **Global Fine** (85 years, 0.25° grid) | **61.3 TB** | High-res global analysis |

---

## 💰 Cost Estimates

### Local Storage

| Storage Needed | Hardware | Approx Cost |
|----------------|----------|-------------|
| **< 100 GB** | 1 TB External SSD | $80-150 |
| **100 GB - 1 TB** | 2-4 TB External HDD | $100-200 |
| **1-10 TB** | 8 TB External HDD or NAS | $200-500 |
| **10-50 TB** | RAID Array or NAS | $1,000-3,000 |
| **50+ TB** | Multi-drive RAID | $5,000+ |

### Cloud Storage (per month)

| Storage Used | AWS S3 Standard | AWS S3 Glacier | Cost Notes |
|--------------|-----------------|----------------|------------|
| **100 GB** | $2.30 | $0.40 | Excellent for backups |
| **500 GB** | $11.50 | $2.00 | Good value |
| **1 TB** | $23 | $4 | Breaking point for local |
| **5 TB** | $115 | $20 | Consider local storage |
| **50 TB** | $1,150 | $200 | Institutional pricing needed |

*Prices approximate as of 2025*

---

## ⏱️ Download Time Estimates

Assuming 100 Mbps connection (~12 MB/s sustained):

| Dataset Size | Download Time |
|--------------|---------------|
| **1.75 GB** (US Subset) | ~2 minutes |
| **17.5 GB** (US Comprehensive) | ~25 minutes |
| **60 GB** (Major Cities) | ~1.5 hours |
| **654 GB** (North America) | ~15 hours |
| **3.83 TB** (Global Coarse) | ~4 days |
| **61.3 TB** (Global Fine) | ~60 days |

**Note:** API rate limits will extend these times significantly. Budget 2-3x the pure download time.

---

## 🎯 Recommended Setups by Use Case

### 🎓 Academic Research / Thesis
**Storage:** 100-500 GB
```
Recommended: Major Cities (60 GB) or North America recent (300 GB)
Hardware: 1 TB External SSD
Cost: ~$100-150
```

### 🏢 Small Company / Startup
**Storage:** 500 GB - 2 TB
```
Recommended: North America Grid (654 GB) or US + Cities
Hardware: 2-4 TB External HDD + Cloud backup
Cost: ~$200 + $20/month cloud
```

### 🏭 Medium Enterprise
**Storage:** 2-10 TB
```
Recommended: Global Coarse (3.83 TB) + Regional Fine
Hardware: NAS with RAID 5/6
Cost: ~$1,000-2,000 initial + ~$50/month cloud
```

### 🏛️ Research Institution
**Storage:** 10-100+ TB
```
Recommended: Multiple Global Fine datasets
Hardware: Enterprise storage array + Cloud
Cost: Institutional pricing
```

---

## 📈 Data Density Analysis

**Most Efficient:** Daily-only datasets
- US Subset: Only **1.75 GB** for 27M records
- US Comprehensive: Only **17.5 GB** for 274M records

**Most Data-Rich:** Hourly global datasets
- Contains 24x more temporal detail
- Enables sub-daily pattern analysis
- 95% of storage goes to hourly data

---

## 🔧 Technical Specifications

### Parquet File Characteristics

```
Average File Sizes (per year, per location):
- Daily only:   ~2-5 KB
- Hourly only:  ~50-120 KB
- Both:         ~55-125 KB

Compression Ratios:
- vs CSV:       ~10:1
- vs JSON:      ~15:1
- vs Raw:       ~20:1
```

### Memory Requirements

| Dataset Size | Recommended RAM | Notes |
|--------------|-----------------|-------|
| < 10 GB | 4 GB | Minimal |
| 10-100 GB | 8 GB | Comfortable |
| 100 GB - 1 TB | 16 GB | Recommended |
| 1-10 TB | 32 GB+ | For processing |
| 10+ TB | 64 GB+ | Heavy workloads |

---

## 🚀 Quick Decision Tree

```
START: How much data do you need?

├─ Just testing / learning?
│  └─ US Subset (1.75 GB) ✓

├─ Specific cities or regions?
│  └─ Major Cities (60 GB) ✓

├─ US-only analysis?
│  └─ US Comprehensive (17.5 GB) ✓

├─ Regional climate study?
│  └─ North America Grid (654 GB) ✓

├─ Global climate research?
│  ├─ Recent data only? → Recent Global (554 GB) ✓
│  ├─ General resolution? → Global Coarse (3.83 TB) ✓
│  └─ High resolution? → Global Fine (61.3 TB) ✓
```

---

## 💡 Pro Tips

### For Small Budgets
1. Start with **Major Cities** (60 GB)
2. Use **1 TB external HDD** ($100)
3. Download recent years only (2000+)
4. Expand later if needed

### For Research Projects
1. Get **North America Grid** (654 GB)
2. Use **2 TB external HDD** ($120)
3. Full historical record (1940+)
4. Both daily and hourly data

### For Enterprises
1. Start with **Global Coarse** (3.83 TB)
2. Use **NAS with RAID** ($1,500)
3. Add cloud backup (S3 Glacier)
4. Scale to fine resolution if needed

---

## 📝 Notes

- All estimates include 15% overhead for metadata
- Actual sizes may vary ±20% based on:
  - Missing data (reduces size)
  - Additional parameters (increases size)
  - Compression efficiency (varies by data patterns)

- Budget **20% extra** for:
  - Backups
  - Intermediate files
  - Working space

---

## 🔗 Related Files

- `STORAGE_ESTIMATES.txt` - Detailed calculations
- `storage_calculator.py` - Run custom scenarios
- `CODE_REVIEW_SUMMARY.md` - Full code review
- `storage_estimates.json` - Machine-readable data

---

**Last Updated:** 2025-11-16
