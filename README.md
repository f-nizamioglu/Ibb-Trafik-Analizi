# İstanbul Trafik Anomali Analizi

İstanbul Büyükşehir Belediyesi (İBB) Açık Veri Portalı'ndan alınan saatlik trafik yoğunluk verileri üzerinde mekansal anomali tespiti, kümeleme ve görselleştirme sistemi.

Bu proje aynı zamanda bir bilgisayar mühendisliği tez çalışmasının parçasıdır.

---

## Metodoloji

**PostGIS Tabanlı Mekansal DBSCAN ve Zamansal Tekrarlılık Analizi**

Bu sistem, Birant & Kut (2007) ST-DBSCAN algoritmasının tam uygulaması değildir. Temporal epsilon (ε₂) komşuluk mantığı, kümeleme aşamasında uygulanmamaktadır. Bunun yerine:

- PostgreSQL/PostGIS içinde `ST_ClusterDBSCAN` pencere fonksiyonu ile mekansal DBSCAN kümeleme (EPSG:32636 metrik geometri üzerinde, eps metre cinsinden)
- Kümeleme sonrası zamansal tekrarlılık ve süre analizi (AIS bileşenleri üzerinden)
- Çok kriterli Anomali Yoğunluk Skoru (AIS) hesabı: Hacim %30, Hız Düşüşü %30, Süre %25, Tekrarlılık %15

| Kavram | Açıklama | Bu projede mi? |
|--------|----------|---------------|
| DBSCAN | Yoğunluk tabanlı mekansal kümeleme | Kavramsal olarak evet |
| ST-DBSCAN | Mekansal + zamansal epsilon ile kümeleme | Tam uygulanmadı |
| PostGIS `ST_ClusterDBSCAN` | Geometri üzerinde mekansal DBSCAN | Evet |
| Proje boru hattı | Mekansal DBSCAN + zamansal tekrarlılık + AIS | Evet |

---

## Teknoloji

| Katman | Teknolojiler |
|--------|-------------|
| Veritabanı | PostgreSQL 16+, PostGIS 3.4+ (MobilityDB Docker imajı) |
| Veri İşleme | Python 3.10+, Pandas, NumPy |
| Backend | FastAPI, Uvicorn, asyncpg, Pydantic |
| Frontend | Vanilla JavaScript, Leaflet.js |
| Altyapı | Docker, Docker Compose |

**Not:** MobilityDB, Docker imajı aracılığıyla kullanılabilir durumdadır ancak mevcut boru hattında aktif olarak kullanılmamaktadır. OSRM harita eşleştirmesi bu boru hattında aktif değildir.

---

## Kurulum

```bash
# 1. Çevre değişkenleri
cp .env.example .env
# .env dosyasına veritabanı giriş bilgilerini girin

# 2. Bağımlılıklar
pip install -r requirements.txt

# 3. Veritabanı (Docker)
docker-compose up -d
# PostgreSQL/PostGIS, localhost:5433 üzerinde başlar
```

---

## Boru Hattı

Aşağıdaki sırayla çalıştırılır:

```bash
# Aşama 1: İBB portalından CSV indir
python download_data.py

# Aşama 2-3: CSV'yi PostgreSQL/PostGIS'e aktar (EPSG:32636 metrik geometri)
python ingest_data.py

# Aşama 4: Tıkanıklık aday filtresi + mekansal indeksler
python create_views.py

# Aşama 5-7: Mekansal DBSCAN kümeleme + zamansal analiz + AIS skorlama
python run_pipeline.py
```

### Boru Hattı Aşamaları

| Aşama | Betik | Açıklama |
|-------|-------|----------|
| 1 | `download_data.py` | İBB portali scraping, CSV indirme |
| 2 | `ingest_data.py` | CSV → PostgreSQL, EPSG:32636 geometri dönüşümü |
| 3 | `create_views.py` | GiST mekansal indeks |
| 4 | `create_views.py` | `high_congestion_zones` VIEW (statik taban filtresi) |
| 5 | `run_pipeline.py` | PostGIS `ST_ClusterDBSCAN` mekansal kümeleme |
| 6 | `run_pipeline.py` | Zamansal tekrarlılık/süre analizi (AIS bileşeni) |
| 7 | `run_pipeline.py` | AIS skorlama + `cluster_scores.csv` çıktısı |
| 8 | `uvicorn ...` | FastAPI REST API |
| 9 | `index.html` | Leaflet.js görselleştirme |

---

## API Sunucusunu Başlatma

```bash
uvicorn backend.app.main:app --reload --port 8000
```

- Arayüz: `http://localhost:8000`
- API Belgelendirme: `http://localhost:8000/docs`

---

## API Uç Noktaları

| Uç Nokta | Açıklama |
|----------|----------|
| `GET /api/clusters` | Tüm anomali kümelerini GeoJSON olarak döner |
| `GET /api/clusters?severity=HIGH` | Seviyeye göre filtreli kümeler |
| `GET /api/clusters/{id}` | Tek küme detayı |
| `GET /api/stats` | Genel istatistikler |
| `GET /api/heatmap` | Nokta yoğunluğu; `?date=YYYY-MM-DD` ile filtreli |
| `GET /api/health` | Servis canlılık kontrolü |

---

## Deneyler

```bash
# Parametre duyarlılık analizi
python experiments/parameter_sensitivity.py

# Geohash hücre yoğunluk prototipi (alan tabanlı yaklaşım, deneysel)
python experiments/dynamic_density.py

# Grafik üretimi (CSV çıktısı gerektirir)
python experiments/generate_charts.py
```

---

## Testler

```bash
pip install pytest httpx  # geliştirme bağımlılıkları
python -m pytest tests/ -v
```

---

## Dizin Yapısı

```
.
├── backend/app/          FastAPI uygulaması, rotalar, servisler, modeller
├── experiments/          Parametre duyarlılık ve yoğunluk prototip deneyleri
├── legacy/               Arşivlenmiş eski Python tabanlı implementasyon
├── outputs/experiments/  Deney CSV ve MD çıktıları (.gitignore hariç)
├── scoring/              AIS (Anomali Yoğunluk Skoru) motoru
├── tests/                pytest test paketi
├── config.py             Merkezi yapılandırma (.env üzerinden)
├── create_views.py       Mekansal indeks + aday filtre VIEW
├── download_data.py      İBB portal scraping
├── ingest_data.py        CSV → PostGIS aktarımı
├── index.html            Leaflet.js harita arayüzü
└── run_pipeline.py       Ana boru hattı orkestratörü
```

---

## Kısıtlamalar

- Tam ε₂ tabanlı ST-DBSCAN uygulanmamıştır; zamansal boyut kümeleme sonrası AIS aracılığıyla ele alınmaktadır.
- MobilityDB Docker imajı üzerinden kullanılabilir durumdadır; ancak aktif boru hattında MobilityDB fonksiyonları kullanılmamaktadır.
- OSRM harita eşleştirmesi mevcut boru hattında aktif değildir.
- Geohash yoğunluk filtresi deneysel prototip seviyesindedir ve alan tabanlı yaklaşıma dayanmaktadır. Yol uzunluğu tabanlı gerçek yoğunluk için yol ağı verisi (OSM) gerekmektedir.
- Parametre deneyleri (`experiments/`) veritabanının çalışır durumda ve verisinin yüklenmiş olmasını gerektirmektedir.
