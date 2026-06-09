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

### Tam veri seti indirme (61 aylık, 2020-01 ile 2025-01 arası)

```bash
# IBB CKAN API üzerinden 61 aylık kaynağı keşfet
python download_data.py --discover --expected-months 63

# Aylık CSV dosyalarını indir (data/raw/ibb_hourly_traffic_density/)
python download_data.py --download

# Sadece belirli bir tarih aralığını indir
python download_data.py --download --start 2022-01 --end 2024-12

# İndirme durumunu görüntüle
python download_data.py --status --expected-months 63
```

**Not:** Ocak 2025 dosyası (`data/raw/ibb_hourly_traffic_density/traffic_density_202501.csv`) zaten mevcuttur.
Manifest'e otomatik olarak eklenir, yeniden indirilmez.

### Veri aktarımı

```bash
# Tüm mevcut CSV dosyalarını PostgreSQL'e aktar (idempotent)
python ingest_data.py

# Belirli bir dosyayı aktar
python ingest_data.py --file data/raw/ibb_hourly_traffic_density/traffic_density_202001.csv

# Aktarım durumunu görüntüle
python ingest_data.py --status
```

### 63 aylık kapsam doğrulama

```bash
python scripts/validate_data_coverage.py --expected-months 63
```

### Ana pipeline

```bash
# Tıkanıklık aday filtresi + mekansal indeksler
python create_views.py

# Mekansal DBSCAN kümeleme + zamansal analiz + AIS skorlama
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

# Yol ağı yoğunluk karşılaştırması (bkz. aşağıdaki kurulum adımları)
python experiments/road_density.py

# Grafik üretimi (CSV çıktısı gerektirir)
python experiments/generate_charts.py
```

---

## Yol Ağı Tabanlı Yoğunluk Filtresi (Opsiyonel)

Statik filtre (`avg_speed < 20, vehicle_count > 500`) varsayılan olarak kullanılır. Yol uzunluğu tabanlı gerçek yoğunluk filtrelemesi için aşağıdaki adımlar izlenir:

### 1. Gereksinimler

```bash
pip install geopandas>=1.0 pyogrio>=0.7
```

### 2. İstanbul OSM Verisi İndir

```
https://download.bbbike.org/osm/bbbike/Istanbul/Istanbul.osm.pbf
```

Dosyayı şuraya kaydet: `data/road_network/Istanbul.osm.pbf` (~75 MB)

### 3. Yol Ağı Şemasını Oluştur

```bash
python scripts/create_road_schema.py
```

Bu komut şunları oluşturur:
- `road_segments` tablosu
- `geohash_cells` tablosu (ibb_traffic_density geohash'larından)
- `geohash_road_lengths` materialized view (PostGIS ST_Intersection ile)
- `road_density_base` view (vehicles_per_road_km hesabı)
- `road_density_congestion_candidates` view (yüzdelik dilim tabanlı dinamik filtre)

### 4. Yol Segmentlerini Aktar

```bash
python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf
```

Araç yollarını (`motorway`, `trunk`, `primary`, `secondary`, `tertiary`, `residential`, vb.) filtreler ve matview'ı günceller.

### 5. Yöntemi Etkinleştir

`.env` dosyasında:
```
DENSITY_FILTER_METHOD=road_length
ROAD_DENSITY_PERCENTILE_THRESHOLD=75.0
```

Seçenekler: `static` | `geohash_area` | `road_length`

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
├── data/road_network/    OSM PBF dosyası için yer tutucu (.gitignore, yalnızca .gitkeep)
├── experiments/          Parametre duyarlılık ve yoğunluk karşılaştırma deneyleri
├── legacy/               Arşivlenmiş eski Python tabanlı implementasyon
├── outputs/experiments/  Deney CSV ve MD çıktıları (.gitignore hariç)
├── scoring/              AIS motoru + geohash yardımcı araçları
├── scripts/              Yol ağı şema ve aktarım betikleri
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
- Geohash yoğunluk filtresi (`geohash_area`) deneysel prototip seviyesindedir ve alan tabanlı yaklaşıma dayanmaktadır.
- Yol uzunluğu tabanlı yoğunluk (`road_length`) OSM PBF dosyasının manuel olarak indirilmesini gerektirir; bu dosya depoda yer almamaktadır.
- OSM çift yönlü yollar yol uzunluğunu çift sayabilir; bu durum sınırlılık olarak belgelenmiştir.
- Parametre deneyleri (`experiments/`) veritabanının çalışır durumda ve verisinin yüklenmiş olmasını gerektirmektedir.
