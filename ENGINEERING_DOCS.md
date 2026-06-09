# Mühendislik ve Mimari Dokümantasyonu

Bu belge, İBB Trafik Analizi projesinin teknik mimarisini, algoritmasını ve dosya yapısını açıklar. Yapay zeka ajanları ve geliştiriciler için referans niteliğindedir.

---

## Metodoloji: PostGIS Tabanlı Mekansal DBSCAN ve Zamansal Tekrarlılık Analizi

Bu sistem, Birant & Kut (2007) ST-DBSCAN algoritmasının tam uygulaması değildir. Temporal epsilon (ε₂) komşuluk kısıtı, kümeleme aşamasında uygulanmamaktadır.

**Aktif yaklaşım:**
1. Statik taban filtresi: `avg_speed < 20 km/h AND vehicle_count > 500`
2. PostGIS `ST_ClusterDBSCAN` ile mekansal DBSCAN (EPSG:32636, eps metre cinsinden)
3. Kümeleme sonrası zamansal analiz: duration_hours ve recurrence_days (AIS bileşenleri)
4. AIS: çok kriterli ağırlıklı kompozit skor

**Aktif boru hattında olmayan:**
- Python BallTree / haversine tabanlı kümeleme (bkz. `legacy/`)
- OSRM harita eşleştirmesi
- MobilityDB zamansal sorgular (Docker imajı mevcut ama kullanılmıyor)
- `clustering/` veya `map_matching/` dizinleri

---

## Veri Akışı

```
İBB CSV
  → ibb_traffic_density       (EPSG:32636 metrik geometri, GEOMETRY(Point,32636))
  → high_congestion_zones     (statik taban filtresi VIEW)
  → PostGIS ST_ClusterDBSCAN  (mekansal DBSCAN, eps_meters cinsinden)
  → traffic_clusters          (cluster_id ile birlikte)
  → AIS skorlama              (duration, recurrence, volume, speed_drop)
  → FastAPI GeoJSON API
  → Leaflet.js harita
```

---

## Koordinat Sistemi

| Aşama | CRS | Açıklama |
|-------|-----|----------|
| İBB CSV girişi | WGS84 EPSG:4326 | Enlem/boylam derece cinsinden |
| `ibb_traffic_density.geom` | EPSG:32636 (UTM Zone 36N) | `ST_Transform` ile aktarımda dönüştürülür; metre cinsinden |
| `ST_ClusterDBSCAN eps` | Metre (EPSG:32636 metrik) | eps doğrudan metre olarak verilir |
| API çıktısı / Leaflet | WGS84 EPSG:4326 | `ST_Transform(centroid, 4326)` ile geri dönüştürülür |

---

## Dosya Yapısı

### Kök Dizin

| Dosya | Açıklama |
|-------|----------|
| `config.py` | Merkezi yapılandırma; tüm ayarları `.env` üzerinden `backend/app/config.py` aracılığıyla yükler; `RAW_DATA_DIR`, `MANIFEST_PATH`, `IBB_CKAN_BASE_URL` içerir |
| `create_views.py` | GiST mekansal indeks, `high_congestion_zones` VIEW, `traffic_clusters` tablosu |
| `download_data.py` | IBB CKAN API üzerinden aylık CSV kaynaklarını keşfeder ve indirir; manifest tabanlı ilerleme takibi; tekrar çalıştırılabilir |
| `ingest_data.py` | Çok dosyalı CSV → PostgreSQL toplu aktarım; `ingested_files` tablosu ile tekrarlama koruması; EPSG:4326 → EPSG:32636 dönüşümü |
| `run_pipeline.py` | Ana orkestratör: ST_ClusterDBSCAN + AIS scoring |
| `check_geohash.py` | Geohash hücresi için yüksek yoğunluk bölge kontrol aracı |
| `index.html` | Leaflet.js harita arayüzü (tek dosya) |

### `backend/app/`

| Dosya | Açıklama |
|-------|----------|
| `main.py` | FastAPI uygulaması, lifespan yönetimi, CORS, router kaydı |
| `config.py` | Pydantic BaseSettings; tüm parametreler `.env` üzerinden |
| `database.py` | asyncpg bağlantı havuzu (min=2, max=10) |
| `limiter.py` | SlowAPI hız limiti (60 istek/dakika) |
| `routers/clusters.py` | `GET /api/clusters`, `/api/clusters/{id}`, `/api/stats` |
| `routers/heatmap.py` | `GET /api/heatmap` |
| `routers/health.py` | `GET /api/health` |
| `services/cluster_service.py` | İş mantığı: ST_ClusterDBSCAN SQL, AIS hesabı, GeoJSON üretimi |
| `models/cluster.py` | Pydantic şemaları: GeoJSON, HeatmapPoint, StatsResponse |

### `scoring/`

| Dosya | Açıklama |
|-------|----------|
| `anomaly_score.py` | AIS motoru: min-max normalizasyon, ağırlıklı kompozit, seviye sınıflandırması |
| `geohash_utils.py` | Geohash kodlama/çözme, hücre alanı, yoğunluk adayları, ST_MakeEnvelope argümanları |

### `scripts/`

| Dosya | Açıklama |
|-------|----------|
| `create_road_schema.py` | Yol ağı tabloları, geohash_cells, materialized view, density view'larını oluşturur |
| `import_road_network.py` | OSM PBF'yi geopandas/pyogrio ile okur, road_segments'e toplu aktarım yapar, matview'ı günceller |
| `validate_data_coverage.py` | Manifest ve veritabanını karşılaştırarak 63 aylık kapsam doğrulama raporu üretir |

### `data/`

| Yol | Açıklama |
|-----|----------|
| `data/download_manifest.json` | İndirme durumu kaydı; kaynak URL, period, sha256, indirilme/aktarılma bayrakları |
| `data/raw/ibb_hourly_traffic_density/` | İndirilen aylık CSV dosyaları; `.gitignore` kapsamında (yalnızca `.gitkeep` Git'te) |
| `data/road_network/` | OSM PBF yer tutucu; yalnızca `.gitkeep` Git'te; bkz. aşağıda |

### `data/road_network/`

OSM PBF dosyası için yer tutucu dizini. Yalnızca `.gitkeep` saklanır; PBF dosyası `.gitignore` kapsamındadır.

İndirme adresi: `https://download.bbbike.org/osm/bbbike/Istanbul/Istanbul.osm.pbf` (~75 MB)

### `experiments/`

| Dosya | Açıklama |
|-------|----------|
| `parameter_sensitivity.py` | Hız/araç eşiği + eps/minpoints kombinasyonlarını test eder |
| `dynamic_density.py` | Geohash hücre alanı tabanlı yoğunluk prototipi (deneysel) |
| `road_density.py` | Statik / geohash-alan / yol-uzunluğu yöntemlerini karşılaştırır |
| `generate_charts.py` | Deney CSV çıktılarından grafik üretir |
| `_dataset_check.py` | Paylaşılan yardımcı modül; deney öncesi veritabanı veri kapsamını raporlar |

### `tests/`

| Dosya | Açıklama |
|-------|----------|
| `conftest.py` | pytest fikstürleri (DB bağlantı bayrağı) |
| `test_pipeline.py` | Yapılandırma, AIS sınırları, koordinat dönüşümü |
| `test_api.py` | GeoJSON yanıt yapısı, seviye filtresi |
| `test_density.py` | Geohash hücre alanı hesabı, yoğunluk skoru mantığı |
| `test_road_density.py` | Geohash envelope argümanları, vehicles_per_road_km mantığı, yüzdelik dilim seçimi; DB testleri yol verisi yoksa atlanır |

### `legacy/`

| Dosya | Açıklama |
|-------|----------|
| `st_dbscan_analysis.py` | Arşivlenmiş: eski Python BallTree tabanlı ST-DBSCAN implementasyonu. Aktif boru hattının parçası değildir. |

---

## Temel Mimari Kararlar

### PostGIS Yerel Kümeleme

Kümeleme SQL pencere fonksiyonu olarak çalışır:

```sql
ST_ClusterDBSCAN(geom, eps := 500, minpoints := 3) OVER ()
```

Tüm hesaplama veritabanı içinde gerçekleşir. Python tarafında mesafe matrisi veya BallTree hesaplaması yoktur. Bu yaklaşım:
- O(n²) Python bellek sorununu ortadan kaldırır
- GiST mekansal indeksten yararlanır
- `run_pipeline.py` ve `cluster_service.py`'de aynı SQL çalışır

### Çift CRS Tasarımı

Depolama EPSG:32636'dadır (metre cinsinden eps için), API çıktısı EPSG:4326'ya dönüştürülür (Leaflet için).

### AIS İki Yerde Uygulanır

`scoring/anomaly_score.py` (DataFrame tabanlı, `run_pipeline.py` tarafından çağrılır) ve `backend/app/services/cluster_service.py` (dict tabanlı, API tarafından çağrılır). İki versiyon senkronize tutulmalıdır.

### Statik Filtre Taban Çizgisi

`high_congestion_zones` VIEW, sabit `avg_speed < 20 km/h AND vehicle_count > 500` koşullarını uygular. Bu, bilimsel açıdan optimize edilmiş bir filtre değil, mühendislik taban çizgisidir. Parametre duyarlılık deneyleri bu değerleri sistematik olarak test eder.

---

## İBB Veri İndirme ve Aktarım Sistemi

### Genel Bakış

Ocak 2020'den Ocak 2025'e kadar 61 aylık İBB saatlik trafik yoğunluğu verisi IBB CKAN API üzerinden erişilebilir durumdadır. Bu sistem, tüm aylık CSV dosyalarını idempotent biçimde keşfeder, indirir ve veritabanına aktarır.

**Önemli:** Profesöre bildirilen 63 aylık hedef ile gerçekte yayınlanmış 61 ay arasında 2 aylık fark bulunmaktadır. Bu fark gizlenmez; `validate_data_coverage.py` çıktısında açıkça raporlanır. Eksik aylar icat edilmez.

### CKAN API Kaynağı

| Parametre | Değer |
|-----------|-------|
| Portal | `https://data.ibb.gov.tr` |
| Dataset slug | `hourly-traffic-density-data-set` |
| API uç noktası | `/api/3/action/package_show?id=hourly-traffic-density-data-set` |
| Kaynak sayısı | 61 (Ocak 2020 – Ocak 2025) |
| Dosya adı kalıbı | `traffic_density_YYYYMM.csv` |

### Manifest Sistemi

`data/download_manifest.json` dosyası, her aylık CSV için kalıcı ilerleme kaydı tutar:

```json
{
  "resources": [
    {
      "period": "2020-01",
      "title": "...",
      "source_url": "https://...",
      "local_path": "data/raw/ibb_hourly_traffic_density/traffic_density_202001.csv",
      "sha256": "abc123...",
      "size_bytes": 12345678,
      "downloaded": true,
      "ingested": true,
      "row_count": 450000,
      "min_date": "2020-01-01T00:00:00",
      "max_date": "2020-01-31T23:00:00",
      "status": "ok"
    }
  ]
}
```

Bu dosya Git'e dahildir ve tekrar çalıştırmalarda neyin atlanacağını belirler.

### Ocak 2025 Özel Durumu (Bootstrap)

Ocak 2025 CSV dosyası (`data/raw/ibb_hourly_traffic_density/traffic_density_202501.csv`) proje başından beri yerel olarak mevcuttur. `download_data.py` bu dosyayı yeniden indirmez; `ingest_data.py` bu dosyayı yeniden aktarmaz. Bunun yerine:

1. `download_data.py --discover`: CKAN kaynakları arasında 2025-01 girişini bulur; SHA256 doğrular ve manifest'e kayıt eder; `downloaded: true` işaretler; dosyayı `data/raw/` altına **kopyalamaz**.
2. `ingest_data.py`: Veritabanında 2025-01 satırları varsa `ingested_files` tablosuna kaydeder ve manifesti günceller; yeniden aktarmaz.

### İndirme Komutu Referansı

```bash
# Kaynakları keşfet (manifest oluştur)
python download_data.py --discover --expected-months 63

# Tüm dosyaları indir
python download_data.py --download

# Belirli aralık
python download_data.py --download --start 2022-01 --end 2024-12

# Durum raporu
python download_data.py --status --expected-months 63

# Kuru çalıştırma (dosya yazmadan)
python download_data.py --download --dry-run

# Zorunlu yeniden indirme
python download_data.py --download --force
```

### Aktarım Komutu Referansı

```bash
# Tüm mevcut CSV'leri aktar (idempotent)
python ingest_data.py

# Belirli bir dosyayı aktar
python ingest_data.py --file data/raw/ibb_hourly_traffic_density/traffic_density_202001.csv

# Aktarım durumunu görüntüle
python ingest_data.py --status

# Takip tablosunu sıfırla (yeniden aktarım için)
python ingest_data.py --reset
```

### Kapsam Doğrulama

```bash
python scripts/validate_data_coverage.py --expected-months 63
python scripts/validate_data_coverage.py --start 2020-01 --end 2025-01
```

Çıktı: indirilen ay sayısı, aktarılan ay sayısı, eksik aylar, veritabanı satır sayısı, tarih aralığı.

### Tam Veri Sonrası Pipeline

Tüm veriler aktarıldıktan sonra pipeline ve deneyler yeniden çalıştırılmalıdır:

```bash
python create_views.py
python run_pipeline.py
python experiments/parameter_sensitivity.py
python experiments/dynamic_density.py
```

### Yoğunluk Filtresi Mimarisi

`DENSITY_FILTER_METHOD` yapılandırma değişkeni üç yöntemi kontrol eder:

| Yöntem | Açıklama | Gereksinim |
|--------|----------|------------|
| `static` | Sabit hız + araç eşiği | Yok |
| `geohash_area` | Geohash hücre alanına göre vehicles/km² | Yok (yaklaşım) |
| `road_length` | OSM yol ağına göre vehicles/road-km | OSM PBF + şema kurulumu |

`road_length` yöntemi şu nesnelere dayanır:

```sql
-- Fiziksel yol segmentleri (OSM'den aktarılır)
road_segments (osm_id, highway, geom_4326, geom_32636, length_m)

-- Trafik geohash'larından oluşturulan hücre poligonları
geohash_cells (geohash, geom_4326, geom_32636, area_km2)

-- PostGIS ST_Intersection ile geohash başına toplam yol uzunluğu
MATERIALIZED VIEW geohash_road_lengths (geohash, road_length_km)

-- Mevcut trafik verisini yol yoğunluğuyla birleştiren VIEW
VIEW road_density_base (geohash, ..., road_length_km, vehicles_per_road_km)

-- Yüzdelik dilim tabanlı dinamik tıkanıklık adayları
VIEW road_density_congestion_candidates
```

`geohash_road_lengths` matview, `import_road_network.py` çalıştıktan sonra `REFRESH MATERIALIZED VIEW CONCURRENTLY` ile güncellenir.

---

## Veritabanı Şeması

```sql
-- Ham veri (EPSG:32636 metrik geometri)
CREATE TABLE ibb_traffic_density (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP,
    lat DOUBLE PRECISION,          -- WGS84 derece (sunum için)
    lon DOUBLE PRECISION,
    geohash VARCHAR(20),
    min_speed INTEGER,
    max_speed INTEGER,
    avg_speed INTEGER,             -- km/h
    vehicle_count INTEGER,
    geom GEOMETRY(Point, 32636)    -- EPSG:32636 metrik, kümeleme için
);

-- Aday filtre (statik taban çizgisi)
CREATE VIEW high_congestion_zones AS
SELECT * FROM ibb_traffic_density
WHERE avg_speed < 20 AND vehicle_count > 500;

-- Kümeleme çıktısı
CREATE TABLE traffic_clusters (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP WITHOUT TIME ZONE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geohash VARCHAR(20),
    vehicle_count INTEGER,
    avg_speed INTEGER,
    cluster_id INTEGER             -- -1 = gürültü (noise)
);
```

### İndirme Takip Tablosu

```sql
CREATE TABLE ingested_files (
    id          SERIAL PRIMARY KEY,
    filename    TEXT NOT NULL,
    local_path  TEXT,
    period      VARCHAR(7),         -- YYYY-MM
    sha256      CHAR(64),
    row_count   INTEGER,
    min_date    TIMESTAMP,
    max_date    TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW(),
    status      TEXT DEFAULT 'ok',
    error_message TEXT
);
-- SHA256 ve period üzerinde UNIQUE indeksler: tekrarlı aktarımı engeller
```

### Yol Ağı Şeması (Opsiyonel)

```sql
-- OSM'den aktarılan yol segmentleri
CREATE TABLE road_segments (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    highway TEXT NOT NULL,
    name TEXT,
    geom_4326 GEOMETRY(Geometry, 4326),
    geom_32636 GEOMETRY(Geometry, 32636),
    length_m DOUBLE PRECISION
);

-- İBB trafik geohash'larından oluşturulan hücre poligonları
CREATE TABLE geohash_cells (
    geohash VARCHAR(12) PRIMARY KEY,
    geom_4326 GEOMETRY(Polygon, 4326),
    geom_32636 GEOMETRY(Polygon, 32636),
    area_km2 DOUBLE PRECISION
);

-- Geohash başına toplam yol uzunluğu (PostGIS ST_Intersection)
CREATE MATERIALIZED VIEW geohash_road_lengths AS
SELECT
    gc.geohash,
    SUM(ST_Length(ST_Intersection(rs.geom_32636, gc.geom_32636))) / 1000.0 AS road_length_km
FROM geohash_cells gc
JOIN road_segments rs ON ST_Intersects(rs.geom_32636, gc.geom_32636)
GROUP BY gc.geohash;
```

---

## Yapılandırma Referansı

Tüm parametreler `.env` aracılığıyla ayarlanabilir:

| Değişken | Varsayılan | Açıklama |
|----------|-----------|----------|
| `DBSCAN_EPS_METERS` | 500.0 | Mekansal epsilon (metre, EPSG:32636) |
| `DBSCAN_MINPOINTS` | 3 | Çekirdek nokta için minimum komşu |
| `HIGH_CONGESTION_MAX_AVG_SPEED` | 20 | Aday filtresi hız eşiği (km/h) |
| `HIGH_CONGESTION_MIN_VEHICLE_COUNT` | 500 | Aday filtresi araç sayısı eşiği |
| `DENSITY_FILTER_ENABLED` | false | Geohash yoğunluk prototipini etkinleştir |
| `DENSITY_PERCENTILE_THRESHOLD` | 75.0 | Yoğunluk filtresi yüzdelik dilim eşiği |
| `DENSITY_FILTER_METHOD` | static | Yoğunluk filtre yöntemi: `static` \| `geohash_area` \| `road_length` |
| `ROAD_DENSITY_PERCENTILE_THRESHOLD` | 75.0 | `road_length` yöntemi için vehicles/road-km yüzdelik dilim eşiği |
| `AIS_WEIGHT_VOLUME` | 0.30 | Hacim bileşeni ağırlığı |
| `AIS_WEIGHT_SPEED_DROP` | 0.30 | Hız düşüşü bileşeni ağırlığı |
| `AIS_WEIGHT_DURATION` | 0.25 | Süre bileşeni ağırlığı |
| `AIS_WEIGHT_RECURRENCE` | 0.15 | Tekrarlılık bileşeni ağırlığı |
