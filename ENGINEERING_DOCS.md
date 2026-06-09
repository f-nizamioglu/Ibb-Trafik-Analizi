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
| `config.py` | Merkezi yapılandırma; tüm ayarları `.env` üzerinden `backend/app/config.py` aracılığıyla yükler |
| `create_views.py` | GiST mekansal indeks, `high_congestion_zones` VIEW, `traffic_clusters` tablosu |
| `download_data.py` | İBB portalını scrape ederek aylık CSV'leri indirir |
| `ingest_data.py` | CSV → PostgreSQL toplu aktarım; EPSG:4326 → EPSG:32636 dönüşümü |
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

### `experiments/`

| Dosya | Açıklama |
|-------|----------|
| `parameter_sensitivity.py` | Hız/araç eşiği + eps/minpoints kombinasyonlarını test eder |
| `dynamic_density.py` | Geohash hücre alanı tabanlı yoğunluk prototipi (deneysel) |
| `generate_charts.py` | Deney CSV çıktılarından grafik üretir |

### `tests/`

| Dosya | Açıklama |
|-------|----------|
| `conftest.py` | pytest fikstürleri (DB bağlantı bayrağı) |
| `test_pipeline.py` | Yapılandırma, AIS sınırları, koordinat dönüşümü |
| `test_api.py` | GeoJSON yanıt yapısı, seviye filtresi |
| `test_density.py` | Geohash hücre alanı hesabı, yoğunluk skoru mantığı |

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
| `AIS_WEIGHT_VOLUME` | 0.30 | Hacim bileşeni ağırlığı |
| `AIS_WEIGHT_SPEED_DROP` | 0.30 | Hız düşüşü bileşeni ağırlığı |
| `AIS_WEIGHT_DURATION` | 0.25 | Süre bileşeni ağırlığı |
| `AIS_WEIGHT_RECURRENCE` | 0.15 | Tekrarlılık bileşeni ağırlığı |
