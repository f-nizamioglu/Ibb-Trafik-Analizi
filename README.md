# Zaman-Uzamsal Verilerle Trafik Yoğunluğu Analizi ve Görselleştirme Web Portalı

Bu depo, YTÜ Bilgisayar Mühendisliği bitirme projesi için hazırlanmıştır. Proje, İstanbul Büyükşehir Belediyesi saatlik trafik yoğunluğu verisini kullanır. FastAPI, PostgreSQL/PostGIS ve Leaflet.js ile tarih/saat seçimine göre trafik yoğunluğu kümelerini harita üzerinde gösterir. Kullanıcı bir tarih ve saat seçer; sistem ilgili bir saatlik aralıktaki düşük hızlı ölçümleri filtreleyip PostGIS ile mekansal kümeleme yapar. Uygulama geçmiş veriyi görselleştirir; trafik tahmini, rota optimizasyonu veya gerçek zamanlı trafik yönetimi yapmaz.

## Projenin Yaptıkları ve Yapmadıkları

| Yapar | Yapmaz |
| --- | --- |
| Tarih/saat bazlı geçmiş trafik yoğunluğu sorgusu yapar. | Tam ST-DBSCAN uygulamaz. |
| Bir saatlik zaman filtresi uygular. | Aktif sorgu yolunda MobilityDB temporal clustering kullanmaz. |
| Düşük hızlı ölçümleri ön filtrelemeden geçirir. | Gerçek zamanlı trafik yönetimi yapmaz. |
| PostGIS `ST_ClusterDBSCAN` ile mekansal kümeleme yapar. | Rota optimizasyonu yapmaz. |
| Küme merkezlerini Leaflet haritasında gösterir. | Gelecek trafik tahmini üretmez. |
| Yoğunluk skorunu API yanıtında döndürür. | Ground-truth ile doğrulanmış anomali tespiti iddia etmez. |

## Teknoloji Yığını

- Python 3.10+
- FastAPI
- PostgreSQL/PostGIS
- Docker / Docker Compose
- Vanilla JavaScript
- Leaflet.js
- Pandas / NumPy
- pytest

## Depo Yapısı

```text
.
|-- README.md
|-- .gitignore
|-- .env.example
|-- docker-compose.yml
|-- requirements.txt
|-- requirements-dev.txt
|-- index.html
|-- config.py
|-- create_views.py
|-- download_data.py
|-- ingest_data.py
|-- run_pipeline.py
|-- backend/
|   |-- __init__.py
|   `-- app/
|-- data/
|   |-- raw/ibb_hourly_traffic_density/.gitkeep
|   `-- road_network/.gitkeep
|-- scoring/
|-- scripts/
|   |-- __init__.py
|   |-- create_road_schema.py
|   |-- import_road_network.py
|   `-- validate_data_coverage.py
`-- tests/
```

## Veri

Tam veri seti 2020-01 ile 2025-01 arasını kapsar. Hızlı kurulum için Ocak 2025 veri dosyası ayrıca paylaşılacaktır: `traffic_density_202501.csv`.

Dosya, repo klonlandıktan sonra şu konuma yerleştirilmelidir:

```text
data/raw/ibb_hourly_traffic_density/traffic_density_202501.csv
```

Bu dosya ana arayüzün tarih/saat filtresi, PostGIS kümeleme akışı ve demo sorgusunu çalıştırmak için yeterlidir.

## Ön Koşullar

- Git
- Python 3.10+
- Docker Desktop
- Docker Compose
- İsteğe bağlı: `psql` veya pgAdmin

## Hızlı Kurulum - Ocak 2025 Verisi

```powershell
git clone https://github.com/f-nizamioglu/Ibb-Trafik-Analizi.git
cd Ibb-Trafik-Analizi

python -m venv .venv
.\.venv\Scripts\activate
python -m pip install -r requirements.txt

copy .env.example .env
docker-compose up -d
```

`traffic_density_202501.csv` dosyasını şu klasöre kopyalayın:

```text
data/raw/ibb_hourly_traffic_density/
```

Veriyi içe aktarın ve yardımcı veritabanı nesnelerini oluşturun:

```powershell
python ingest_data.py
python create_views.py
```

`ingest_data.py` PostGIS eklentisini, `ibb_traffic_density` ana tablosunu, temel indeksleri ve `ingested_files` takip tablosunu oluşturur. `create_views.py`, `high_congestion_zones` görünümünü ve eski toplulaştırılmış uç noktalar için boş `traffic_clusters` tablosunu hazırlar.

Ana web arayüzü ve demo endpoint için `run_pipeline.py` gerekli değildir. Legacy küme/statistik uç noktalarını doldurmak için ayrıca çalıştırılabilir:

```powershell
python run_pipeline.py
```

Uygulamayı başlatın:

```powershell
uvicorn backend.app.main:app --reload --port 8000
```

Tarayıcı:

```text
http://localhost:8000
```

Demo endpoint:

```text
http://localhost:8000/api/clusters?date=2025-01-17&hour=18
```

macOS/Linux için kısa karşılık:

```bash
python -m venv .venv
source .venv/bin/activate
cp .env.example .env
```

## Tam Veri Kurulumu

```powershell
python download_data.py --discover --expected-months 61
python download_data.py --download
python ingest_data.py
python create_views.py
```

Bu yol tüm veri setini kurar ve uzun sürebilir. Legacy küme/statistik çıktıları için tam veri aktarımından sonra `python run_pipeline.py` çalıştırılabilir.

## Ortam Değişkenleri

`.env.example` yerel Docker/PostGIS kurulumu için beklenen değerleri içerir.

| Değişken | Değer | Açıklama |
| --- | --- | --- |
| `DB_HOST` | `localhost` | Backend'in bağlanacağı host. |
| `DB_PORT` | `5433` | Docker Compose dış portu. |
| `DB_NAME` | `istanbul_traffic` | PostgreSQL veritabanı. |
| `DB_USER` | `postgres` | PostgreSQL kullanıcısı. |
| `DB_PASSWORD` | `postgres` | Yerel PostgreSQL parolası. |
| `APP_ENV` | `development` | Geliştirme modu. |
| `ALLOWED_ORIGINS` | `http://localhost:3000,http://localhost:8000` | CORS izinleri. |

Uygulama ayrı bir `DATABASE_URL` ortam değişkeni okumaz; bağlantı dizesi bu alanlardan `backend/app/config.py` içinde oluşturulur.

## Web Uygulamasını Çalıştırma

```powershell
uvicorn backend.app.main:app --reload --port 8000
```

- Web arayüzü: `http://localhost:8000`
- Swagger/OpenAPI: `http://localhost:8000/docs`

## API Uç Noktaları

| Uç nokta | Açıklama |
| --- | --- |
| `GET /` | Web arayüzünü döndürür. |
| `GET /api/health` | Servis ve veritabanı bağlantı durumunu döndürür. |
| `GET /api/clusters?date=YYYY-MM-DD&hour=HH` | Ana tarih/saat bazlı küme sorgusu. |
| `GET /api/clusters?date=YYYY-MM-DD&hour=HH&severity=HIGH` | Ana sorguyu `LOW`, `MEDIUM` veya `HIGH` yoğunluk düzeyiyle filtreler. |
| `GET /api/heatmap` | Isı haritası noktalarını döndürür. |
| `GET /api/heatmap?date=YYYY-MM-DD` | Isı haritası noktalarını tarihe göre filtreler. |
| `GET /api/clusters` | Legacy toplulaştırılmış küme çıktısı; `run_pipeline.py` sonrası anlamlıdır. |
| `GET /api/clusters/{cluster_id}` | Legacy tek küme detayı. |
| `GET /api/stats` | Legacy istatistik çıktısı; `traffic_clusters` tablosu gerektirir. |

## Yöntem Özeti

Ana endpoint seçilen tarih ve saatten bir saatlik zaman penceresi oluşturur. Bu pencere içinde `avg_speed < 25` koşulunu sağlayan ölçümler alınır. PostGIS `ST_ClusterDBSCAN` fonksiyonu EPSG:32636 metrik geometri üzerinde çalışır. Saatlik akışta kullanılan parametreler `eps=1000` ve `minpoints=2` değerleridir. Haritadaki işaretçiler araç, sensör veya yol segmenti değil, DBSCAN küme merkezleridir. `congestion_score` kullanıcı arayüzü için hesaplanan yoğunluk/görselleştirme skorudur.

## Test ve Kontrol

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
python -c "from backend.app.main import app; print('app import ok')"
docker-compose --env-file .env.example config
```

Sunucu çalışırken:

```text
http://localhost:8000/api/health
http://localhost:8000/api/clusters?date=2025-01-17&hour=18
```

## Sorun Giderme

| Sorun | Kontrol |
| --- | --- |
| Docker çalışmıyor | Docker Desktop'ı başlatıp `docker-compose up -d` komutunu tekrar çalıştırın. |
| `.env` eksik | `copy .env.example .env` komutunu çalıştırın. |
| Veritabanı boş | CSV dosyasının doğru klasörde olduğundan emin olun ve `python ingest_data.py` çalıştırın. |
| CSV bulunamıyor | Dosya adı `traffic_density_202501.csv`, klasör `data/raw/ibb_hourly_traffic_density/` olmalıdır. |
| `5433` portu dolu | Portu kullanan servisi kapatın veya Docker port ayarını değiştirin. |
| Harita boş | Veri içe aktarılmamış olabilir veya seçilen saat için küme bulunmayabilir. |
| İlk sorgu yavaş | Büyük veri ve PostGIS kümeleme nedeniyle ilk sorgu daha uzun sürebilir. |

## Kısıtlar

- Tam ST-DBSCAN uygulanmamıştır.
- MobilityDB temporal fonksiyonları aktif sorgu yolunda kullanılmaz.
- Sistem geçmiş veri görselleştirmesidir, canlı trafik sistemi değildir.
- Rota tahmini veya rota optimizasyonu yapmaz.
- Ground-truth anomali etiketi üretmez.
