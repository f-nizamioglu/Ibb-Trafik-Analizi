# Zaman-Uzamsal Verilerle Trafik Yoğunluğu Analizi ve Görselleştirme Web Portalı

Bu depo, YTÜ Bilgisayar Mühendisliği bitirme projesi için hazırlanmıştır. Proje, İstanbul Büyükşehir Belediyesi saatlik trafik yoğunluğu verisini kullanır.

FastAPI, PostgreSQL/PostGIS ve Leaflet.js ile tarih/saat seçimine göre trafik yoğunluğu kümelerini harita üzerinde gösterir. Kullanıcı bir tarih ve saat seçer; sistem ilgili bir saatlik aralıktaki düşük hızlı ölçümleri filtreleyip PostGIS ile mekansal kümeleme yapar.

Uygulama geçmiş veriyi görselleştirir; trafik tahmini, rota optimizasyonu veya gerçek zamanlı trafik yönetimi yapmaz.

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

`ingest_data.py` PostGIS eklentisini, `ibb_traffic_density` ana tablosunu, temel indeksleri ve `ingested_files` takip tablosunu oluşturur.

`create_views.py`, `high_congestion_zones` görünümünü ve eski toplulaştırılmış uç noktalar için boş `traffic_clusters` tablosunu hazırlar.

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
| `GET /api/clusters?date=YYYY-MM-DD&hour=HH&min_lon=...&min_lat=...&max_lon=...&max_lat=...` | Ana tarih/saat sorgusunu WGS84 dikdörtgen bbox alanıyla sınırlar. |
| `GET /api/clusters?date=YYYY-MM-DD&hour=HH&district=besiktas` | Ana tarih/saat sorgusunu içe aktarılan gerçek ilçe poligonuyla sınırlar. |
| `GET /api/districts/{district_key}/boundary` | Seçili ilçenin sınır poligonunu WGS84 (EPSG:4326) GeoJSON olarak döndürür; harita üzerinde çizgi olarak gösterilir. |
| `GET /api/districts/besiktas/boundary` | Beşiktaş için örnek ilçe sınır endpoint'i; haritadaki ilçe poligonu bindirmesini besler. |
| `GET /api/clusters?date=YYYY-MM-DD&hour=HH&severity=HIGH` | Ana sorguyu `LOW`, `MEDIUM` veya `HIGH` yoğunluk düzeyiyle filtreler. |
| `GET /api/heatmap` | Isı haritası noktalarını döndürür. |
| `GET /api/heatmap?date=YYYY-MM-DD` | Isı haritası noktalarını tarihe göre filtreler. |
| `GET /api/clusters` | Legacy toplulaştırılmış küme çıktısı; `run_pipeline.py` sonrası anlamlıdır. |
| `GET /api/clusters/{cluster_id}` | Legacy tek küme detayı. |
| `GET /api/stats` | Legacy istatistik çıktısı; `traffic_clusters` tablosu gerektirir. |

## Bölge Analizi (Bölgesel Filtreleme)

Arayüzdeki **Bölge Analizi** bölümü, tarih/saat sorgusunu belirli bir alanla sınırlamak için dört kapsam sunar. Bunlardan üçü (Harita Görünümü, Dikdörtgen Seç ve dolaylı olarak başlangıç görünümü) opsiyonel WGS84 dikdörtgen (bbox) parametrelerini (`min_lon`, `min_lat`, `max_lon`, `max_lat`) kullanır; **İlçe poligonu** modu ise bbox yerine içe aktarılan gerçek ilçe poligonuna karşılık gelen `district` parametresini gönderir. `district` ve bbox parametreleri birlikte kullanılamaz (aynı anda gönderilirse HTTP 400 döner).

- **Tüm İstanbul** (varsayılan): bbox parametresi gönderilmez. Seçili dikdörtgen kaldırılır, ilçe seçimi sıfırlanır ve kapsam metni `Kapsam: Tüm İstanbul` olur. Bu mod mevcut `/api/clusters?date=YYYY-MM-DD&hour=HH` davranışını değiştirmeden korur.
- **Harita Görünümü**: o anki görünür harita sınırları (`map.getBounds()`) bbox olarak gönderilir. Kaydırma/yakınlaştırma otomatik yeni istek tetiklemez; kullanıcı **Yenile** düğmesine basınca güncel sınırlar yeniden alınır. Kapsam metni: `Kapsam: Harita görünümü`.
- **Dikdörtgen Seç**: kullanıcı haritada fareyle sürükleyerek bir dikdörtgen çizer (yalnızca Leaflet `L.rectangle` ve `mousedown`/`mousemove`/`mouseup` kullanılır; harici eklenti yoktur). Çizim sırasında istek atılmaz; seçili dikdörtgen haritada kalır ve **Seçili Alanı Analiz Et** düğmesine basıldığında bbox sorgusu çalışır. Kapsam metni: `Kapsam: Seçili dikdörtgen alan` ve seçilen alanın koordinatları panelde gösterilir.
- **İlçe poligonu**: `Beşiktaş`, `Kadıköy`, `Şişli`, `Üsküdar`, `Fatih`, `Bakırköy`, `Ataşehir`, `Maltepe`, `Pendik`, `Sarıyer` ilçeleri açılır listede yer alır. İlçe seçildiğinde arayüz `district=<ilçe_anahtarı>` sorgusu gönderir (örn. `district=fatih`); backend bu ilçenin **gerçek poligonunu** kullanarak ölçümleri filtreler. bbox kullanılmaz. Ayrıca arayüz `/api/districts/<ilçe_anahtarı>/boundary` ucundan ilçe sınır poligonunu alıp haritada **sınır çizgisi** olarak çizer. Kapsam metni örn. `Kapsam: Fatih ilçesi`, sorgu türü `Tarih/saat + ilçe poligonu`. İlçe sınırları veritabanına aktarılmamışsa arayüz net bir Türkçe uyarı gösterir ve sessizce bbox'a düşmez.

> **Not:** İlçe modu gerçek ilçe poligonlarını kullanır; bu poligonlar yerel olarak sağlanan `data/boundaries/istanbul_districts.geojson` dosyasından `istanbul_district_boundaries` tablosuna içe aktarılmalıdır (aşağıya bakın). İlçe seçildiğinde sınır poligonu haritada çizgiyle gösterilir ve harita o poligona sığdırılır. GeoJSON dosyası depoya eklenmez; yerelde sağlanır ve izlenmez.

Bölge panelinde aktif kapsam, sorgu türü (`Tarih/saat + tüm alan`, `Tarih/saat + bbox` veya `Tarih/saat + ilçe poligonu`), bbox modunda koordinatlar ve ilçe modunda ilçe adı gösterilir. Dört metrik kartı her zaman **aktif kapsam** için hesaplanır.

Bölge filtresi aktif olduğunda sistem, seçilen alan içindeki ölçümleri alıp DBSCAN kümelemesini bu alt veri üzerinde yeniden hesaplar. Bu nedenle bölgesel marker merkezleri Tüm İstanbul görünümündeki marker merkezleriyle birebir aynı olmak zorunda değildir.

Örnek bbox endpoint'i:

```text
/api/clusters?date=2025-01-17&hour=18&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10
```

`min_lon`, `min_lat`, `max_lon` ve `max_lat` koordinatları WGS84 longitude/latitude değerleridir. Bu dört parametre birlikte verildiğinde sistem seçilen bir saatlik pencereyi bu dikdörtgen alanla sınırlar, ardından mevcut `avg_speed < 25` ve PostGIS `ST_ClusterDBSCAN` akışını aynı şekilde çalıştırır. Yoğunluk düzeyi (`severity=HIGH|MEDIUM|LOW`) filtresi bbox ile birlikte kullanılabilir.

Örnek ilçe (poligon) endpoint'leri:

```text
/api/clusters?date=2025-01-17&hour=18&district=besiktas
/api/clusters?date=2025-01-17&hour=18&district=besiktas&severity=HIGH
/api/districts/besiktas/boundary
```

İlçe modunda sistem önce ilgili ilçenin poligonunu (`istanbul_district_boundaries`) alır, bir saatlik pencere ve `avg_speed < 25` koşulundan sonra ölçümleri `t.geom && d.geom AND ST_Intersects(t.geom, d.geom)` ile bu poligona göre filtreler ve **ardından** PostGIS `ST_ClusterDBSCAN` çalıştırır. Yani kümeleme ilçe alt kümesi üzerinde yeniden hesaplanır; küresel kümeler sonradan kırpılmaz. Yoğunluk düzeyi filtresi ilçe ile birlikte kullanılabilir.

### İlçe poligonu verisi ve içe aktarma

İlçe modu, yerel olarak sağlanan bir GeoJSON dosyasına dayanır. Bu dosya **otomatik indirilmez** ve kaynağı/lisansı açıkça onaylanmadıkça Git ile izlenmez (depoda yalnızca `data/boundaries/.gitkeep` tutulur).

1. İstanbul ilçe sınırlarını içeren bir GeoJSON `FeatureCollection` dosyasını şu konuma yerleştirin:

   ```text
   data/boundaries/istanbul_districts.geojson
   ```

2. Poligonları PostGIS'e aktarın (tablo ve indeksler gerekirse otomatik oluşturulur, EPSG:32636'ya dönüştürülür):

   ```powershell
   python scripts/import_district_boundaries.py
   ```

İçe aktarma, ilçe adını yaygın özellik adlarından (`name`, `NAME`, `district`, `DISTRICT`, `ilce`, `ILCE`, `Ilce`, `İLÇE`, `ADI`, `ad`, `AD`) okur ve güvenli bir ilçe anahtarına normalize eder (örn. `Beşiktaş → besiktas`, `Kadıköy → kadikoy`). Aynı normalize fonksiyonu API tarafında da kullanıldığından `district=Beşiktaş` gibi Türkçe yazımlar da kabul edilir. TRUNCATE ve tüm INSERT'ler tek bir işlemde (transaction) çalışır; bir hata olursa tablo önceki içeriğiyle bozulmadan kalır. Boş geometriler atlanır ve içe aktarma sonunda kısa bir doğrulama özeti yazılır (ilçe sayısı, SRID, geçersiz/boş geometri sayısı ve WGS84 zarfı; zarf İstanbul aralığının dışındaysa uyarı verilir). Dosya yoksa script net bir hata ile durur. Sınırlar içe aktarılmadan ilçe modu kullanılırsa API, `503` ile birlikte `District boundaries are not imported. Run python scripts/import_district_boundaries.py.` mesajını döndürür ve arayüz bunu Türkçe bir uyarı olarak gösterir.

> **Not:** İlçe modu yalnızca geçerli bir GeoJSON içe aktarıldıktan sonra çalışır; gerçek poligon doğrulaması (SRID, geçerlilik, sınır kontrolü) bu içe aktarma adımıyla yapılır.

## Yöntem Özeti

Ana endpoint seçilen tarih ve saatten bir saatlik zaman penceresi oluşturur. Bu pencere içinde `avg_speed < 25` koşulunu sağlayan ölçümler alınır. PostGIS `ST_ClusterDBSCAN` fonksiyonu EPSG:32636 metrik geometri üzerinde çalışır.

Saatlik akışta kullanılan parametreler `eps=1000` ve `minpoints=2` değerleridir. Haritadaki işaretçiler araç, sensör veya yol segmenti değil, DBSCAN küme merkezleridir. `congestion_score` kullanıcı arayüzü için hesaplanan yoğunluk/görselleştirme skorudur.

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
