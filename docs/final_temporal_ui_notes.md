# Saatlik Trafik Yoğunluğu Arayüzü

## Amaç

Bu belge, İstanbul trafik yoğunluğu analiz arayüzünün son kullanıcıya dönük
çalışma biçimini açıklar. Arayüz, İBB saatlik trafik yoğunluğu verisi üzerinde
seçilen tarih ve saat için trafik yoğunluk kümelerini harita üzerinde gösterir.

Uygulama bir tahmin sistemi değildir. Canlı arayüz, seçilen tarih-saat kesitinde
veritabanındaki gözlenmiş trafik kayıtlarını kullanır.

Bu harita ham SQL/DBSCAN nokta saçılımı değildir. Kullanıcıya yönelik bir trafik
yoğunluğu / tıkanıklık riski görselleştirmesidir. Renk ve boyut; hız düşüşü,
saatlik araç hacmi ve küme kapsamından türetilen `congestion_score` ile
ilişkilidir. İşaretçiler küme merkezlerini gösterir.

## Arayüz ve Kod Dili

- Kullanıcı arayüzü Türkçedir.
- Kod, API parametreleri, dosya adları ve veritabanı alanları İngilizce kalır.
- Örnek kullanıcı etiketleri: `Tarih`, `Saat`, `Yoğunluk Düzeyi`,
  `Küme Ortalama Hızı`, `Saatlik Araç Hacmi`, `Yüksek Yoğunluk`, `Hücre Sayısı`.
- Örnek kod/API adları: `date`, `hour`, `severity`, `congestion_score`,
  `cluster_id`, `fetchTemporalClusters`.

## Temporal Endpoint

Arayüzün kullandığı temel endpoint:

```text
GET /api/clusters?date=YYYY-MM-DD&hour=HH
```

Seviyeye göre filtreleme isteğe bağlıdır:

```text
GET /api/clusters?date=YYYY-MM-DD&hour=HH&severity=HIGH
```

Kurallar:

- `date` değeri `YYYY-MM-DD` biçiminde gerçek bir takvim tarihi olmalıdır.
- Desteklenen veri aralığı `2020-01-01` ile `2025-01-31` arasındadır.
- `hour` değeri 0-23 arasında bir tam sayı olmalıdır.
- `severity` değeri verilirse `HIGH`, `MEDIUM` veya `LOW` olmalıdır.
- `date` ve `hour` birlikte verilmelidir; yalnızca biri verilirse API 400 döner.

## Saat Seçimi

Saat kaydırıcısı 0 ile 23 arasında çalışır. Her değer bir saatlik yarı açık
zaman aralığını temsil eder:

```text
18 -> 18:00:00 <= record_time < 19:00:00
23 -> 23:00:00 <= record_time < ertesi gün 00:00:00
```

## Yoğunluk Skoru ve Seviyeler

Önceki sürümde yoğunluk düzeyi yalnızca küme ortalama hızına göre atanıyordu.
Bu yaklaşım yanıltıcıydı çünkü:

- `Düşük` etiketi düşük trafik gibi okunabiliyordu; oysa aday kümeler zaten
  yavaş hız filtresinden geçmiştir.
- Yoğun akşam saatlerinde (ör. 18:00) yüksek araç hacmi görsel olarak yeterince
  vurgulanmıyordu.
- Gece saatlerinde (ör. 23:00) düşük hacimli küçük kümeler aynı görsel ağırlıkta
  kalabiliyordu.

Güncel saatlik modda her küme için `congestion_score` (0-100) hesaplanır:

```text
congestion_score = 100 * (
    0.45 * speed_component
  + 0.35 * volume_component
  + 0.15 * coverage_component
  + 0.05 * min_speed_component
)
```

Bileşenler:

| Bileşen | Kaynak | Açıklama |
|---------|--------|----------|
| `speed_component` | `avg_speed_kmh` | 10 km/h → 1.0, 30 km/h → 0.0 (doğrusal) |
| `volume_component` | `sum_vehicle_count` | Saat içi göreli log ölçek; mutlak düşük hacim cezası |
| `coverage_component` | `point_count` | Küçük (2 hücreli) kümeler daha düşük güven |
| `min_speed_component` | `min_speed_kmh` | Küme içi en yavaş hücreyi yakalar |

Seviye eşikleri (audit verisine göre ayarlanmış mutlak hacim korumaları ile):

| API | Arayüz | Koşul özeti |
|-----|--------|-------------|
| `HIGH` | Yüksek Yoğunluk | `congestion_score >= 75` ve `avg_speed < 22` ve hacim ≥ 500 ve hücre ≥ 2; veya `avg_speed <= 14.5` ve hacim ≥ 180; veya `min_speed <= 11` ve hacim ≥ 300 |
| `MEDIUM` | Orta Yoğunluk | `congestion_score >= 48` ve `avg_speed < 27` ve hacim ≥ 120 |
| `LOW` | Düşük Yoğunluk | Diğer aday kümeler |

## Arayüz Metrikleri

**Küme Ortalama Hızı** seçilen saatteki tüm kümelerin hücre ağırlıklı ortalama
hızıdır; İstanbul geneli ortalama hızı değildir.

**Saatlik Araç Hacmi** seçilen saat aralığında kümeye giren ölçüm hücrelerindeki
araç akışlarının toplamıdır.

**Yüksek Yoğunluk** seçilen saatte `HIGH` sınıfındaki küme sayısıdır.

**Hücre Sayısı** kümenin kaç trafik ölçüm hücresinden (geohash tabanlı) oluştuğunu
gösterir.

Haritadaki işaretçi, DBSCAN kümesinin geometrik merkezidir. Gerçek araç, sensör
veya yol noktası değildir. Popup bu sınırlılığı açıkça belirtir.

## Yol Kapasitesi ve Yol Boyutu

Canlı endpoint yol uzunluğu normalizasyonu kullanmaz. Yol yoğunluğu deneyleri
(`geohash_area`, `road_length`) yalnızca deneysel katmandadır.

## Doğrulama Örnekleri (2025-01-17)

Beklenen davranış:

- 18:00, 23:00'dan görsel olarak daha yoğun okunmalıdır (daha yüksek toplam hacim,
  daha büyük/koyu işaretçiler, daha fazla orta/yüksek yoğunluk).
- 23:00'da düşük hacimli kümeler çoğunlukla küçük ve daha şeffaf kalmalıdır.
- Tüm saatlerde harita kırmızıya dönmemelidir.

## Yerel Çalıştırma

```bash
uvicorn backend.app.main:app --reload --port 8000
```

Arayüz: `http://localhost:8000`

Örnek API çağrıları:

```bash
curl "http://localhost:8000/api/clusters?date=2025-01-17&hour=18"
curl "http://localhost:8000/api/health"
```

Testler:

```bash
pytest tests/ -q
```

## Bilinen Sınırlılıklar

- Saatlik mod tek saatlik gözlem kesitini analiz eder; süre ve tekrarlılık
  bileşenlerini içermez (bunlar yalnızca legacy AIS modunda vardır).
- Canlı endpoint tam ε₂ tabanlı ST-DBSCAN uygulaması değildir.
- Canlı endpoint yol uzunluğu başına araç yoğunluğu kullanmaz.
- Sonuçlar yerel veritabanındaki veri kapsamına bağlıdır.
