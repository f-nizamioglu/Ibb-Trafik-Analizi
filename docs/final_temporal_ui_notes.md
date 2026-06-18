# Saatlik Trafik Yoğunluğu Arayüzü

## Amaç

Bu belge, İstanbul trafik yoğunluğu analiz arayüzünün son kullanıcıya dönük
çalışma biçimini açıklar. Arayüz, İBB saatlik trafik yoğunluğu verisi üzerinde
seçilen tarih ve saat için trafik yoğunluğu kümelerini harita üzerinde gösterir.

Uygulama bir tahmin sistemi değildir. Canlı arayüz, seçilen tarih-saat kesitinde
veritabanındaki gözlenmiş trafik kayıtlarını kullanır.

## Arayüz ve Kod Dili

- Kullanıcı arayüzü Türkçedir.
- Kod, API parametreleri, dosya adları ve veritabanı alanları İngilizce kalır.
- Örnek kullanıcı etiketleri: `Tarih`, `Saat`, `Yoğunluk Düzeyi`,
  `Saatlik Araç Hacmi`, `Ölçüm Hücresi`.
- Örnek kod/API adları: `date`, `hour`, `severity`, `cluster_id`,
  `fetchTemporalClusters`.

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

Bu yaklaşım, günün son saatinde de aynı mantığın kullanılmasını sağlar.

## Yoğunluk Seviyeleri

Saatlik modda yoğunluk düzeyi AIS skoruna göre değil, seçilen saat içindeki
ortalama hıza göre sınıflandırılır:

| API Değeri | Arayüz Etiketi | Açıklama |
|------------|----------------|----------|
| `HIGH` | Yüksek | Ortalama hız 15 km/h altında |
| `MEDIUM` | Orta | Ortalama hız 15-20 km/h aralığında |
| `LOW` | Düşük | Ortalama hız 20-25 km/h aralığında |

Bu sınıflandırma, tek saatlik kesitte süre ve tekrarlılık bilgisi olmadığı için
AIS yerine hız düşüşünü kullanır.

## Arayüz Metrikleri

**Yoğunluk Düzeyi** seçilen saatlik kümenin hız tabanlı yoğunluk sınıfıdır.
Tek başına araç sayısı bu etiketi belirlemez.

**Saatlik Araç Hacmi** seçilen saat aralığında kümeye giren ölçüm hücrelerindeki
araç akışlarının toplamıdır. Bu değer, işaretçi noktasında aynı anda fiziksel
olarak bulunan araç sayısı değildir.

**Ölçüm Hücresi** kümenin kaç trafik ölçüm noktasından/geohash hücresinden
oluştuğunu gösterir.

Haritadaki işaretçi, kümenin geometrik merkezidir. Tüm araçların fiziksel
konumunu temsil etmez.

## Yol Kapasitesi ve Yol Boyutu

Canlı endpoint, yol kapasitesi için doğrudan yol uzunluğu normalizasyonu
kullanmaz. Bunun yerine ortalama hız düşüşünü, talebin yerel kapasiteye göre
zorlanmasına ilişkin ampirik bir gösterge olarak kullanır.

Ham araç sayısı tek başına tıkanıklığı belirlemek için yeterli değildir. Aynı
araç hacmi kısa bir bağlantı yolunda ciddi tıkanıklık anlamına gelebilirken,
daha uzun veya yüksek kapasiteli bir arterde olağan akışa karşılık gelebilir.

Projede yol yoğunluğu normalizasyonu deneysel katman olarak incelenmiştir.
Ancak canlı endpoint'te kullanılmamaktadır. Geohash sınırları ile yol geometrisi
kesişimleri, çok kısa yol parçaları, eksik OSM kapsamı veya sınıf filtreleri
nedeniyle uç değerler üretebilir. Bu nedenle son arayüzde daha kararlı olan
hız tabanlı saatlik sınıflandırma tercih edilmiştir.

## Yerel Çalıştırma

Veritabanı ve veri hazırlandıktan sonra API sunucusu:

```bash
uvicorn backend.app.main:app --reload --port 8000
```

Arayüz:

```text
http://localhost:8000
```

Örnek API çağrıları:

```bash
curl "http://localhost:8000/api/clusters?date=2025-01-17&hour=18"
curl "http://localhost:8000/api/clusters?date=2025-01-17&hour=18&severity=HIGH"
```

Testler:

```bash
pytest tests/ -q
```

## Bilinen Sınırlılıklar

- Saatlik mod, tek saatlik gözlem kesitini analiz eder; süre ve tekrarlılık
  bileşenlerini içermez.
- Saatlik yoğunluk düzeyi AIS skoru değildir.
- Canlı endpoint tam ε₂ tabanlı ST-DBSCAN uygulaması değildir.
- Canlı endpoint yol uzunluğu başına araç yoğunluğu kullanmaz.
- Sonuçlar yerel veritabanındaki veri kapsamına bağlıdır.
