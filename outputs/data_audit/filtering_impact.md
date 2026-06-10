# Filtering Pipeline Impact Audit

Dataset: 61 months  (2020-01-01 00:00:00 to 2025-01-31 23:00:00)

## Stage Summary

| Stage | Rows | % of Raw | Geohashes |
|-------|------|----------|-----------|
| A. Raw (ibb_traffic_density) | 99,628,851 | 100% | 5,014 |
| B. Static filter (speed<20 AND vehicles>500) | 9,633 | 0.0097% | 174 |
| C. Road-density filter (75th pct vehicles/road-km) | 188,114 | 0.1888% | 821 |
| D. After ST_ClusterDBSCAN (total input) | 9,633 | 0.0097% | — |
| E. Non-noise cluster points | 9,555 | 0.0096% | — |

## A. Raw Table

| Metric | Value |
|--------|-------|
| Total rows | 99,628,851 |
| Distinct months | 61 |
| Date range | 2020-01-01 00:00:00 to 2025-01-31 23:00:00 |
| Distinct geohashes | 5,014 |

## B. Static Candidate Filter (`high_congestion_zones`)

Condition: `avg_speed < 20 km/h AND vehicle_count > 500`

| Condition | Rows | % of Raw |
|-----------|------|----------|
| speed < 20 km/h only | 3,953,680 | 3.9684% |
| vehicle_count > 500 only | 1,044,854 | 1.0487% |
| **Both conditions (active filter)** | **9,633** | **0.0097%** |
| Distinct geohashes after filter | 174 | — |
| `high_congestion_zones` view rows | 9,633 | matches filter count |

## C. Road-Length-Aware Density Filter

Method: `vehicles_per_road_km` above the 75th percentile threshold.

| Metric | Value |
|--------|-------|
| Candidate rows | 188,114 |
| % of raw | 0.1888% |
| Distinct geohashes | 821 |
| Static-filter geohashes also in road-density | 156 |
| Rows with no OSM road coverage | 34,375,270 (34.5033%) |

## D–E. Clustering Output (`traffic_clusters`)

Input: 9,633 static-filter records  -> `ST_ClusterDBSCAN(eps=500m, minpoints=3)`

| Metric | Value |
|--------|-------|
| Total clustered input records | 9,633 |
| Non-noise clusters | 116 |
| Noise points (cluster_id = -1) | 78 |
| Noise percentage | 0.8% |
| AIS range | 0.044 – 0.769 (mean 0.347) |
| Severity distribution | HIGH=3 MEDIUM=59 LOW=54 |

## Notes

- The IBB CSV files are **raw hourly aggregated traffic-density data**, not pre-filtered.
- The pipeline applies filtering after ingestion; `ibb_traffic_density` stores raw rows.
- Stage B (static filter) and Stage C (road-density) are alternative candidate selection methods, not sequential stages.
- The active pipeline uses Stage B (static filter) as input to ST_ClusterDBSCAN.
- Stage C (road-density) requires OSM road network data (`road_segments` table).
