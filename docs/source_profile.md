# Source Profile: Open-Meteo Air Quality API

## Overview
| Item | Value |
|---|---|
| Endpoint | `https://air-quality-api.open-meteo.com/v1/air-quality` |
| Authentication | None (free, no API key) |
| Format | JSON |
| Data provider | CAMS (Copernicus Atmosphere Monitoring Service), via Open-Meteo |
| Licence requirement | Attribution to CAMS ENSEMBLE and Open-Meteo is required |
| Docs | https://open-meteo.com/en/docs/air-quality-api |

## Request used
- `latitude`, `longitude`: city coordinates
- `hourly`: pm2_5, pm10, ozone, nitrogen_dioxide, sulphur_dioxide, carbon_monoxide
- `past_days=2`, `forecast_days=5`
- No `timezone` parameter, so times are returned in GMT (UTC)

## Response structure
- Metadata: `latitude`, `longitude`, `elevation`, `timezone`, `utc_offset_seconds`, `generationtime_ms`
- `hourly_units`: unit per variable
- `hourly`: columnar arrays (`time` plus one array per pollutant), same length

## Profiling results (6 cities, run on 2026-10-31)
| Finding | Detail |
|---|---|
| Grain | One row per grid cell per hour |
| Rows per call | 168 (7 days x 24 hours), no gaps |
| Time range | Starts at 00:00 UTC two days before the run date, ends 23:00 UTC four days after |
| Timezone | GMT, offset 0 |
| Location | API snaps to a ~0.1 degree grid (e.g. Windsor 42.3149,-83.0364 returned as 42.3,-83.0) |
| Units | All pollutants in ug/m3, including carbon monoxide |
| Nulls | None in sample |
| Value ranges | PM2.5 0.3-46.0, ozone 0.0-96.0, NO2 0.9-78.2, CO 92-1499 (ug/m3) |

## Design decisions
1. **Store times as UTC `TIMESTAMPTZ`**; convert to local time only for display.
2. **Store requested and returned coordinates** for lineage.
3. **Keep raw units in raw**; unit conversions (e.g. ozone and NO2 to ppb for AQHI) happen in dbt.
4. **Pollutant columns are nullable**; completeness is enforced by quality checks, not by the load.
5. **Ozone of 0 is valid** (night-time urban ozone); the quality rule is "no negative values".
6. **Raw is append-only.** Daily runs overlap and forecasts get revised, so each run is kept as a snapshot keyed by fetch time. The latest version per city and hour is selected in dbt.
