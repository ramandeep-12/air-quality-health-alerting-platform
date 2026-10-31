import json
from pathlib import Path

import requests

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
POLLUTANTS = ["pm2_5", "pm10", "ozone", "nitrogen_dioxide", "sulphur_dioxide", "carbon_monoxide"]
CITIES = {
    "windsor": (42.3149, -83.0364),
    "toronto": (43.6532, -79.3832),
    "ottawa": (45.4215, -75.6972),
    "montreal": (45.5019, -73.5674),
    "calgary": (51.0447, -114.0719),
    "vancouver": (49.2827, -123.1207),
}
SAMPLES_DIR = Path(__file__).parent / "samples"


def profile_city(city: str, lat: float, lon: float) -> None:
    response = requests.get(
        API_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(POLLUTANTS),
            "past_days": 2,
            "forecast_days": 5,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    SAMPLES_DIR.mkdir(exist_ok=True)
    (SAMPLES_DIR / f"{city}.json").write_text(json.dumps(payload, indent=2))

    hourly = payload["hourly"]
    times = hourly["time"]

    print(f"\n=== {city.upper()} ===")
    print(f"Requested location : {lat}, {lon}")
    print(f"Returned location  : {payload['latitude']}, {payload['longitude']} (grid cell)")
    print(f"Timezone           : {payload.get('timezone')}")
    print(f"Top-level keys     : {list(payload.keys())}")
    print(f"Rows (hours)       : {len(times)}")
    print(f"Time range         : {times[0]} -> {times[-1]}")
    print(f"{'pollutant':<18}{'unit':<10}{'nulls':>6}{'min':>10}{'max':>10}")

    for name in POLLUTANTS:
        values = hourly[name]
        present = [v for v in values if v is not None]
        nulls = len(values) - len(present)
        low = min(present) if present else "-"
        high = max(present) if present else "-"
        unit = payload["hourly_units"][name]
        print(f"{name:<18}{unit:<10}{nulls:>6}{low:>10}{high:>10}")


if __name__ == "__main__":
    for city, (lat, lon) in CITIES.items():
        profile_city(city, lat, lon)
