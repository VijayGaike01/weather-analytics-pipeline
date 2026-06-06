import json
import requests
from datetime import datetime


# Get Timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Read config
with open("config/config.json", "r") as file:
    config = json.load(file)

api_key = config["api_key"]
cities = config["cities"]

successful_cities = []
failed_cities = []

for city in cities:
    try:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?q={city}&appid={api_key}&units=metric"
        )

        response = requests.get(url)

        if response.status_code == 200:

                weather_data = response.json()

                successful_cities.append(weather_data)

                print(f"[SUCCESS] {city}")

        else:

            failed_cities.append({
                "city": city,
                "status_code": response.status_code,
                "error": response.text
            })

            print(f"[FAILED] {city}")

    except Exception as e:

        failed_cities.append({
            "city": city,
            "error": str(e)
        })

        print(f"[ERROR] {city} : {e}")

#final output structure
output_data = {
    "ingestion_timestamp": timestamp,
    "successful_records": len(successful_cities),
    "failed_records": len(failed_cities),
    "weather_data": successful_cities,
    "failed_cities": failed_cities
}

# Save Single File Per Run
output_file = f"data/raw/weather_{timestamp}.json"

with open(output_file, "w") as file:
    json.dump(output_data, file, indent=4)

print("\n========== INGESTION SUMMARY ==========")
print(f"Cities Requested : {len(cities)}")
print(f"Success          : {len(successful_cities)}")
print(f"Failed           : {len(failed_cities)}")
print(f"Output File      : {output_file}")
print("=======================================\n")