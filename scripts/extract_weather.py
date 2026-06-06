import json
import requests
from datetime import datetime
import logging


# Get Timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

#logger config
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Read config
with open("config/config.json", "r") as file:
    config = json.load(file)

api_key = config["api_key"]
cities = config["cities"]

successful_cities = []
failed_cities = []

logging.info("Starting weather ingestion process")

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

                logging.info(
                    f"Successfully fetched weather data for {city}"
                )

        else:

            failed_cities.append({
                "city": city,
                "status_code": response.status_code,
                "error": response.text
            })

            logging.error(
                f"Failed to fetch weather data for {city}. "
                f"Status Code: {response.status_code}"
            )

    except Exception as e:

        failed_cities.append({
            "city": city,
            "error": str(e)
        })

        logging.exception(
            f"Exception occurred while processing city {city}"
        )

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

logging.info(
    f"Output file generated successfully: {output_file}"
)

print("\n========== INGESTION SUMMARY ==========")
print(f"Cities Requested : {len(cities)}")
print(f"Success          : {len(successful_cities)}")
print(f"Failed           : {len(failed_cities)}")
print(f"Output File      : {output_file}")
print("=======================================\n")

logging.info(
    f"Ingestion completed. "
    f"Success={len(successful_cities)}, "
    f"Failed={len(failed_cities)}"
)