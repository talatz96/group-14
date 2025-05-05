import requests
import csv

# API URL for fetching weather data
URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1 & 2: Connecting to an API and Fetching Data

def fetch_weather_data():
    """Fetches weather data from the API and returns JSON response."""
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def save_to_csv(data, filename):
    """Saves fetched weather data into a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Writing header row
        writer.writerow(["Time", "Temperature (°C)", "Humidity (%)", "Wind Speed (m/s)"])
        
        # Extract hourly data
        times = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        humidities = data["hourly"]["relative_humidity_2m"]
        wind_speeds = data["hourly"]["wind_speed_10m"]

        # Writing data rows
        for i in range(len(times)):
            writer.writerow([times[i], temperatures[i], humidities[i], wind_speeds[i]])
    print(f"Data saved to {filename}")


### Part 3: Cleaning the Dataset

def clean_data(input_file, output_file):
    """Cleans weather data by filtering out invalid values."""
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Read and write the header
        header = next(reader)
        writer.writerow(header)

        # Filter and clean data
        for row in reader:
            try:
                temp = float(row[1])
                humidity = float(row[2])
                wind_speed = float(row[3])
                
                if (0 <= temp <= 60) and (0 <= humidity <= 80) and (3 <= wind_speed <= 150):
                    writer.writerow(row)  # Only write valid rows
            except ValueError:
                continue  # Skip rows with invalid data
    
    print(f"Cleaned data saved to {output_file}")


### Part 4: Aggregation

def summarize_data(filename):
    """Computes summary statistics for cleaned weather data."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list

        if not data:
            print("No data available to summarize.")
            return

        # Extract numerical values
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        total_records = len(data)
        avg_temp = sum(temperatures) / total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / total_records
        avg_wind_speed = sum(wind_speeds) / total_records

        # Print summary
        print("\n Weather Data Summary")
        print(f"Total Records: {total_records}")
        print(f"Average Temperature: {avg_temp:.2f}°C")
        print(f" Max Temperature: {max_temp:.2f}°C")
        print(f"Min Temperature: {min_temp:.2f}°C")
        print(f" Average Humidity: {avg_humidity:.1f}%")
        print(f" Average Wind Speed: {avg_wind_speed:.2f} m/s")


if __name__ == "__main__":
    # Fetch and save raw weather data
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        
        # Clean the dataset
        clean_data("weather_data.csv", "cleaned_data.csv")
        
        # Summarize the cleaned data
        summarize_data("cleaned_data.csv")
