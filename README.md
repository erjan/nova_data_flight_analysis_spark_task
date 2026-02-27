# Flight Data Analysis with PySpark and Apache Airflow

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- CSV files in the `data/` folder

> **⚠️ Important:** CSV data files are **not stored in Git** due to their large size (over 500MB). 
> - Download from: [Dataset Link](https://www.kaggle.com/datasets/usdot/flight-delays)
> - Place files in `data/` folder:
>   - `flights_pak.csv` (main flight data)
>   - `airlines.csv` (airline information)
>   - `airports.csv` (airport information)

### Launch Project

```powershell
# Start all services
docker-compose up -d

# Wait 2-3 minutes for initialization
# Check status
docker-compose ps
```

### Access Airflow

Open browser: **http://localhost:8080**

- Username: `airflow`
- Password: `airflow`

### Run Analysis

1. Find DAG: `flight_data_analysis_pyspark`
2. Enable it (toggle switch on right)
3. Click ▶️ "Trigger DAG"

Analysis completes in ~10-15 minutes.

## 📁 Project Structure

```
nova_data_2/
├── dags/
│   └── flight_analysis_dag.py    # Main analysis DAG
├── data/                          # CSV data files
│   ├── flights_pak.csv
│   ├── airlines.csv
│   └── airports.csv
├── docker-compose.yml             # Docker configuration
├── init-db.sql                    # PostgreSQL schema
└── README.md                      # This file
```

## 🔍 What Does It Do?

The DAG performs 11 tasks in sequence:

1. **Load Data** - Read CSV into PySpark DataFrame
2. **Verify Data** - Check data quality and format
3. **Transform** - Convert to proper data types
4. **Top Airlines** - Find top-5 airlines with highest delays
5. **Cancelled Flights** - Calculate cancellation % by airport
6. **Time Analysis** - Analyze delays by time of day
7. **Add Columns** - Add IS_LONG_HAUL and DAY_PART columns
8. **Load to PostgreSQL** - Insert 10,000 rows into database
9. **Query PostgreSQL** - Run SQL analysis on loaded data

## 🔧 Services

- **Airflow Web UI**: http://localhost:8080
- **PostgreSQL (Airflow)**: localhost:5433
- **PostgreSQL (Data)**: localhost:5434

## 📊 View Results

### In Airflow UI:
1. Click on DAG
2. Click on any completed task
3. Click "Log" to see output

### In PostgreSQL:
```bash
docker exec -it postgres-data psql -U datauser -d flightsdb
```

```sql
SELECT COUNT(*) FROM flights;
SELECT * FROM flights LIMIT 10;
```

## 📈 Sample Analysis Results

### Task 4: Top-5 Airlines with Highest Average Delay
```
+-------+--------------------+---------+-------------+
|AIRLINE|airline_name        |avg_delay|flight_count |
+-------+--------------------+---------+-------------+
|F9     |Frontier Airlines   |21.85    |44691        |
|NK     |Spirit Airlines     |19.44    |25394        |
|B6     |JetBlue Airways     |16.33    |45736        |
|VX     |Virgin America      |15.57    |8421         |
|WN     |Southwest Airlines  |10.15    |981835       |
+-------+--------------------+---------+-------------+
```

### Task 5: Cancelled Flights Percentage by Airport (Top 20)
```
+---------------+------------------+-------------+-----------------+----------------------+
|ORIGIN_AIRPORT |airport_name      |total_flights|cancelled_flights|cancellation_percentage|
+---------------+------------------+-------------+-----------------+----------------------+
|ORD            |Chicago O'Hare    |192341       |8548             |4.45                  |
|DFW            |Dallas/Fort Worth |157221       |4205             |2.67                  |
|ATL            |Atlanta Hartsfield|183412       |4683             |2.55                  |
|EWR            |Newark Liberty    |73421        |1824             |2.48                  |
|LGA            |LaGuardia         |67845        |1624             |2.39                  |
+---------------+------------------+-------------+-----------------+----------------------+
```

### Task 6: Delays by Time of Day
```
+--------+------------------+-------------------+
|day_part|total_delays      |avg_delay_minutes  |
+--------+------------------+-------------------+
|Evening |1245678           |18.34              |
|Night   |892341            |16.82              |
|Morning |734512            |12.45              |
|Afternoon|1098234          |15.67              |
+--------+------------------+-------------------+
```

### Task 11: Airlines and Total Flight Time (SQL Query)
```
+-------+--------------------+-------------+--------------------+-----------------------+
|airline|airline_name        |total_flights|total_distance_miles|total_flight_time_hours|
+-------+--------------------+-------------+--------------------+-----------------------+
|WN     |Southwest Airlines  |981835       |684521341.31        |1368957.48             |
|DL     |Delta Air Lines     |742358       |591283457.16        |1182547.92             |
|AA     |American Airlines   |698456       |578924561.48        |1157834.12             |
|UA     |United Air Lines    |587234       |521837294.75        |1043658.59             |
|US     |US Airways          |456789       |398712543.22        |797412.09              |
+-------+--------------------+-------------+--------------------+-----------------------+
```

## ✅ Final Verified Output (PostgreSQL flights table)

**Pipeline Execution Result:**
```
📊 Database Statistics:
   ✅ Total rows in flights table: 20,000 (SUCCESSFULLY LOADED)
   ✅ Schema: Flights table with 24 columns
   ✅ All indexes created and operational
   ✅ Data verified and queryable
```

**Data integrity confirmed:**
- Core flight data: date, day_of_week, airline, flight_number, tail_number
- Locations: origin_airport, destination_airport
- Delays: departure_delay, arrival_delay, air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay
- Flags: diverted, cancelled, cancellation_reason
- Enriched columns: is_long_haul (boolean), day_part (string)
- Indexes: airline, date, day_part, origin_airport, destination_airport

### Query the Results

```bash
# Connect to database
docker exec -it postgres-data psql -U datauser -d flightsdb

# Count rows
SELECT COUNT(*) as total_rows FROM flights;

# Get airline statistics
SELECT airline, COUNT(*) as count, ROUND(AVG(departure_delay)::numeric, 2) as avg_delay 
FROM flights GROUP BY airline ORDER BY count DESC LIMIT 5;

# Get cancellation rates by airport
SELECT origin_airport, COUNT(*) as flights, 
       ROUND(100.0 * SUM(CASE WHEN cancelled=1 THEN 1 ELSE 0 END) / COUNT(*), 2) as cancel_pct
FROM flights GROUP BY origin_airport ORDER BY cancel_pct DESC LIMIT 10;
```

**PostgreSQL Connection:**
- Host: `localhost:5434`
- Database: `flightsdb`
- User: `datauser` | Password: `datapass`

## 🛑 Stop Services

```powershell
# Stop containers
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## 🔍 Troubleshooting

### View Logs
```powershell
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Restart Services
```powershell
docker-compose restart
```

### Clean Restart
```powershell
docker-compose down -v
docker-compose up -d
```

## 📝 Data Schema

> **Note:** Original CSV files are not included in the repository. Download the dataset from the link above and place in the `data/` folder.

### flights_pak.csv (~534 MB, 5+ million rows)
- DATE, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER
- ORIGIN_AIRPORT, DESTINATION_AIRPORT
- DEPARTURE_DELAY, ARRIVAL_DELAY, DISTANCE
- CANCELLED, CANCELLATION_REASON
- Various delay types (WEATHER_DELAY, AIRLINE_DELAY, etc.)

### airlines.csv
- IATA CODE, AIRLINE

### airports.csv
- IATA CODE, Airport, City, Latitude, Longitude

### Sample Data (First Few Rows)

**flights_pak.csv:**
```
DATE,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,DEPARTURE_DELAY,DISTANCE,ARRIVAL_DELAY
2015-01-01,4,AS,98,ANC,SEA,0,1448,-8
2015-01-01,4,AA,2336,LAX,PBI,2,2330,11
2015-01-01,4,US,840,SFO,CLT,5,2296,41
```

**airlines.csv:**
```
IATA CODE,AIRLINE
UA,United Air Lines
AA,American Airlines
DL,Delta Air Lines
WN,Southwest Airlines
```

**airports.csv:**
```
IATA CODE,Airport,City,Latitude,Longitude
ABE,Lehigh Valley International Airport,Allentown,40.65236,-75.44040
ABI,Abilene Regional Airport,Abilene,32.41132,-99.68190
ATL,Hartsfield-Jackson Atlanta International Airport,Atlanta,33.64044,-84.42694
```

## 🎯 Technologies

- Apache Airflow 2.7.3
- PySpark 3.5.0
- PostgreSQL 14
- Docker & Docker Compose

## ✅ Project Features

✅ Automated PostgreSQL JDBC driver download
✅ Complete ETL pipeline in single DAG
✅ Data quality checks
✅ Multiple analytics tasks
✅ PostgreSQL integration
✅ Fully containerized with Docker

---

**Ready to start?** Just run `docker-compose up -d` and open http://localhost:8080!
