"""
Airflow DAG для анализа данных о полетах с помощью PySpark
Flight Data Analysis DAG using PySpark
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, count, sum as spark_sum, 
    to_date, hour, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DateType
)
import os

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# PostgreSQL connection parameters
POSTGRES_HOST = "postgres-data"
POSTGRES_PORT = "5432"
POSTGRES_DB = "flightsdb"
POSTGRES_USER = "datauser"
POSTGRES_PASSWORD = "datapass"
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Data file paths
DATA_DIR = "/opt/airflow/data"
FLIGHTS_FILE = f"{DATA_DIR}/flights_pak.csv"
AIRLINES_FILE = f"{DATA_DIR}/airlines.csv"
AIRPORTS_FILE = f"{DATA_DIR}/airports.csv"


def create_spark_session():
    """Create and configure Spark session"""
    import os
    os.environ['SPARK_HOME'] = '/opt/spark'
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
    
    spark = SparkSession.builder \
        .appName("FlightDataAnalysis") \
        .master("local[*]") \
        .config("spark.jars", "/opt/airflow/jdbc/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/jdbc/postgresql-42.6.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/jdbc/postgresql-42.6.0.jar") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    return spark


def read_flights(spark):
    """Read flights data with BOM/encoding handling and cache as Parquet."""
    parquet_path = f"{DATA_DIR}/flights_parquet"

    # If parquet cache exists, read from it to avoid repeated CSV parsing
    if os.path.exists(parquet_path):
        df = spark.read.parquet(parquet_path)
        print(f"✅ Read flights from parquet: {parquet_path}. Rows: {df.count()}")
        return df


    # Try reading with default/UTF-8 first, then try UTF-16LE if the first fails
    try:
        df = spark.read.csv(FLIGHTS_FILE, header=True, inferSchema=True)
        print(f"✅ CSV flights read (default). Columns: {df.columns}")
    except Exception as e1:
        print(f"⚠️ Default CSV read failed: {e1}")
        try:
            df = spark.read.option("encoding", "UTF-16LE") \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .csv(FLIGHTS_FILE, header=True, inferSchema=True)

            print(f"✅ CSV flights read (UTF-16LE). Columns: {df.columns}")
        except Exception as e2:
            print(f"❌ Both CSV read attempts failed: default error={e1}; utf16 error={e2}")
            raise

    # Persist cleaned dataframe as parquet for subsequent tasks
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"✅ Wrote flights parquet to: {parquet_path}")

    return spark.read.parquet(parquet_path)


def task_1_load_data(**context):
    """
    Task 1: Загрузка данных в PySpark DataFrame
    Load data into PySpark DataFrame and print row count
    """
    spark = create_spark_session()
    
    # Load flights data (use helper to handle encoding and caching)
    df_flights = read_flights(spark)
    
    # Print row count
    row_count = df_flights.count()
    print(f"Количество строк в файле flights: {row_count}")
    print(f"Number of rows in flights file: {row_count}")
    
    # Store for next tasks
    context['ti'].xcom_push(key='row_count', value=row_count)
    
    spark.stop()


def task_2_verify_data(**context):
    """
    Task 2: Проверка корректности данных
    Verify data is correctly read
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    
    print("Схема данных / Data Schema:")
    df_flights.printSchema()
    
    print("\nПервые 5 строк / First 5 rows:")
    df_flights.show(5)
    
    # Check for empty rows
    total_rows = df_flights.count()
    non_null_rows = df_flights.dropna(how='all').count()
    
    print(f"\nВсего строк / Total rows: {total_rows}")
    print(f"Строк без пустых значений / Non-empty rows: {non_null_rows}")
    print(f"Пустых строк / Empty rows: {total_rows - non_null_rows}")
    
    # Check for null values in key columns
    print("\nПроверка NULL значений в ключевых колонках:")
    for column in ['DATE', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']:
        null_count = df_flights.filter(col(column).isNull()).count()
        print(f"{column}: {null_count} NULL values")
    
    spark.stop()


def task_3_transform_data(**context):
    """
    Task 3: Преобразование данных в правильные типы
    Transform data to appropriate types
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    
    # Convert DATE to date type
    df_flights = df_flights.withColumn("DATE", to_date(col("DATE"), "yyyy-MM-dd"))
    
    # Ensure numeric columns are proper types
    numeric_columns = [
        'DAY_OF_WEEK', 'FLIGHT_NUMBER', 'DEPARTURE_DELAY', 'DISTANCE',
        'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED', 'AIR_SYSTEM_DELAY',
        'SECURITY_DELAY', 'AIRLINE_DELAY', 'LATE_AIRCRAFT_DELAY',
        'WEATHER_DELAY', 'DEPARTURE_HOUR', 'ARRIVAL_HOUR'
    ]
    
    for col_name in numeric_columns:
        if col_name in df_flights.columns:
            df_flights = df_flights.withColumn(col_name, col(col_name).cast(FloatType()))
    
    print("Данные преобразованы / Data transformed")
    df_flights.printSchema()
    
    spark.stop()


def task_4_top_airlines_delay(**context):
    """
    Task 4: Топ-5 авиалиний с наибольшей средней задержкой
    Top-5 airlines with highest average delay
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    df_airlines = spark.read.csv(AIRLINES_FILE, header=True, inferSchema=True)
    
    # Calculate average delay (considering both departure and arrival delays)
    df_delays = df_flights.filter(
        (col("DEPARTURE_DELAY").isNotNull()) & (col("ARRIVAL_DELAY").isNotNull())
    ).groupBy("AIRLINE").agg(
        avg((col("DEPARTURE_DELAY") + col("ARRIVAL_DELAY")) / 2).alias("avg_delay"),
        count("*").alias("flight_count")
    )
    
    # Join with airline names
    df_result = df_delays.join(
        df_airlines,
        df_delays.AIRLINE == df_airlines["IATA CODE"],
        "left"
    ).select(
        df_delays.AIRLINE,
        df_airlines.AIRLINE.alias("airline_name"),
        "avg_delay",
        "flight_count"
    ).orderBy(col("avg_delay").desc()).limit(5)
    
    print("\n=== Топ-5 авиалиний с наибольшей средней задержкой ===")
    print("=== Top-5 airlines with highest average delay ===\n")
    df_result.show(truncate=False)
    
    spark.stop()


def task_5_cancelled_flights_percentage(**context):
    """
    Task 5: Процент отмененных рейсов для каждого аэропорта
    Percentage of cancelled flights for each airport
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    df_airports = spark.read.csv(AIRPORTS_FILE, header=True, inferSchema=True)
    
    # Calculate cancellation percentage by origin airport
    df_cancelled = df_flights.groupBy("ORIGIN_AIRPORT").agg(
        count("*").alias("total_flights"),
        spark_sum(when(col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights")
    ).withColumn(
        "cancellation_percentage",
        (col("cancelled_flights") / col("total_flights") * 100)
    )
    
    # Join with airport names
    df_result = df_cancelled.join(
        df_airports,
        df_cancelled.ORIGIN_AIRPORT == df_airports["IATA CODE"],
        "left"
    ).select(
        df_cancelled.ORIGIN_AIRPORT,
        df_airports.Airport.alias("airport_name"),
        "total_flights",
        "cancelled_flights",
        "cancellation_percentage"
    ).orderBy(col("cancellation_percentage").desc()).limit(20)
    
    print("\n=== Процент отмененных рейсов по аэропортам (топ-20) ===")
    print("=== Cancelled flights percentage by airport (top-20) ===\n")
    df_result.show(truncate=False)
    
    spark.stop()


def task_6_delays_by_time_of_day(**context):
    """
    Task 6: Время суток с наибольшими задержками
    Time of day most associated with delays
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    
    # Define time of day function
    df_with_time = df_flights.withColumn(
        "time_of_day",
        when((col("DEPARTURE_HOUR") >= 6) & (col("DEPARTURE_HOUR") < 12), "Утро/Morning")
        .when((col("DEPARTURE_HOUR") >= 12) & (col("DEPARTURE_HOUR") < 18), "День/Day")
        .when((col("DEPARTURE_HOUR") >= 18) & (col("DEPARTURE_HOUR") < 24), "Вечер/Evening")
        .otherwise("Ночь/Night")
    )
    
    # Calculate delays by time of day
    df_time_delays = df_with_time.filter(
        col("DEPARTURE_DELAY").isNotNull()
    ).groupBy("time_of_day").agg(
        avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
        count("*").alias("flight_count"),
        spark_sum(when(col("DEPARTURE_DELAY") > 0, 1).otherwise(0)).alias("delayed_flights")
    ).withColumn(
        "delay_percentage",
        (col("delayed_flights") / col("flight_count") * 100)
    ).orderBy(col("avg_departure_delay").desc())
    
    print("\n=== Задержки по времени суток ===")
    print("=== Delays by time of day ===\n")
    df_time_delays.show(truncate=False)
    
    spark.stop()


def task_7_add_new_columns(**context):
    """
    Task 7: Добавление новых столбцов IS_LONG_HAUL и DAY_PART
    Add new columns: IS_LONG_HAUL and DAY_PART
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    
    # Add IS_LONG_HAUL column (distance > 1000 miles)
    df_enriched = df_flights.withColumn(
        "IS_LONG_HAUL",
        when(col("DISTANCE") > 1000, True).otherwise(False)
    )
    
    # Add DAY_PART column based on departure hour
    df_enriched = df_enriched.withColumn(
        "DAY_PART",
        when((col("DEPARTURE_HOUR") >= 6) & (col("DEPARTURE_HOUR") < 12), "Morning")
        .when((col("DEPARTURE_HOUR") >= 12) & (col("DEPARTURE_HOUR") < 18), "Day")
        .when((col("DEPARTURE_HOUR") >= 18) & (col("DEPARTURE_HOUR") < 24), "Evening")
        .otherwise("Night")
    )
    
    print("\n=== Данные с новыми столбцами ===")
    print("=== Data with new columns ===\n")
    df_enriched.select(
        "DATE", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
        "DISTANCE", "IS_LONG_HAUL", "DEPARTURE_HOUR", "DAY_PART"
    ).show(20)
    
    # Show statistics
    print("\nСтатистика дальнемагистральных рейсов / Long-haul flights statistics:")
    df_enriched.groupBy("IS_LONG_HAUL").count().show()
    
    print("\nРаспределение по времени суток / Distribution by time of day:")
    df_enriched.groupBy("DAY_PART").count().orderBy("DAY_PART").show()
    
    # Save enriched dataframe path for next task
    context['ti'].xcom_push(key='enriched_data_ready', value=True)
    
    spark.stop()


def task_9_load_to_postgres(**context):
    """
    Tasks 9-10: Подключение к PostgreSQL и загрузка 10,000 строк
    Connect to PostgreSQL and load 10,000 rows
    """
    spark = create_spark_session()
    
    df_flights = read_flights(spark)
    
    # Transform and enrich data
    df_enriched = df_flights.withColumn(
        "DATE", to_date(col("DATE"), "yyyy-MM-dd")
    ).withColumn(
        "IS_LONG_HAUL",
        when(col("DISTANCE") > 1000, True).otherwise(False)
    ).withColumn(
        "DAY_PART",
        when((col("DEPARTURE_HOUR") >= 6) & (col("DEPARTURE_HOUR") < 12), "Morning")
        .when((col("DEPARTURE_HOUR") >= 12) & (col("DEPARTURE_HOUR") < 18), "Day")
        .when((col("DEPARTURE_HOUR") >= 18) & (col("DEPARTURE_HOUR") < 24), "Evening")
        .otherwise("Night")
    )
    
    # Select and rename columns to match PostgreSQL schema
    df_to_load = df_enriched.select(
        col("DATE").alias("date"),
        col("DAY_OF_WEEK").cast(IntegerType()).alias("day_of_week"),
        col("AIRLINE").alias("airline"),
        col("FLIGHT_NUMBER").cast(IntegerType()).alias("flight_number"),
        col("TAIL_NUMBER").alias("tail_number"),
        col("ORIGIN_AIRPORT").alias("origin_airport"),
        col("DESTINATION_AIRPORT").alias("destination_airport"),
        col("DEPARTURE_DELAY").cast(FloatType()).alias("departure_delay"),
        col("DISTANCE").cast(FloatType()).alias("distance"),
        col("ARRIVAL_DELAY").cast(FloatType()).alias("arrival_delay"),
        col("DIVERTED").cast(IntegerType()).alias("diverted"),
        col("CANCELLED").cast(IntegerType()).alias("cancelled"),
        col("CANCELLATION_REASON").alias("cancellation_reason"),
        col("AIR_SYSTEM_DELAY").cast(FloatType()).alias("air_system_delay"),
        col("SECURITY_DELAY").cast(FloatType()).alias("security_delay"),
        col("AIRLINE_DELAY").cast(FloatType()).alias("airline_delay"),
        col("LATE_AIRCRAFT_DELAY").cast(FloatType()).alias("late_aircraft_delay"),
        col("WEATHER_DELAY").cast(FloatType()).alias("weather_delay"),
        col("DEPARTURE_HOUR").cast(IntegerType()).alias("departure_hour"),
        col("ARRIVAL_HOUR").cast(IntegerType()).alias("arrival_hour"),
        col("IS_LONG_HAUL").alias("is_long_haul"),
        col("DAY_PART").alias("day_part")
    ).limit(10000)
    
    # Load to PostgreSQL
    print(f"\nЗагрузка 10,000 строк в PostgreSQL...")
    print(f"Loading 10,000 rows to PostgreSQL...")
    
    df_to_load.write \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", "flights") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print("Данные успешно загружены в PostgreSQL!")
    print("Data successfully loaded to PostgreSQL!")
    
    spark.stop()


def task_11_query_postgres(**context):
    """
    Task 11: SQL запрос - компания и общее время полетов
    SQL query: company and total flight time
    """
    spark = create_spark_session()
    
    # Read data from PostgreSQL
    print("\nЧитаем данные из PostgreSQL...")
    print("Reading data from PostgreSQL...")
    
    df_from_postgres = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", "flights") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Register as temp view for SQL queries
    df_from_postgres.createOrReplaceTempView("flights")
    
    # SQL query: Calculate total flight time per airline
    # Flight time = distance / average_speed (assuming average speed ~500 mph)
    query = """
    SELECT 
        airline,
        COUNT(*) as total_flights,
        SUM(distance) as total_distance_miles,
        ROUND(SUM(distance / 500.0), 2) as total_flight_time_hours
    FROM flights
    WHERE distance IS NOT NULL
    GROUP BY airline
    ORDER BY total_flight_time_hours DESC
    """
    
    result = spark.sql(query)
    
    print("\n=== Компании и общее время полетов (в часах) ===")
    print("=== Airlines and total flight time (in hours) ===\n")
    result.show(20, truncate=False)
    
    # Also join with airline names from CSV for better readability
    df_airlines = spark.read.csv(AIRLINES_FILE, header=True, inferSchema=True)
    
    result_with_names = result.join(
        df_airlines,
        result.airline == df_airlines["IATA CODE"],
        "left"
    ).select(
        result.airline,
        df_airlines.AIRLINE.alias("airline_name"),
        "total_flights",
        "total_distance_miles",
        "total_flight_time_hours"
    ).orderBy(col("total_flight_time_hours").desc())
    
    print("\n=== С названиями компаний / With airline names ===\n")
    result_with_names.show(20, truncate=False)
    
    spark.stop()


# Create DAG
with DAG(
    'flight_data_analysis_pyspark',
    default_args=default_args,
    description='Анализ данных о полетах с использованием PySpark / Flight Data Analysis using PySpark',
    schedule_interval=None,
    catchup=False,
    tags=['pyspark', 'flights', 'analysis', 'postgresql'],
) as dag:
    
    # Task 1: Load data
    load_data = PythonOperator(
        task_id='task_1_load_data',
        python_callable=task_1_load_data,
    )
    
    # Task 2: Verify data
    verify_data = PythonOperator(
        task_id='task_2_verify_data',
        python_callable=task_2_verify_data,
    )
    
    # Task 3: Transform data types
    transform_data = PythonOperator(
        task_id='task_3_transform_data',
        python_callable=task_3_transform_data,
    )
    
    # Task 4: Top-5 airlines with highest delay
    top_airlines_delay = PythonOperator(
        task_id='task_4_top_airlines_delay',
        python_callable=task_4_top_airlines_delay,
    )
    
    # Task 5: Cancelled flights percentage
    cancelled_flights = PythonOperator(
        task_id='task_5_cancelled_flights_percentage',
        python_callable=task_5_cancelled_flights_percentage,
    )
    
    # Task 6: Delays by time of day
    delays_by_time = PythonOperator(
        task_id='task_6_delays_by_time_of_day',
        python_callable=task_6_delays_by_time_of_day,
    )
    
    # Task 7: Add new columns
    add_new_columns = PythonOperator(
        task_id='task_7_add_new_columns',
        python_callable=task_7_add_new_columns,
    )
    
    # Task 9-10: Load to PostgreSQL
    load_to_postgres = PythonOperator(
        task_id='task_9_10_load_to_postgres',
        python_callable=task_9_load_to_postgres,
    )
    
    # Task 11: Query PostgreSQL
    query_postgres = PythonOperator(
        task_id='task_11_query_postgres',
        python_callable=task_11_query_postgres,
    )
    
    # Define task dependencies
    load_data >> verify_data >> transform_data
    transform_data >> [top_airlines_delay, cancelled_flights, delays_by_time]
    [top_airlines_delay, cancelled_flights, delays_by_time] >> add_new_columns
    add_new_columns >> load_to_postgres >> query_postgres
