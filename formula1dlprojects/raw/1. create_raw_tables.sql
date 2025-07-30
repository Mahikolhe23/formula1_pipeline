-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table using CSV File

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) 
USING CSV
OPTIONS (path "/mnt/formula1dlprojects/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table using CSV file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) 
USING CSV
OPTIONS (path "/mnt/formula1dlprojects/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Constructors table
-- MAGIC   - Single Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/formula1dlprojects/raw/constructors.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Drivers table
-- MAGIC   - Single Line JSON
-- MAGIC   - Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/formula1dlprojects/raw/drivers.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Results table
-- MAGIC   - Single Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING JSON
OPTIONS (path "/mnt/formula1dlprojects/raw/results.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Pitstops table
-- MAGIC   - Multi Line JSON
-- MAGIC   - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS (path "/mnt/formula1dlprojects/raw/pit_stops.json", header true, multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Lap times table
-- MAGIC   - CSV file
-- MAGIC   - Multiple files
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dlprojects/raw/lap_times/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Qualifying table
-- MAGIC   - JSON File
-- MAGIC   - Multiline JSON
-- MAGIC   - Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/formula1dlprojects/raw/qualifying/", multiLine true)