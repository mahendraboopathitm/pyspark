# pyspark
# PySpark Learning Documentation

## 1. Introduction to PySpark

### 1.1 PySpark Overview
PySpark is the **Python API for Apache Spark**, an open-source distributed computing system. Spark allows fast, scalable, and fault-tolerant processing of large datasets. PySpark brings the power of Spark to Python developers, enabling big data analysis, machine learning, and data processing using Python.

**Key Points:**
- Handles **big data efficiently** with in-memory processing.
- Supports **batch and streaming data**.
- Provides high-level APIs like **RDDs, DataFrames, and Datasets**.
- Integrates with **Hadoop, Hive, HDFS, and cloud platforms**.

### 1.2 Introduction to PySpark and Its Role in Big Data Processing
PySpark provides a **bridge between Python and the Spark Engine**, enabling distributed computation without writing Java/Scala code. It is used for:
- **Data preprocessing** for machine learning.
- **Batch processing** of large datasets.
- **Streaming analytics** in real-time.
- Performing **SQL-like queries** on large datasets using DataFrames.
  
### 1.3 Python API for Apache Spark
The Python API (`pyspark`) includes:
- **SparkContext**: The entry point for low-level RDD operations.
- **SparkSession**: Unified entry point for DataFrames and SQL operations.
- **RDD API**: For functional-style distributed data operations.
- **DataFrame API**: For structured data and SQL-like operations.
- **MLlib**: Spark's machine learning library accessible via Python.

---

## 2. Revision on Spark Architecture

### 2.1 Revising the Architecture of Spark
Spark architecture is based on **Master-Slave architecture** with the following components:

- **Driver**: The main program that controls the execution and creates **SparkContext** or **SparkSession**.
- **Cluster Manager**: Allocates resources across applications (e.g., YARN, Mesos, or Standalone Spark Cluster).
- **Worker Nodes**: Execute tasks and store data in memory or disk.
- **Executors**: Run on worker nodes and execute tasks assigned by the driver.
- **Task**: Smallest unit of work sent to executors.
- **Job**: Spark action triggers a job, which is divided into tasks.

**Flow:**
1. Driver receives user code and creates RDD/DataFrame.
2. Driver converts transformations into **DAG (Directed Acyclic Graph)**.
3. DAG scheduler breaks it into stages and tasks.
4. Tasks are executed on worker nodes via executors.

### 2.2 Integration with Spark Components like Driver and Executors
- **Driver**: Coordinates tasks, schedules jobs, and maintains metadata.
- **Executor**: Runs tasks, caches data in memory, and reports results back to the driver.
- **Communication**: Spark uses **network RPC** for coordination between driver and executors.
- **Fault Tolerance**: Lost tasks are re-executed automatically using lineage information.

---

## 3. Revision on Spark Components
- **RDD (Resilient Distributed Dataset)**: Immutable, distributed collection of objects. Can be **transformed** or **acted upon**.
- **DataFrame**: Distributed collection of data organized into **named columns**. Provides **SQL-like operations**.
- **Dataset**: Strongly-typed, distributed collection that combines RDD and DataFrame benefits.
- **Transformations**: Lazy operations that return a new RDD/DataFrame.
- **Actions**: Operations that trigger execution and return results to driver.

---

## 4. SparkSession

### 4.1 Explanation of SparkSession as the Entry Point to PySpark
- `SparkSession` is the **entry point for DataFrame and SQL operations**.
- Replaces the older combination of **SparkContext, SQLContext, and HiveContext**.
- Provides:
  - `.read` API to read data from CSV, JSON, Parquet, Hive, JDBC, etc.
  - `.sql()` API to run SQL queries on DataFrames.
  - `.createDataFrame()` to convert RDDs or lists into DataFrames.

### 4.2 Configuring and Creating a SparkSession
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()
```
# 5.PySpark DataFrame API Documentation

## Overview
A **DataFrame** in PySpark is a **distributed collection of data organized into named columns**. It is similar to a table in a relational database or a Pandas DataFrame, but designed for **big data and distributed computation**.  

**Key Features:**
- Supports **SQL-like operations** (filtering, grouping, joining, aggregations).
- **Lazy execution**: transformations are only computed when an action is called.
- Can be created from **RDDs, lists, CSV/JSON files, Parquet files, and external databases**.
- Provides **high-level APIs** for structured data.

---

## Creating a DataFrame

### 1. From a Python List
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameDemo").getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)
df.show()
```
## DataFrame Operations

###  Viewing Data

df.show(n): Displays the first n rows.

df.printSchema(): Shows the schema (columns and data types).

df.columns: Lists column names.

## Selecting Columns
```python
df.select("name").show()
df.select("name", "age").show()
```
##  Filtering Rows
```python
from pyspark.sql.functions import col

df.filter(col("age") > 30).show()
```
##  Adding Columns
```python
df_with_new = df.withColumn("age_plus_5", col("age") + 5)
df_with_new.show()
```
##  Dropping Columns
```python
df.drop("age_plus_5").show()
```
##  Grouping and Aggregation
```python
from pyspark.sql.functions import sum, avg

df_sales = spark.createDataFrame(
    [("Alice", 100), ("Bob", 200), ("Alice", 300)],
    ["name", "sales"]
)
```
# Total sales per name
```python
df_grouped = df_sales.groupBy("name").agg(sum("sales").alias("total_sales"))
df_grouped.show()
```
# Average sales per name
```python
df_avg = df_sales.groupBy("name").agg(avg("sales").alias("avg_sales"))
df_avg.show()
```
##  Sorting
```python
df_ordered = df_sales.orderBy(col("sales").desc())
df_ordered.show()
```
##  Limiting Rows
```python
df.limit(2).show()
```
##  Collecting Data to Driver
```python
rows = df.collect()
for row in rows:
    print(row)
```
