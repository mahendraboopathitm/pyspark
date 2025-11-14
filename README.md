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

## Transformations and Actions

### Understanding Transformations and Actions in PySpark
- **Transformations**: Operations on RDDs or DataFrames that produce a new RDD or DataFrame. They are **lazy**, meaning they are not executed immediately but only when an action is called.
- **Actions**: Operations that trigger execution and return a result to the driver program or write data to external storage.

###  Examples of Common Transformations and Actions
**Transformations**:
- `map()`: Applies a function to each element.
- `filter()`: Filters elements based on a condition.
- `flatMap()`: Maps and flattens the result.
- `distinct()`: Returns distinct elements.
- `union()`: Combines two RDDs.
- `join()`: Joins two RDDs by key.

**Actions**:
- `collect()`: Returns all elements to the driver.
- `count()`: Counts the number of elements.
- `take(n)`: Returns the first `n` elements.
- `reduce()`: Reduces elements using a function.
- `saveAsTextFile()`: Saves RDD to an external storage.

---

## Revision on Spark RDDs (Resilient Distributed Datasets)

### Overview of RDDs in PySpark
- RDD is the fundamental data structure in Spark, representing an **immutable distributed collection of objects**.
- Key features:
  - Fault-tolerant
  - Lazy evaluated
  - Can be cached or persisted
  - Supports **parallel operations**.

###  Differences Between RDDs and DataFrames
| Feature            | RDD                             | DataFrame                        |
|-------------------|---------------------------------|---------------------------------|
| Type               | Low-level API                   | High-level API                  |
| Schema             | No schema, only raw objects     | Schema-based, tabular           |
| Performance        | Slower due to lack of optimization | Faster due to Catalyst optimizer |
| Ease of Use        | Complex                         | Easy, SQL-like syntax           |

---

## PySpark Data Structures
- **RDD**: Distributed collection of objects.
- **DataFrame**: Distributed collection of data organized into named columns.
- **Dataset**: Type-safe, object-oriented extension of DataFrame (mainly in Scala/Java).

---

## SparkContext

###  The Role of SparkContext in PySpark Applications
- The entry point to Spark functionality.
- Establishes a connection to a Spark cluster.
- Manages resources, configuration, and job execution.
- Every Spark application needs a `SparkContext`.

### Creating and Configuring SparkContext
```python
from pyspark import SparkContext

# Create SparkContext
sc = SparkContext(appName="MyPySparkApp")

# Set configuration (optional)
sc.setLogLevel("WARN")
```
## PySpark DataFrames
### Introduction to PySpark DataFrames

DataFrame is a distributed collection of data organized into named columns.

Supports SQL queries, transformations, and actions.

## Operations on DataFrames

Filtering: df.filter(df['age'] > 25)

Selecting: df.select('name', 'salary')

Aggregating: df.groupBy('department').agg({'salary':'avg'})

Adding Columns: df.withColumn('new_col', df['salary']*2)

## PySpark SQL
### Integration of SQL Queries with PySpark

PySpark allows SQL queries on DataFrames.

Provides flexibility to use familiar SQL syntax.

### Registering DataFrames as Temporary SQL Tables
### Register DataFrame as temp table
```pyspark
df.createOrReplaceTempView("employees")
````
# Execute SQL query
```pyspark
result = spark.sql("SELECT name, salary FROM employees WHERE age > 25")
result.show()
```

Temporary views are session-scoped.

Useful for complex SQL queries on DataFrames.


#  Persist and Cache in PySpark

##  Overview

In PySpark, transformations are **lazy** — meaning data isn’t computed until an **action** (like `count()` or `show()`) is called.  
If you reuse the same RDD or DataFrame multiple times, Spark will **recompute** it each time, which can be expensive.  

To **avoid recomputation** and **improve performance**, you can **cache** or **persist** the dataset in memory (and optionally on disk).

---

##  `cache()` — Store in Memory Only

###  Definition:
`cache()` stores the RDD or DataFrame **in memory only** (`MEMORY_ONLY` storage level).

###  Syntax:
```python
df.cache()
```

###  Example:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CacheExample").getOrCreate()

data = [
    (1, "Alice", 4000),
    (2, "Bob", 5000),
    (3, "Charlie", 4500),
    (4, "David", 7000)
]
columns = ["ID", "Name", "Salary"]

df = spark.createDataFrame(data, columns)

# Cache the DataFrame
df.cache()

# Trigger an action to store data in memory
df.show()

# Reuse it (no recomputation now)
high_salary = df.filter(col("Salary") > 4500)
high_salary.show()
```

###  Notes:
- The first action (`show()`) will **compute** and **cache** the DataFrame.
- The next actions will read it directly from memory, improving performance.

---

##  `persist()` — Store with a Custom Storage Level

###  Definition:
`persist()` allows you to **choose where and how** to store your dataset.

###  Syntax:
```python
df.persist(storageLevel)
```

###  Storage Levels:
| Storage Level | Description |
|----------------|--------------|
| `MEMORY_ONLY` | Default for `cache()` — keeps data in memory only. |
| `MEMORY_AND_DISK` | Keeps data in memory; spills to disk if not enough memory. |
| `DISK_ONLY` | Stores data only on disk. |
| `MEMORY_ONLY_SER` | Stores serialized objects in memory (saves space). |
| `MEMORY_AND_DISK_SER` | Serialized and spills to disk if needed. |
| `OFF_HEAP` | Stores data in off-heap memory (requires configuration). |

### Example:
```python
from pyspark import StorageLevel

# Persist DataFrame with MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger caching
df.count()

# Reuse without recomputation
df.filter(col("Salary") > 4500).show()
```

---

##  Difference Between `cache()` and `persist()`

| Feature | `cache()` | `persist()` |
|----------|------------|-------------|
| Storage level | MEMORY_ONLY | Customizable (e.g., MEMORY_AND_DISK, DISK_ONLY, etc.) |
| Flexibility | Less | More |
| Usage | Simple and quick caching | When data is large or memory is limited |
| Example | `df.cache()` | `df.persist(StorageLevel.MEMORY_AND_DISK)` |

---

##  Removing Cached/Persisted Data

You can uncache or remove persisted data to free up memory:

```python
df.unpersist()
```

---

##  When to Use

 **Use `cache()`** when:
- Dataset fits easily in memory.
- It is reused multiple times in your job.

 **Use `persist()`** when:
- Dataset is **large** or **partially reused**.
- You need a **custom storage level** (e.g., `MEMORY_AND_DISK`).

---

## Performance Tip

Always perform a **lazy action** (like `count()`, `show()`, or `collect()`) after calling `cache()` or `persist()` to **materialize** the dataset — otherwise, it won’t actually be stored yet.

```python
df.cache()
df.count()   # triggers computation and stores data
```

---

##  Example Summary Code

```python
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CachePersistExample").getOrCreate()

data = [
    (101, "Alice", "IT", 50000),
    (102, "Bob", "HR", 40000),
    (103, "Charlie", "Finance", 55000),
    (104, "David", "IT", 60000)
]
columns = ["ID", "Name", "Dept", "Salary"]

df = spark.createDataFrame(data, columns)

# Cache example
df.cache()
df.count()

# Persist example
df.persist(StorageLevel.MEMORY_AND_DISK)
df.show()

# Unpersist
df.unpersist()
```

---

# PySpark DataFrame Practice — General & String Functions
## Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, min, max
from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, substring, substring_index, split, repeat, rpad, lpad, regexp_replace, regexp_extract, length, instr, initcap

spark = SparkSession.builder.appName("func").getOrCreate()

# Dataset 1 — General DataFrame Functions
data = [
    (101, "Alice", "HR", 40000, 29),
    (102, "Bob", "IT", 55000, 34),
    (103, "Charlie", "Finance", 48000, 28),
    (104, "David", "IT", 61000, 41),
    (105, "Eve", "HR", 39000, 25),
    (106, "Frank", "Finance", 53000, 38)
]

rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(["id", "name", "dept", "sal", "age"])
```
# General DataFrame Functions
## show()

### Displays the entire DataFrame.
```
df.show()
```

Output:
```
+---+-------+-------+-----+---+
|id |name   |dept   |sal  |age|
+---+-------+-------+-----+---+
|101|Alice  |HR     |40000|29 |
|102|Bob    |IT     |55000|34 |
|103|Charlie|Finance|48000|28 |
|104|David  |IT     |61000|41 |
|105|Eve    |HR     |39000|25 |
|106|Frank  |Finance|53000|38 |
+---+-------+-------+-----+---+
```
## collect()

### Returns all rows as a list of Row objects.
```
df.collect()

```
Output:
```
[Row(id=101, name='Alice', dept='HR', sal=40000, age=29), ...]
```
### take(n)

### Takes the first n rows.
```
df.take(2)
```

Output:
```
[Row(id=101, name='Alice', dept='HR', sal=40000, age=29),
 Row(id=102, name='Bob', dept='IT', sal=55000, age=34)]
```
### printSchema()

### Displays column structure.
```
df.printSchema()
```

Output:
```
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- sal: long (nullable = true)
 |-- age: long (nullable = true)
```
## count()

### Counts total rows.
```
df.count()

```
### Output: 6

## select()

### Selects specific columns.
```
df.select("id", "name").show()
```
### filter() / where()

### Filters based on conditions.
```
df.filter(col("age") > 30).show()
df.where(col("dept") == "IT").show()
```
### between()

### Selects rows between a range.
```
df.filter(col("sal").between(40000, 55000)).show()
```
### sort()

### Sorts data in ascending/descending order.
```
df.sort(col("sal").desc()).show()
```
### describe()

### Gives summary stats.
```
df.describe().show()
```
### columns

### Shows column names.
```
df.columns
```

Output:
```
['id', 'name', 'dept', 'sal', 'age']
```
# Dataset 2 — String Functions
```
data = [
    (1, "  alice  ", "HR,Admin", "Alice123"),
    (2, "bob", "IT,Support", "B@b_2025"),
    (3, "  CHARLIE", "Finance,Audit", "Ch@rlie#99"),
    (4, "DAVID  ", "IT,Security", "David!!"),
    (5, "eve", "HR,Training", "EvE2024")
]
cols = ["ID", "Name", "Departments", "Username"]
df = spark.createDataFrame(data, cols)
```

### upper()

Purpose: Converts all characters in a string to uppercase.
```
df.select(upper(col("Name")).alias("Upper_Name")).show()
```

Output:
```
Upper_Name
ALICE
BOB
CHARLIE
DAVID
EVE
```
### trim()

Purpose: Removes leading and trailing spaces.
```
df.select(trim(col("Name")).alias("Trimmed_Name")).show()

```
Output:
```
Trimmed_Name
alice
bob
CHARLIE
DAVID
eve`
```
### ltrim()

Purpose: Removes leading (left) spaces.
```
df.select(ltrim(col("Name")).alias("LTrim_Name")).show()
```

Output:
```
LTrim_Name
alice
bob
CHARLIE
DAVID
eve
```
### rtrim()

Purpose: Removes trailing (right) spaces.
```
df.select(rtrim(col("Name")).alias("RTrim_Name")).show()
```

Output:
```
RTrim_Name
alice
bob
CHARLIE
DAVID
eve
```
### substring_index()

Purpose: Returns substring before a specific delimiter.
```
df.select(substring_index(col("Departments"), ",", 1).alias("Main_Dept")).show()

```
Output:
```
Main_Dept
HR
IT
Finance
IT
HR
```
### substring()

Purpose: Extracts part of a string based on position.
```
df.select(substring(col("Username"), 1, 5).alias("Sub_Username")).show()
```

Output:
```
Sub_Username
Alice
B@b_2
Ch@rl
David
EvE20
```
### split()

Purpose: Splits a string based on a delimiter.
```
df.select(split(col("Departments"), ",").alias("Split_Dept")).show(truncate=False)

```
Output:
```
Split_Dept
[HR, Admin]
[IT, Support]
[Finance, Audit]
[IT, Security]
[HR, Training]
```
### repeat()

Purpose: Repeats a string a specified number of times.
```
df.select(repeat(trim(col("Name")), 2).alias("Repeat_Name")).show()

```
Output:
```
Repeat_Name
alicealice
bobbob
CHARLIECHARLIE
DAVIDDAVID
eveeve
```
### rpad()

Purpose: Pads the string to the right with a specific character.
```
df.select(rpad(trim(col("Name")), 10, "*").alias("Rpad_Name")).show()
```

Output:
```
Rpad_Name
alice*****
bob*******
CHARLIE***
DAVID*****
eve*******
```
### lpad()

Purpose: Pads the string to the left with a specific character.
```
df.select(lpad(trim(col("Name")), 10, "#").alias("Lpad_Name")).show()
```

Output:
```
Lpad_Name
#####alice
#######bob
###CHARLIE
#####DAVID
#######eve
```
### regexp_replace()

Purpose: Replaces matching substrings using a regular expression.
```
df.select(regexp_replace(col("Username"), "[^a-zA-Z]", "").alias("Clean_Username")).show()

```
Output:
```
Clean_Username
Alice
Bb
Chrlie
David
EvE
```
### lower()

Purpose: Converts string to lowercase.
```
df.select(lower(col("Departments")).alias("Lower_Dept")).show()

```
Output:
```
Lower_Dept
hr,admin
it,support
finance,audit
it,security
hr,training
```
### regexp_extract()

Purpose: Extracts substring using a regular expression.
```
df.select(regexp_extract(col("Username"), r"[A-Za-z]+", 0).alias("Extracted_Text")).show()
```

Output:
```
Extracted_Text
Alice
B
Ch
David
EvE
```
### length()

Purpose: Returns length of the string.
```
df.select(length(trim(col("Name"))).alias("Name_Length")).show()

```
Output:
```
Name_Length
5
3
7
5
3
```
### instr()

Purpose: Returns the position of substring.
```
df.select(instr(col("Departments"), "IT").alias("IT_Position")).show()

```
Output:
```
IT_Position
0
1
0
1
0
```
### initcap()

Purpose: Converts the first letter of each word to uppercase.
````
df.select(initcap(lower(col("Departments"))).alias("Initcap_Dept")).show()
````

Output:
```
Initcap_Dept
Hr,Admin
It,Support
Finance,Audit
It,Security
Hr,Training
```
# PySpark Functions Documentation day-4

This documentation covers various PySpark aggregate and date/time functions with examples and expected outputs.

## 1. Numeric Functions

### 1.1 SUM()

```python
from pyspark.sql.functions import sum

# Sum of salaries
df.agg(sum("sal").alias("total_sal")).show()
```

**Output:**

```
+---------+
|total_sal|
+---------+
| 1868000 |
+---------+
```

### 1.2 AVG()

```python
from pyspark.sql.functions import avg, round

# Average salary
df.agg(round(avg("sal"),2).alias("avg_sal")).show()
```

**Output:**

```
+-------+
|avg_sal|
+-------+
|62300.0|
+-------+
```

### 1.3 MIN()

```python
from pyspark.sql.functions import min

# Minimum salary
df.agg(min("sal").alias("min_sal")).show()
```

**Output:**

```
+-------+
|min_sal|
+-------+
|45000  |
+-------+
```

### 1.4 MAX()

```python
from pyspark.sql.functions import max

# Maximum salary
df.agg(max("sal").alias("max_sal")).show()
```

**Output:**

```
+-------+
|max_sal|
+-------+
|80000  |
+-------+
```

### 1.5 ROUND()

```python
from pyspark.sql.functions import round, avg

# Rounded average salary
df.agg(round(avg("sal"),2).alias("avg_sal_rounded")).show()
```

**Output:**

```
+---------------+
|avg_sal_rounded|
+---------------+
|62300.0        |
+---------------+
```

### 1.6 ABS()

```python
from pyspark.sql.functions import abs, col

# Absolute difference example
df1 = df.withColumn("diff", abs(col("sal") - 60000))
df1.select("Name", "diff").show(5)
```

**Output:**

```
+-----+-----+
| Name| diff|
+-----+-----+
|Aarav| 8000|
|Isha |1000 |
|Rohan| 0   |
|Meera|15000|
|Dev  |15000|
+-----+-----+
```

## 2. Date and Time Functions

### 2.1 CURRENT_DATE()

```python
from pyspark.sql.functions import current_date

# Add current date
df.select("Name", current_date().alias("today")).show(5)
```

**Output:**

```
+-----+----------+
|Name | today    |
+-----+----------+
|Aarav|2025-11-10|
|Isha |2025-11-10|
|Rohan|2025-11-10|
|Meera|2025-11-10|
|Dev  |2025-11-10|
+-----+----------+
```

### 2.2 CURRENT_TIMESTAMP()

```python
from pyspark.sql.functions import current_timestamp

# Add current timestamp
df.select("Name", current_timestamp().alias("timestamp")).show(5)
```

**Output:**

```
+-----+-----------------------+
|Name | timestamp             |
+-----+-----------------------+
|Aarav|2025-11-10 15:30:12.0 |
|Isha |2025-11-10 15:30:12.0 |
|Rohan|2025-11-10 15:30:12.0 |
|Meera|2025-11-10 15:30:12.0 |
|Dev  |2025-11-10 15:30:12.0 |
+-----+-----------------------+
```

### 2.3 DATE_ADD()

```python
from pyspark.sql.functions import date_add

# Add 30 days to joining date
df.select("Name", date_add("Joining_Timestamp", 30).alias("after_30_days")).show(5)
```

**Output:**

```
+-----+------------+
|Name | after_30_days|
+-----+------------+
|Aarav|2021-06-09  |
|Isha |2019-09-13  |
|Rohan|2020-01-24  |
|Meera|2022-03-31  |
|Dev  |2023-11-11  |
+-----+------------+
```

### 2.4 DATEDIFF()

```python
from pyspark.sql.functions import datediff, current_date

# Difference in days from current date
df = df.withColumn("Record_Created_Date", current_date())
df1 = df.select("Name", datediff("Joining_Timestamp", "Record_Created_Date").alias("date_difference"))
df1 = df1.withColumn("date_difference", abs(col("date_difference")))
df1.show(5)
```

**Output:**

```
+-----+---------------+
|Name | date_difference|
+-----+---------------+
|Aarav| 1645          |
|Isha | 2280          |
|Rohan| 1781          |
|Meera| 1350          |
|Dev  | 760           |
+-----+---------------+
```

### 2.5 YEAR(), 2.6 MONTH(), 2.7 DAY()

```python
from pyspark.sql.functions import year, month, dayofmonth

df.select("Name", year("Joining_Timestamp").alias("year"),
          month("Joining_Timestamp").alias("month"),
          dayofmonth("Joining_Timestamp").alias("day")).show(5)
```

**Output:**

```
+-----+----+-----+---+
|Name |year|month|day|
+-----+----+-----+---+
|Aarav|2021|5    |10 |
|Isha |2019|8    |14 |
|Rohan|2020|12   |25 |
|Meera|2022|3    |1  |
|Dev  |2023|10   |12 |
+-----+----+-----+---+
```

### 2.8 TO_DATE() and 2.9 DATE_FORMAT()

```python
from pyspark.sql.functions import to_date, date_format

# Convert string to date
df.select("Name", to_date("Joining_Timestamp").alias("date_only")).show(5)

# Format date
df.select("Name", date_format("Joining_Timestamp", "dd-MM-yyyy").alias("change_format")).show(5)
```

**Output:**

```
+-----+------------+
|Name | change_format|
+-----+------------+
|Aarav|10-05-2021  |
|Isha |14-08-2019  |
|Rohan|25-12-2020  |
|Meera|01-03-2022  |
|Dev  |12-10-2023  |
+-----+------------+
```

## 3. Aggregate Functions

### 3.1 mean(), 3.2 avg()

```python
from pyspark.sql.functions import mean, avg, round

# Mean / Avg
df.agg(round(mean("sal"),2).alias("mean_sal"), round(avg("bonus"),2).alias("avg_bonus")).show()
```

**Output:**

```
+---------+---------+
|mean_sal |avg_bonus|
+---------+---------+
|62300.0  |3376.67  |
+---------+---------+
```

### 3.3 collect_list()

```python
df.groupBy("dept").agg(collect_list("Name").alias("dept_vise_names")).show()
```

**Output:**

```
+-------+--------------------+
|dept   |dept_vise_names     |
+-------+--------------------+
|IT     |[Isha, Dev, Tara...]|
|Finance|[Rohan, Sneha, Vik...]|
|Sales  |[Aarav, Kiran, Neh...]|
|HR     |[Meera, Rahul, Nikh...]|
+-------+--------------------+
```

### 3.4 collect_set()

```python
df.groupBy("dept").agg(collect_set("reg").alias("dept_vise_reg")).show()
```

**Output:**

```
+-------+----------+
|dept   |dept_vise_reg|
+-------+----------+
|IT     |[West, North]|
|Finance|[South]      |
|Sales  |[East]       |
|HR     |[North]      |
+-------+----------+
```

### 3.5 countDistinct()

```python
df.groupBy("dept").agg(count_distinct("Name").alias("dept_vise_name_count")).show()
```

**Output:**

```
+-------+------------------+
|dept   |dept_vise_name_count|
+-------+------------------+
|IT     |6                 |
|Finance|8                 |
|Sales  |7                 |
|HR     |5                 |
+-------+------------------+
```

### 3.6 count()

```python
df.groupBy("dept").agg(count("Name").alias("dept_vise_count")).show()
```

**Output:**

```
+-------+----------------+
|dept   |dept_vise_count|
+-------+----------------+
|IT     |6               |
|Finance|8               |
|Sales  |7               |
|HR     |5               |
+-------+----------------+
```

### 3.7 first(), 3.8 last()

```python
df1=df.groupBy("dept").agg(first("Name").alias("dept_vise_firstname"), last("Name").alias("dept_vise_lastname"))
df2=df.join(df1,on="dept").filter((df["Name"]==df1["dept_vise_firstname"]) | (df["Name"]==df1["dept_vise_lastname"]))
df2.show()
```

**Output:**

```
+-----+-------+-----+-----+-----+---+---+
|id   |Name   |dept |sal  |bonus|reg|exp|
+-----+-------+-----+-----+-----+---+---+
|1    |Aarav  |Sales|52000|4000 |East|5  |
|16   |Aditi  |Sales|54000|2700 |East|5  |
|2    |Isha   |IT   |70000|5000 |West|7  |
|29   |Deepa  |IT   |74000|3400 |West|8  |
|3    |Rohan  |Finance|60000|3000|South|6|
|30   |Varun  |Finance|65000|2500|South|6|
|4    |Meera  |HR   |45000|2000 |North|3  |
|19   |Nikhil |HR   |47000|1800 |North|3  |
+-----+-------+-----+-----+-----+---+---+
```

### 3.9 max(), 3.10 min(), 3.11 sum()

```python
df.agg(max("sal").alias("max_sal"), min("sal").alias("min_sal"), sum("sal").alias("total_sal")).show()
```

**Output:**

```
+-------+-------+---------+
|max_sal|min_sal|total_sal|
+-------+-------+---------+
|80000  |45000  |1868000  |
+-------+-------+---------+
```
# day 5 documentation

## PySpark Practice – Joins, Functions & Windows
### Dataset Creation
```PYTHON
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("practice").getOrCreate()

data_emp = [
    (1, "Aarav", 101, 45000.75),
    (2, "Isha", 102, 60000.20),
    (3, "Rohan", 101, 52000.60),
    (4, "Meera", 103, 48000.40),
    (5, "Arjun", 104, 75000.00),
    (6, "Priya", 101, 50000.00),
    (7, "Kiran", 102, 65000.90),
    (8, "Diya", 105, 40000.50),
    (9, "Manav", 106, 39000.25),
    (10, "Sneha", 107, 47000.00),
    (11, "Varun", 103, 58000.35),
    (12, "Asha", 104, 72000.10),
    (13, "Vivek", 105, 51000.00),
    (14, "Riya", 106, 43000.70),
    (15, "Neel", 108, 62000.55)
]

data_dept = [
    (101, "HR", "Mumbai"),
    (102, "IT", "Bangalore"),
    (103, "Finance", "Pune"),
    (104, "Marketing", "Delhi"),
    (105, "Operations", "Chennai"),
    (106, "Admin", "Hyderabad")
]

columns_emp = ["emp_id", "name", "dept_id", "salary"]
columns_dept = ["dept_id", "dept_name", "location"]

dF= spark.createDataFrame(data_emp, columns_emp)
df1 = spark.createDataFrame(data_dept, columns_dept)
```
# 1. JOINS
## 1.1 INNER JOIN
```
df.join(df1, "dept_id", "inner").show()


Output:

+-------+------+-------+----------+----------+
|dept_id|emp_id|name   |salary    |dept_name |
+-------+------+-------+----------+----------+
|101    |1     |Aarav  |45000.75  |HR        |
|101    |3     |Rohan  |52000.6   |HR        |
|101    |6     |Priya  |50000.0   |HR        |
|102    |2     |Isha   |60000.2   |IT        |
|102    |7     |Kiran  |65000.9   |IT        |
|103    |4     |Meera  |48000.4   |Finance   |
|103    |11    |Varun  |58000.35  |Finance   |
|104    |5     |Arjun  |75000.0   |Marketing |
|104    |12    |Asha   |72000.1   |Marketing |
|105    |8     |Diya   |40000.5   |Operations|
|105    |13    |Vivek  |51000.0   |Operations|
|106    |9     |Manav  |39000.25  |Admin     |
|106    |14    |Riya   |43000.7   |Admin     |
+-------+------+-------+----------+----------+

```
### Explanation:
Only employees whose dept_id matches a department appear here. Missing departments (107, 108) are excluded.

# 1.2 LEFT JOIN
```
df.join(df1, "dept_id", "left").orderBy("emp_id").show()


Output:

+-------+------+-------+----------+----------+
|dept_id|emp_id|name   |salary    |dept_name |
+-------+------+-------+----------+----------+
|101    |1     |Aarav  |45000.75  |HR        |
|102    |2     |Isha   |60000.2   |IT        |
|101    |3     |Rohan  |52000.6   |HR        |
|103    |4     |Meera  |48000.4   |Finance   |
|104    |5     |Arjun  |75000.0   |Marketing |
|101    |6     |Priya  |50000.0   |HR        |
|102    |7     |Kiran  |65000.9   |IT        |
|105    |8     |Diya   |40000.5   |Operations|
|106    |9     |Manav  |39000.25  |Admin     |
|107    |10    |Sneha  |47000.0   |null      |
|103    |11    |Varun  |58000.35  |Finance   |
|104    |12    |Asha   |72000.1   |Marketing |
|105    |13    |Vivek  |51000.0   |Operations|
|106    |14    |Riya   |43000.7   |Admin     |
|108    |15    |Neel   |62000.55  |null      |
+-------+------+-------+----------+----------+

```
### Explanation:
All employees appear. Dept columns are null where no matching dept (107, 108).

# 1.3 RIGHT JOIN
```python
df.join(df1, "dept_id", "right").show()


Output:

+-------+------+-------+----------+----------+
|dept_id|emp_id|name   |salary    |dept_name |
+-------+------+-------+----------+----------+
|101    |1     |Aarav  |45000.75  |HR        |
|102    |2     |Isha   |60000.2   |IT        |
|103    |4     |Meera  |48000.4   |Finance   |
|104    |5     |Arjun  |75000.0   |Marketing |
|105    |8     |Diya   |40000.5   |Operations|
|106    |9     |Manav  |39000.25  |Admin     |
+-------+------+-------+----------+----------+

```
# Explanation:
All departments appear; missing employees are filled with null (if any dept has no employee).

# 1.4 FULL OUTER JOIN
```python
df.join(df1, "dept_id", "outer").orderBy("emp_id").show()


Output includes all matches, unmatched from both sides:

|dept_id|emp_id|name|salary|dept_name|
|101|1|Aarav|45000.75|HR|
|102|2|Isha|60000.2|IT|
|107|10|Sneha|47000.0|null|
|108|15|Neel|62000.55|null|
...

```
# Explanation:
Returns all records — matched or unmatched from both sides.

# 1.5 CROSS JOIN
```python
df.crossJoin(df1).show(5)


Output (first 5 rows):

|emp_id|name|dept_id|salary|dept_id|dept_name|location|
|1|Aarav|101|45000.75|101|HR|Mumbai|
|1|Aarav|101|45000.75|102|IT|Bangalore|
|1|Aarav|101|45000.75|103|Finance|Pune|
|1|Aarav|101|45000.75|104|Marketing|Delhi|
|1|Aarav|101|45000.75|105|Operations|Chennai|

```
# Explanation:
Cartesian product — every employee paired with every department.

# 1.6 LEFT SEMI JOIN
```
dfpython.join(df1, "dept_id", "left_semi").show()


Output:

+------+-------+-------+----------+
|emp_id|name   |dept_id|salary    |
+------+-------+-------+----------+
|1     |Aarav  |101    |45000.75  |
|2     |Isha   |102    |60000.2   |
|3     |Rohan  |101    |52000.6   |
|4     |Meera  |103    |48000.4   |
|5     |Arjun  |104    |75000.0   |
|6     |Priya  |101    |50000.0   |
|7     |Kiran  |102    |65000.9   |
|8     |Diya   |105    |40000.5   |
|9     |Manav  |106    |39000.25  |
|11    |Varun  |103    |58000.35  |
|12    |Asha   |104    |72000.1   |
|13    |Vivek  |105    |51000.0   |
|14    |Riya   |106    |43000.7   |
+------+-------+-------+----------+

```
# Explanation:
Returns only employees having a matching department (like filter inner join, but returns only left columns).

# 1.7 LEFT ANTI JOIN
```python
df.join(df1, "dept_id", "left_anti").show()


Output:

+------+----+-------+------+
|emp_id|name|dept_id|salary|
+------+----+-------+------+
|10    |Sneha|107|47000.0|
|15    |Neel |108|62000.55|
+------+----+-------+------+

```
# Explanation:
Shows employees whose department doesn’t exist in department table.

# 2. MATHEMATICAL FUNCTIONS
# 2.1 ABS()
```python
df.select("name", abs(col("salary") - 50000).alias("abs_diff")).show(5)


Output:

|name |abs_diff|
|Aarav|5000.75 |
|Isha |10000.2 |
|Rohan|2000.6  |
|Meera|2000.4  |
|Arjun|25000.0 |

```
# Explanation:
Returns the absolute (positive) value.

# 2.2 CEIL()
```python
df.select("name", ceil("salary").alias("ceil_salary")).show(5)


Output:

|Aarav|45001|
|Isha |60001|
|Rohan|52001|
|Meera|48001|
|Arjun|75000|

```
# Explanation:
Rounds up to the next integer.

# 2.3 FLOOR()
```python
df.select("name", floor("salary").alias("floor_salary")).show(5)


Output:

|Aarav|45000|
|Isha |60000|
|Rohan|52000|
|Meera|48000|
|Arjun|75000|
```

# Explanation:
Rounds down to the nearest integer.

# 2.4 EXP()
```python
df.select("name", exp(lit(1)).alias("e")).limit(1).show()


Output:

|name|e     |
|Aarav|2.71828|

```
# Explanation:
Exponential function e^x. Common for scientific calculations.

# 2.5 LOG()
```python
df.select("name", log(10, "salary").alias("log_salary")).show(3)


Output:

|Aarav|4.653|
|Isha |4.778|
|Rohan|4.716|

```
# Explanation:
Computes logarithm (base 10 here).

# 2.6 POWER()
```python
df.select("name", power("salary", 2).alias("power2")).show(3)


Output:

|Aarav|2025068.56|
|Isha |3600024.04|
|Rohan|2706249.36|

```
# Explanation:
Raises value to the given power.

# 2.7 SQRT()
```python
df.select("name", sqrt("salary").alias("root_salary")).show(3)


Output:

|Aarav|212.12|
|Isha |244.95|
|Rohan|228.00|

```
# Explanation:
Returns square root.

# 3. CONVERSION FUNCTIONS
# 3.1 CAST()
```python
df.select("name", col("salary").cast("int").alias("salary_int")).show(5)


Output:

|Aarav|45000|
|Isha |60000|
|Rohan|52000|
|Meera|48000|
|Arjun|75000|

```
# Explanation:
Converts column data type (float → int).

# 4. WINDOW FUNCTIONS
```python
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("dept_id").orderBy(col("salary").desc())
```
# 4.1 ROW_NUMBER()
```python
df.withColumn("row_num", row_number().over(windowSpec)).show(5)


Output:

|dept_id|name |salary |row_num|
|101    |Rohan|52000.6|1|
|101    |Priya|50000.0|2|
|101    |Aarav|45000.75|3|
...
```

# Explanation:
Gives unique sequential number per department.

# 4.2 RANK()
```python
df.withColumn("rank", rank().over(windowSpec)).show(5)


Output:

|dept_id|name |salary |rank|
|101    |Rohan|52000.6|1|
|101    |Priya|50000.0|2|
|101    |Aarav|45000.75|3|

```
# Explanation:
Gives ranking with gaps for ties.

# 4.3 DENSE_RANK()
```python
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show(5)

```
# Explanation:
Similar to RANK, but without gaps for ties.

# 4.4 LEAD()
```python
df.withColumn("next_salary", lead("salary").over(windowSpec)).show(5)


Output:

|dept_id|name |salary |next_salary|
|101|Rohan|52000.6|50000.0|
|101|Priya|50000.0|45000.75|
|101|Aarav|45000.75|null|
```

# Explanation:
Shows next row value within partition.

# 4.5 LAG()
```python
df.withColumn("prev_salary", lag("salary").over(windowSpec)).show(5)


Output:

|dept_id|name |salary |prev_salary|
|101|Rohan|52000.6|null|
|101|Priya|50000.0|52000.6|
|101|Aarav|45000.75|50000.0|

```
# Explanation:
Shows previous row value within partition.

# 5. ARRAY FUNCTIONS
```python
from pyspark.sql.functions import array, array_contains, size, array_position, array_remove
```
# 5.1 ARRAY()
```python
df.select("name", array("emp_id", "salary").alias("emp_array")).show(3)


Output:

|Aarav|[1,45000.75]|
|Isha |[2,60000.2]|
|Rohan|[3,52000.6]|

```
# Explanation:
Creates an array column from given columns.

# 5.2 ARRAY_CONTAINS()
```python
df.withColumn("arr", array(lit(101), lit(102), lit(103))) \
      .withColumn("check", array_contains(col("arr"), col("dept_id"))).show(3)


Output:

|Aarav|101|[101,102,103]|true|
|Isha |102|[101,102,103]|true|
|Rohan|101|[101,102,103]|true|
````

# Explanation:
Checks if array contains value.

# 5.3 ARRAY_LENGTH()
```python
df.withColumn("arr", array("emp_id", "dept_id", "salary")) \
      .withColumn("len", size("arr")).show(3)


Output:

|Aarav|[1,101,45000.75]|3|
```

# Explanation:
Returns size of array.

# 5.4 ARRAY_POSITION()
```python
df.withColumn("arr", array(lit(101), lit(102), lit(103))) \
      .withColumn("pos", array_position(col("arr"), col("dept_id"))).show(3)


Output:

|Aarav|101|[101,102,103]|1|
|Isha |102|[101,102,103]|2|
|Rohan|101|[101,102,103]|1|

```
# Explanation:
Returns position (1-based index) of element in array.

# 5.5 ARRAY_REMOVE()
```python
df.withColumn("arr", array(lit(101), lit(102), lit(103), lit(101))) \
      .withColumn("new_arr", array_remove(col("arr"), lit(101))).show(3)


Output:

|Aarav|[101,102,103,101]|[102,103]|

```
# Explanation:
Removes all occurrences of a specific element from array.
# day 6 documentation


## Explode Array and Map Functions

PySpark provides functions to transform complex data types (arrays, maps) into multiple rows.

###  `explode()`

`explode()` transforms each element in an array or map into a separate row.

**Code Example:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

data = [(1, ["apple", "banana"]), (2, ["orange", "grapes"])]
df = spark.createDataFrame(data, ["id", "fruits"])

df_exploded = df.select("id", explode("fruits").alias("fruit"))
df_exploded.show()
Output:

diff
Copy code
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  1|banana|
|  2|orange|
|  2|grapes|
+---+------+
```
# explode_outer()
explode_outer() is similar to explode(), but preserves null values instead of dropping them.

Code Example:

```python

data = [(1, ["apple", "banana"]), (2, None)]
df = spark.createDataFrame(data, ["id", "fruits"])

df_exploded_outer = df.select("id", explode_outer("fruits").alias("fruit"))
df_exploded_outer.show()
Output:

sql
Copy code
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  1|banana|
|  2|  null|
+---+------+
```
# posexplode_outer()
posexplode_outer() returns both the position (index) and value of each element in the array, including nulls.

Code Example:

```python

from pyspark.sql.functions import posexplode_outer

data = [(1, ["apple", "banana"]), (2, None)]
df = spark.createDataFrame(data, ["id", "fruits"])

df_pos_exploded = df.select("id", posexplode_outer("fruits").alias("pos", "fruit"))
df_pos_exploded.show()
Output:

sql
Copy code
+---+---+------+
| id|pos| fruit|
+---+---+------+
|  1|  0| apple|
|  1|  1|banana|
|  2|null|  null|
+---+---+------+
```
# User Defined Functions (UDF)
UDFs allow you to define custom transformations for PySpark DataFrames.

Code Example:

```python
Copy code
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(name):
    return name.upper()

upper_udf = udf(upper_case, StringType())

data = [(1, "alice"), (2, "bob")]
df = spark.createDataFrame(data, ["id", "name"])

df_with_udf = df.withColumn("name_upper", upper_udf("name"))
df_with_udf.show()
Output:

pgsql
Copy code
+---+-----+----------+
| id| name|name_upper|
+---+-----+----------+
|  1|alice|     ALICE|
|  2|  bob|       BOB|
+---+-----+----------+
```
# Different File Formats with Schema
PySpark allows defining schemas explicitly for structured data. This helps maintain consistent data types and improves performance.

# Schema Definition
You can define a schema using StructType and StructField.

# StructType
StructType represents the structure of a DataFrame.

# StructField
StructField defines individual columns, their data type, and nullability.

# DataType
DataType specifies the type of each column (StringType, IntegerType, etc.).

Code Example:

```python

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
```
# Reading Different File Formats with Schema
PySpark supports reading CSV, JSON, and Parquet files with predefined schemas.

# Reading CSV with Schema
Code Example:

```python

df_csv = spark.read.csv("data.csv", header=True, schema=schema)
df_csv.show()
Output (Example CSV):

pgsql
Copy code
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 25|
|  2|  Bob| 30|
+---+-----+---+
```
## Reading Parquet
Parquet files are optimized for big data processing.

Code Example:

```python

df_parquet = spark.read.schema(schema).parquet("data.parquet")
df_parquet.show()
```
# Reading JSON with Schema
Code Example:

```python
Copy code
df_json = spark.read.json("data.json", schema=schema)
df_json.show()
Output (Example JSON):

pgsql
Copy code
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 25|
|  2|  Bob| 30|
+---+-----+---+
```
# day 7 documentation
## 1. Writing Different File Formats with Schema

### 🔹 Definition
PySpark allows us to write DataFrames to multiple formats such as **CSV**, **Parquet**, and **JSON**.  
Defining a schema ensures **data consistency** and **avoids Spark’s automatic schema inference**, which can be slower or inaccurate.

---

### 1.1 Write DataFrame to CSV with Schema

####  Example Code
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("WriteCSVExample").getOrCreate()
```
# Define schema
```python
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
```
# Create DataFrame
```python
data = [(1, "Aarav", 50000.0), (2, "Isha", 60000.0), (3, "Rohan", 55000.0)]
df = spark.createDataFrame(data, schema)
```
# Write to CSV
```
df.write.mode("overwrite").option("header", True).csv("output/employee_csv")
```
### Output Files
bash
```python
output/employee_csv/part-0000.csv
```
### Content Example:
```python
emp_id,emp_name,salary
1,Aarav,50000.0
2,Isha,60000.0
3,Rohan,55000.0
```
💡 Why Use
Portable and easy to read.

Ideal for sharing datasets across tools like Excel or Python Pandas.

# Write DataFrame to Parquet
## Example Code
```python

# Write to Parquet format
df.write.mode("overwrite").parquet("output/employee_parquet")
```
# Read the Parquet file
```python
parquet_df = spark.read.parquet("output/employee_parquet")
parquet_df.show()
```
### Output
+------+--------+-------+
|emp_id|emp_name|salary |
+------+--------+-------+
|1     |Aarav   |50000.0|
|2     |Isha    |60000.0|
|3     |Rohan   |55000.0|
+------+--------+-------+
```
💡 Why Use
Parquet is columnar → faster read/write and smaller storage size.

Great for big data and analytics workloads.

# Write DataFrame to JSON with Schema
# Example Code
```python

# Write DataFrame as JSON
df.write.mode("overwrite").json("output/employee_json")
```
# Read JSON
```python
json_df = spark.read.json("output/employee_json")
json_df.show()
```
📊 Output
```python

+------+--------+-------+
|emp_id|emp_name|salary |
+------+--------+-------+
|1     |Aarav   |50000.0|
|2     |Isha    |60000.0|
|3     |Rohan   |55000.0|
+------+--------+-------+
```
# Why Use
JSON is human-readable and widely used for API communication.

Flexible and self-describing structure.

## Slowly Changing Dimensions (SCD)
# Definition
SCDs manage historical changes in dimension tables (e.g., Employee, Customer).
They ensure that when data changes over time, we retain previous versions or update appropriately.

# Type 1 – Overwrite Old Data
## Definition
Type 1 replaces old data with the new value — no history is kept.

## Example Code
```python

from pyspark.sql import Row
```
# Existing Dimension Table
```python
dim_df = spark.createDataFrame([
    Row(emp_id=1, name="Aarav", dept="Sales"),
    Row(emp_id=2, name="Isha", dept="IT")
])
```
# New incoming data
```python
update_df = spark.createDataFrame([
    Row(emp_id=2, name="Isha", dept="HR")  # Department changed
])
```
# Type 1 SCD - Overwrite existing record
```python
merged_df = dim_df.alias("d").join(update_df.alias("u"), "emp_id", "left") \
    .selectExpr("coalesce(u.emp_id, d.emp_id) as emp_id",
                "coalesce(u.name, d.name) as name",
                "coalesce(u.dept, d.dept) as dept")

merged_df.show()
```
### Output
```pgsql

+------+-----+-----+
|emp_id|name |dept |
+------+-----+-----+
|1     |Aarav|Sales|
|2     |Isha |HR   |
+------+-----+-----+
```
💡 Why Use
Simple and fast.

Used when no historical tracking is needed.

# Type 2 – Preserve History
## Definition
Type 2 keeps historical data by adding start_date, end_date, and current_flag columns.

# Example Code
```python

from pyspark.sql.functions import current_date, lit
```
# Existing dimension table
```python
dim_df = spark.createDataFrame([
    (1, "Aarav", "Sales", "2020-01-01", None, True),
    (2, "Isha", "IT", "2020-01-01", None, True)
], ["emp_id", "name", "dept", "start_date", "end_date", "is_current"])
```
# Incoming update
```python
update_df = spark.createDataFrame([
    (2, "Isha", "HR")  # changed department
], ["emp_id", "name", "dept"])
```
# Mark old record as not current
```python
dim_updated = dim_df.withColumn("end_date", when(dim_df.emp_id == 2, current_date())) \
                    .withColumn("is_current", when(dim_df.emp_id == 2, lit(False)).otherwise(dim_df.is_current))
```
# Add new record
```python
new_record = update_df.withColumn("start_date", current_date()) \
                      .withColumn("end_date", lit(None)) \
                      .withColumn("is_current", lit(True))

final_df = dim_updated.union(new_record)
final_df.show()
```
### Output
pgsql
```
+------+-----+-----+----------+----------+-----------+
|emp_id|name |dept |start_date|end_date  |is_current |
+------+-----+-----+----------+----------+-----------+
|1     |Aarav|Sales|2020-01-01|null      |true       |
|2     |Isha |IT   |2020-01-01|2025-11-13|false      |
|2     |Isha |HR   |2025-11-13|null      |true       |
+------+-----+-----+----------+----------+-----------+
```

💡 Why Use
Preserves full change history.

Essential for auditing and data warehousing.
```
# Type 3 – Track Limited Changes
## Definition
Type 3 keeps previous and current values in the same row (no extra history rows).

Example Code
```python

dim_df = spark.createDataFrame([
    (1, "Aarav", "Sales", None),
    (2, "Isha", "IT", None)
], ["emp_id", "name", "current_dept", "previous_dept"])

update_df = spark.createDataFrame([
    (2, "Isha", "HR")
], ["emp_id", "name", "new_dept"])
```
# Update existing record
```python
from pyspark.sql.functions import when, col

final_df = dim_df.join(update_df, "emp_id", "left") \
    .withColumn("previous_dept", when(col("new_dept").isNotNull(), col("current_dept")).otherwise(col("previous_dept"))) \
    .withColumn("current_dept", when(col("new_dept").isNotNull(), col("new_dept")).otherwise(col("current_dept"))) \
    .drop("new_dept")

final_df.show()
```
 Output
```

+------+-----+------------+-------------+
|emp_id|name |current_dept|previous_dept|
+------+-----+------------+-------------+
|1     |Aarav|Sales       |null         |
|2     |Isha |HR          |IT           |
+------+-----+------------+-------------+
```
 Why Use
Keeps limited change history (one previous version).

Useful when only the last change matters.

# Write Methods
## Overwrite
 Definition
Completely replaces existing data in the output path.

```python

df.write.mode("overwrite").parquet("output/employees")
 Use when: You want to refresh data entirely.
```
# Overwrite Partition
##  Definition
Overwrites only a specific partition instead of the entire dataset.

```python

df.write.mode("overwrite").partitionBy("dept").parquet("output/employees_partitioned")
```
 Use when: You want to update one department without rewriting all others.


# Append
## Definition
Adds new records without modifying or deleting existing data.

```python

df.write.mode("append").parquet("output/employee_parquet")
 Use when: You’re adding daily or incremental data.

```
