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
