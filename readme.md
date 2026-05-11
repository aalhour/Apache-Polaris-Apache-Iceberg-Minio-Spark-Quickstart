# 🧊 Local Lakehouse Setup with Apache Polaris, MinIO, and Spark

This repository provides a ready-to-use **local data lakehouse environment** using [Apache Polaris](https://polaris.apache.org/), [MinIO](https://min.io/), and [Apache Spark](https://spark.apache.org/).

Once deployed, you’ll have:
- A local **Polaris Catalog** managing your Iceberg tables.
- A **MinIO** S3-compatible object store hosting table data.
- A **Spark environment** for running SQL queries and building pipelines.
- An automated **bootstrap script** that configures catalogs, principals, and prints ready-to-use Spark connection settings.

---

## 🚀 Prerequisites

Before starting, make sure you have:
- **Docker** and **Docker Compose** installed.
- **Internet access** to pull images.

---

## 📦 Components Overview

| Service | Description | Port |
|----------|--------------|------|
| **Polaris** | Catalog service managing Iceberg metadata and authentication. | 8181 |
| **MinIO** | S3-compatible storage for table data. | 9000 (API), 9001 (Console) |
| **Spark** | Pre-configured Jupyter Notebook + Spark 3.5 environment. | 8888 |
| **MinIO Client** | Initializes MinIO buckets. | – |

---

## ⚙️ Step 1: Start the Environment

From the root of this repo, run:

```bash
docker compose up -d
```

This will:

- Start Polaris, MinIO, Spark, and the MinIO Client.

- Pre-create the following MinIO buckets:

    - lakehouse

    - warehouse

You can check that everything is running with:

```bash
docker ps
```

To access:

- Polaris REST API: http://localhost:8181

- MinIO Console: http://localhost:9001 (User: admin, Password: password)

- Jupyter Notebook: http://localhost:8888 (inside the spark container)

## 🧰 Step 2: Bootstrap Polaris
Once the containers are running, bootstrap Polaris with the provided script.

The easiest way is to run it directly via `docker exec` (the script uses internal Docker hostnames like `polaris:8181`, so it must run inside the container network):

```bash
docker cp bootstrap.py spark:/root/bootstrap.py
docker exec spark python3 /root/bootstrap.py
```

Notes:
- Use `python3`, not `python` — the spark container does not have a `python` symlink.
- Copy to `/root/`, not `/home/jovyan/` — that path does not exist in this image.
- The script prints `clientId` and `clientSecret` at the end — save these for Step 3.

Alternatively, open http://localhost:8888, create a new notebook, and paste the full contents of `bootstrap.py` into a cell and run it.

This script will:

- Authenticate with the Polaris service.

- Create two catalogs (lakehouse, warehouse) configured for MinIO.

- Create a principal (user1) and generate credentials.

- Grant full access to both catalogs for that principal.

- Print out the PySpark configuration you can copy into your notebooks.

## 📊 Step 3: Connect Spark to Polaris
In a new notebook (http://localhost:8888), paste the configuration printed at the end of running bootstrap.py.

It will look like this (replace `<clientId>:<clientSecret>` with the values from the bootstrap output):

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.jars.packages", "org.apache.polaris:polaris-spark-3.5_2.13:1.1.0-incubating,org.apache.iceberg:iceberg-aws-bundle:1.10.0,io.delta:delta-spark_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.polaris", "org.apache.polaris.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.uri", "http://polaris:8181/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", "lakehouse")
    .config("spark.sql.catalog.polaris.credential", "<clientId>:<clientSecret>")
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.token-refresh-enabled", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .getOrCreate())
```

> **Note:** Do not set `X-Iceberg-Access-Delegation: vended-credentials` — MinIO does not support STS credential vending. Use the static S3A credentials above instead.

## 🧪 Step 4: Verify the Setup
You can now run SQL commands through Spark:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.db")
spark.sql("CREATE TABLE IF NOT EXISTS polaris.db.example (name STRING) USING iceberg")
spark.sql("INSERT INTO polaris.db.example VALUES ('example value')")
spark.sql("SELECT * FROM polaris.db.example").toPandas()
```

> **Note:** Use `.toPandas()` instead of `.show()` in Jupyter — `.show()` output goes to the Spark driver log (container stdout), not the notebook cell. Also always specify `USING iceberg` when creating tables.

You should see your table appear and the data stored in MinIO under the lakehouse bucket.

## 🧹 Step 5: Tear Down
When you’re done:

```bash
docker compose down -v
```

This stops all services and removes containers, networks, and volumes.

## 🧠 Notes
Networking: All services share the polaris-net Docker network, allowing them to resolve each other by name (polaris, minio, spark).

- Storage: The MinIO buckets persist within the container unless you remove volumes with -v.

- STSv2: stsUnavailable is set to true to disable AWS STS behavior since MinIO doesn’t support token vending.

## Author
Alex Merced
Head of DevRel @ Dremio
alexmerced.com
