import requests, json, time

# ==============================
# CONFIGURATION SECTION
# ==============================

# Polaris endpoints
POLARIS_MANAGEMENT = "http://polaris:8181/api/management/v1"
POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
AUTH = "http://polaris:8181/api/catalog/v1/oauth/tokens"

# Admin bootstrap credentials (from POLARIS_BOOTSTRAP_CREDENTIALS)
ADMIN_CLIENT_ID = "admin"
ADMIN_CLIENT_SECRET = "password"

# Catalogs to create
CATALOG_NAMES = ["lakehouse", "warehouse"]

# Principal to create (and give full access)
PRINCIPAL_NAME = "user1"

# Spark configuration defaults
SPARK_CATALOG_NAME = "polaris"
POLARIS_SPARK_CLIENT_PKG = "org.apache.polaris:polaris-spark-3.5_2.13:1.1.0-incubating"

# ==============================
# SCRIPT LOGIC
# ==============================

def authenticate():
    """Authenticate to Polaris via OAuth2 client credentials flow"""
    payload = {
        "grant_type": "client_credentials",
        "client_id": ADMIN_CLIENT_ID,
        "client_secret": ADMIN_CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL"
    }
    for _ in range(20):
        try:
            r = requests.post(AUTH, data=payload)
            if r.status_code == 200:
                print("Authenticated as admin.")
                return r.json()["access_token"]
        except Exception:
            pass
        time.sleep(3)
    raise Exception("Polaris not ready or authentication failed.")

token = authenticate()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# --- 1. Ensure catalogs exist ---
def ensure_catalog(name):
    resp = requests.get(f"{POLARIS_MANAGEMENT}/catalogs/{name}", headers=headers)
    if resp.status_code == 200:
        print(f"Catalog '{name}' already exists.")
        return

    body = {
    "catalog": {
        "name": name,
        "type": "INTERNAL",
        "properties": {"default-base-location": f"s3://{name}"},
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [f"s3://{name}/*"],
            "region": "us-east-1",
            "endpoint": "http://minio:9000",
            "pathStyleAccess": True,
            "stsUnavailable": True
        }
    }
}

    r = requests.post(f"{POLARIS_MANAGEMENT}/catalogs", headers=headers, data=json.dumps(body))
    if r.status_code in [200, 201]:
        print(f"Catalog '{name}' created.")
    elif r.status_code == 409:
        print(f"Catalog '{name}' already exists (409).")
    else:
        print(f"Catalog '{name}' creation failed: {r.text}")

for cat in CATALOG_NAMES:
    ensure_catalog(cat)

# --- 2. Ensure principal exists and has credentials ---
def ensure_principal(name):
    resp = requests.get(f"{POLARIS_MANAGEMENT}/principals/{name}", headers=headers)
    if resp.status_code == 200:
        print(f"Principal '{name}' already exists.")
        return None

    body = {
        "principal": {"name": name, "properties": {"purpose": "demo"}},
        "credentialRotationRequired": False
    }
    r = requests.post(f"{POLARIS_MANAGEMENT}/principals", headers=headers, data=json.dumps(body))

    if r.status_code == 201:
        data = r.json()
        creds = data["credentials"]
        print(f"Created principal '{name}'.")
        print(f"  clientId: {creds['clientId']}")
        print(f"  clientSecret: {creds['clientSecret']}")
        return creds
    elif r.status_code == 409:
        print(f"Principal '{name}' already exists (409).")
        return None
    else:
        raise Exception(f"Failed to create principal: {r.text}")

creds = ensure_principal(PRINCIPAL_NAME)

# Rotate credentials if existing
if creds is None:
    r = requests.post(f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}/rotate", headers=headers)
    if r.status_code == 200:
        data = r.json()
        creds = data["credentials"]
        print(f"Rotated credentials for '{PRINCIPAL_NAME}'.")
        print(f"  clientId: {creds['clientId']}")
        print(f"  clientSecret: {creds['clientSecret']}")
    else:
        raise Exception(f"Failed to rotate credentials: {r.text}")

# --- 3. Create and assign roles ---
ROLE_NAME = f"{PRINCIPAL_NAME}_role"
role_body = {"principalRole": {"name": ROLE_NAME}}
requests.post(f"{POLARIS_MANAGEMENT}/principal-roles", headers=headers, data=json.dumps(role_body))
requests.put(f"{POLARIS_MANAGEMENT}/principals/{PRINCIPAL_NAME}/principal-roles", headers=headers, data=json.dumps(role_body))
print(f"Assigned principal role '{ROLE_NAME}' to '{PRINCIPAL_NAME}'.")

for cat in CATALOG_NAMES:
    cat_role = f"{cat}_role"
    cat_role_body = {"catalogRole": {"name": cat_role}}
    requests.post(f"{POLARIS_MANAGEMENT}/catalogs/{cat}/catalog-roles", headers=headers, data=json.dumps(cat_role_body))
    requests.put(
        f"{POLARIS_MANAGEMENT}/principal-roles/{ROLE_NAME}/catalog-roles/{cat}",
        headers=headers,
        data=json.dumps(cat_role_body)
    )
    grant_body = {
        "grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}
    }
    requests.put(
        f"{POLARIS_MANAGEMENT}/catalogs/{cat}/catalog-roles/{cat_role}/grants",
        headers=headers,
        data=json.dumps(grant_body)
    )
    print(f"Granted full access on '{cat}'.")

print("\n✅ Polaris setup complete.\n")

# --- 4. Print PySpark connection settings ---
print("=== PySpark Configuration ===")
for cat in CATALOG_NAMES:
    print(f"""
# Spark configuration for catalog: {cat}
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.jars.packages", "{POLARIS_SPARK_CLIENT_PKG},org.apache.iceberg:iceberg-aws-bundle:1.10.0,io.delta:delta-spark_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}", "org.apache.polaris.spark.SparkCatalog")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.uri", "{POLARIS_CATALOG_URI}")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.warehouse", "{cat}")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.credential", "{creds['clientId']}:{creds['clientSecret']}")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.header.X-Iceberg-Access-Delegation", "vended-credentials")
    .config("spark.sql.catalog.{SPARK_CATALOG_NAME}.token-refresh-enabled", "true")
    .getOrCreate())

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.db").show()
spark.sql("CREATE TABLE IF NOT EXISTS polaris.db.example (name STRING)").show()
spark.sql("INSERT INTO polaris.db.example VALUES ('example value')").show()
spark.sql("SELECT * FROM polaris.db.example").show()