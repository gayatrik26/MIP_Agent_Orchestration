import psycopg2

# ======================================================
# PostgreSQL CONNECTION DETAILS
# ======================================================
PG_CONFIG = {
    "host": "kdnai-partnersquad-psql-dev-eastus2.postgres.database.azure.com",
    "port": 5432,
    "user": "psqladmin",
    "password": "Myserver@123",
    "database": "postgres",
    "sslmode": "require"
}


# ======================================================
# Create PG connection
# ======================================================
def get_db_connection():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except Exception as e:
        print("❌ PostgreSQL connection error:", e)
        return None