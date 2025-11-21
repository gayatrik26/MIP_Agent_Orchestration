import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime

# ------------------------------------------------------
# PostgreSQL config (reuse your existing details)
# ------------------------------------------------------
PG_CONFIG = {
    "host": "kdnai-partnersquad-psql-dev-eastus2.postgres.database.azure.com",
    "port": 5432,
    "user": "psqladmin",
    "password": "Myserver@123",
    "database": "postgres",
    "sslmode": "require"
}


def get_pg_conn():
    """Create PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


# ------------------------------------------------------
# Fetch history from DB (df)
# ------------------------------------------------------
def fetch_history_df(days=None):
    """
    Returns a pandas DataFrame containing quality_history rows.
    If days is provided, filters for last N days.
    """

    try:
        conn = get_pg_conn()
        query = "SELECT * FROM quality_history"

        # Apply time filtering
        if days:
            query += f" WHERE timestamp >= NOW() - INTERVAL '{int(days)} days'"

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        conn.close()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Convert timestamp column to datetime for plotting
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        return df

    except Exception as e:
        print("❌ Failed loading history from DB:", e)
        return pd.DataFrame()
