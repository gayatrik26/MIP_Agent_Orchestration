from fastapi import APIRouter
from src.db.db import get_db_connection
import psycopg2.extras

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])


@router.get("/latest")
def get_latest_recommendations():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT * FROM recommendation_history
        ORDER BY timestamp DESC
        LIMIT 20
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()

    return {"recommendations": results}


@router.get("/supplier/{supplier_id}")
def get_recommendations_by_supplier(supplier_id: str):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT * FROM recommendation_history
        WHERE supplier_id = %s
        ORDER BY timestamp DESC
    """, (supplier_id,))

    results = cur.fetchall()

    cur.close()
    conn.close()

    return {"supplier_id": supplier_id, "recommendations": results}
