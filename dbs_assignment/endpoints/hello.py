from fastapi import APIRouter

from dbs_assignment.config import settings

import psycopg2

router = APIRouter()

@router.get("/v1/status")
async def connect():
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(user=settings.POSTGRES_USER,
                                password=settings.POSTGRES_PASSWORD,
                                host=settings.POSTGRES_SERVER,
                                port=settings.POSTGRES_PORT,
                                database=settings.POSTGRES_DB)

        # create a cursor
        cur = conn.cursor()

        cur.execute('SELECT version()')

        db_version = cur.fetchone()

        cur.close()

        return {
            "version": db_version[0]
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




