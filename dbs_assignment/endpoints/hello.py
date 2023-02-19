from fastapi import APIRouter
import asyncio

from dbs_assignment.config import settings

import psycopg2

router = APIRouter()

@router.get("/v1/status")
async def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(user=settings.POSTGRES_USER,
                                password=settings.POSTGRES_PASSWORD,
                                host=settings.POSTGRES_SERVER,
                                port=settings.POSTGRES_PORT,
                                database=settings.POSTGRES_DB)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        # print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()

        # close the communication with the PostgreSQL
        cur.close()

        return {
            "version": db_version[0]
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




