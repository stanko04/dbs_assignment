import collections
import json

from fastapi import APIRouter

from dbs_assignment.config import settings

import psycopg2

router = APIRouter()


def connect_database():
    conn = None

    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(user=settings.POSTGRES_USER,
                                password=settings.POSTGRES_PASSWORD,
                                host=settings.POSTGRES_SERVER,
                                port=settings.POSTGRES_PORT,
                                database=settings.POSTGRES_DB)

        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


@router.get("/v1/flights/late-departure/{number}")
async def late_departure(number: int):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
        'flight_id, '
        'flight_no, '
        '((extract (epoch from ((actual_departure - scheduled_departure)/60))))::integer as delays '
        'FROM flights WHERE (((extract (epoch from ((actual_departure - scheduled_departure)/60))))::integer) > %s '
        'ORDER BY delays ASC, flight_id ASC ', [number])
    results = cur.fetchall()

    # cur.close()

    data = {"results": []}


    for item in results:
        d = collections.OrderedDict()
        # data['results'] = {'flight_id': item[0], 'flight_no': item[1], 'delay': item[2]}
        data['results'].append({'flight_id': item[0], 'flight_no': item[1], 'delay': item[2]})



    return data


@router.get("/v1/bookings/{booking_id}")
async def bookings(booking_id: str):
    conn = connect_database()

    cur = conn.cursor()

    cur.execute(
        'SELECT '
        'bookings.book_ref, '
        'bookings.book_date, '
        'tickets.ticket_no, '
        'tickets.passenger_id, '
        'tickets.passenger_name, '
        'boarding_passes.boarding_no, '
        'flights.flight_id, '
        'boarding_passes.seat_no, '
        'flights.aircraft_code, '
        'flights.arrival_airport, '
        'flights.departure_airport, '
        'flights.scheduled_arrival, '
        'flights.scheduled_departure '
        'FROM bookings '
        'JOIN tickets ON (tickets.book_ref = bookings.book_ref)'
        'JOIN boarding_passes ON (tickets.ticket_no = boarding_passes.ticket_no)'
        'JOIN ticket_flights ON (ticket_flights.ticket_no = tickets.ticket_no)'
        'JOIN flights ON (ticket_flights.flight_id = flights.flight_id)'

        'WHERE tickets.book_ref = %s', [booking_id])

    results = cur.fetchall()

    data = {"results": {"id": results[0][0], "book_date": results[0][1], "boarding_passes": []}}

    store_list = []

    for item in results:
        data['results']['boarding_passes'].append({"id":item[2], "passenger_id":item[3], "passenger_name":item[4],
                                                   "boarding_no":item[5], "flight_no":item[6], "seat":item[7],
                                                   "aircraft_code":item[8], "arrival_airport":item[9],
                                                   "departure_airport": item[10], "scheduled_arrival":item[11],
                                                   "scheduled_departure": item[12]})

    return data


@router.get("/v1/airports/{airport}/destinations")
async def arrival_airports(airport: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
	        'DISTINCT arrival_airport '
        'FROM flights '
        'WHERE departure_airport = %s', [airport])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append(item[0])

    return data


@router.get("/v1/status")
async def get_version():
    conn = None

    conn = connect_database()

    # create a cursor
    cur = conn.cursor()

    cur.execute('SELECT version()')

    db_version = cur.fetchone()

    cur.close()

    return {
        "version": db_version[0]
    }


