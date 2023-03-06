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
        'FROM bookings.flights WHERE (((extract (epoch from ((actual_departure - scheduled_departure)/60))))::integer) > %s '
        'ORDER BY delays DESC, flight_id ASC ', [number])
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
        'flights.flight_no, '
        'boarding_passes.seat_no, '
        'flights.aircraft_code, '
        'flights.arrival_airport, '
        'flights.departure_airport, '
        'flights.scheduled_arrival, '
        'flights.scheduled_departure '
        'FROM bookings.bookings '
        'JOIN bookings.tickets ON (tickets.book_ref = bookings.book_ref)'
        'JOIN bookings.boarding_passes ON (tickets.ticket_no = boarding_passes.ticket_no)'
        'JOIN bookings.flights ON (boarding_passes.flight_id = flights.flight_id)'

        'WHERE tickets.book_ref = %s '
        'ORDER BY ticket_no ASC, boarding_no ASC', [booking_id])

    results = cur.fetchall()

    data = {"result": {"id": results[0][0], "book_date": results[0][1], "boarding_passes": []}}

    store_list = []

    for item in results:
        data['result']['boarding_passes'].append({"id":item[2], "passenger_id":item[3], "passenger_name":item[4],
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
        'FROM bookings.flights '
        'WHERE departure_airport = %s'
        'ORDER BY arrival_airport ASC', [airport])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append(item[0])

    return data


@router.get("/v1/top-airlines")
async def most_served_passengers(limit: int):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'flights.flight_no, '
            'COUNT(ticket_flights) as counter '
        'FROM bookings.flights '
        ' JOIN bookings.boarding_passes ON (flights.flight_id = boarding_passes.flight_id) '
        ' WHERE (flights.flight_id = boarding_passes.flight_id) '
        ' GROUP BY flight_no '
        ' ORDER BY counter DESC '
        ' LIMIT %s', [limit])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append({"flight_no": item[0], "count": item[1]})

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


