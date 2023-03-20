from decimal import *
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


# Zadanie 3
@router.get("/v3/aircrafts/{aircraft_code}/seats/{seat_choice}")
def seat_choices(aircraft_code: str, seat_choice:int):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'sub.seat_no, '
            'COUNT(sub.seat_no) as \"count\" '
    'FROM( '
        'SELECT '
            'seat_no, '
            'DENSE_RANK() OVER(PARTITION BY flights.flight_id ORDER BY bookings.book_date) as \"rank\" '
        'FROM bookings.boarding_passes '
        'JOIN bookings.tickets ON(boarding_passes.ticket_no = tickets.ticket_no) '
        'JOIN bookings.flights ON(boarding_passes.flight_id = flights.flight_id) '
        'JOIN bookings.bookings ON(tickets.book_ref = bookings.book_ref) '
        'WHERE aircraft_code = %s ) AS sub '
        'WHERE sub."rank" = %s '
        'GROUP BY sub.seat_no '
        'ORDER BY "count" DESC '
        'LIMIT 1 ', [aircraft_code, seat_choice])

    results = cur.fetchall()

    cur.close()

    # data = {"results": ["seat"]}

    # for item in results:
        # data["results"] = {"seat": item[0], "count": item[1]})
    data = {"results": {"seat": results[0][0], "count": results[0][1]}}

    return data

@router.get("/v3/air-time/{book_ref}")
def time_flight(book_ref: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
	        'tickets.ticket_no, '
	        'tickets.passenger_name, '
	        'array_agg(array[f.departure_airport, f.arrival_airport, f.flight_time, f.cumulative_flight_time] ORDER BY f.actual_departure) '
        'FROM bookings.tickets '
        'JOIN ( '
            'SELECT '
	        'passenger_name, '
	        'tickets.ticket_no, '
	        'ticket_flights.flight_id, '
	        'departure_airport, '
	        'arrival_airport, '
	        'actual_departure, '
	        '(concat((EXTRACT(EPOCH FROM actual_arrival - actual_departure)/60),\'minutes\')::interval)::varchar as flight_time, '
	        '(concat((sum(EXTRACT(EPOCH FROM actual_arrival - actual_departure)/60) OVER (PARTITION BY passenger_name ORDER BY actual_departure)), \'minutes\')::interval)::varchar as cumulative_flight_time '
            'FROM bookings.tickets '
	        'JOIN bookings.ticket_flights on (tickets.ticket_no = ticket_flights.ticket_no) '
	        'JOIN bookings.flights on (ticket_flights.flight_id = flights.flight_id) '
	        'WHERE book_ref = %s '
	        'GROUP BY tickets.ticket_no, passenger_name, ticket_flights.flight_id, flights.departure_airport, flights.arrival_airport, flights.actual_departure, flights.actual_arrival '
        ') as f ON f.ticket_no = tickets.ticket_no '
        'WHERE book_ref = %s '
        ' GROUP BY tickets.ticket_no '
        ' ORDER by ticket_no ', [book_ref, book_ref])

    results = cur.fetchall()

    cur.close()

    data = {"results": []}

    for item in results:
        result_data = {"ticket_no": item[0], "passenger_name": item[1], "flights": []}
        for element in item[2]:
            result_data["flights"].append({"departure_airport": element[0], "arrival_airport": element[1],
                                           "flight_time": element[2], "total_time": element[3]})

        data["results"].append(result_data)

    return data

@router.get('/v3/airlines/{flight_no}/top_seats')
def top_seats(flight_no: str, limit: int):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'sub.seat_no, '
            'COUNT(sub.flight_id) as "count", '
            'array_agg(sub.flight_id ORDER BY flight_id), '
            'sub.sequence_identifier '
        'FROM( '
            'SELECT '
                'seat_no, '
                'boarding_passes.flight_id as flight_id, '
                'DENSE_RANK() OVER(ORDER BY seat_no, boarding_passes.flight_id) as dense_rank_result, '
                'boarding_passes.flight_id - RANK() OVER(ORDER BY seat_no, boarding_passes.flight_id) as sequence_identifier '
            'FROM bookings.boarding_passes '
            'JOIN bookings.flights ON(flights.flight_id = boarding_passes.flight_id) '
            'WHERE flight_no = %s ) AS sub '
        'GROUP BY sub.sequence_identifier, sub.seat_no '
        'ORDER BY \"count\" '
        'DESC, sub.seat_no '
        'LIMIT %s ', [flight_no, limit])

    results = cur.fetchall()

    cur.close()

    data = {"results": []}

    for item in results:
        results_data = {"seat": item[0], "flights_count": item[1], "flights": []}
        for element in item[2]:
            results_data["flights"].append(element)

        data["results"].append(results_data)

    return data

@router.get("/v3/aircrafts/{aircraft_code}/top-incomes")
def top_incomes(aircraft_code: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT sum_amount::Integer as \"amount\", to_char(\"date\", \'YYYY-MM\') as "\month\", num_day::varchar '
        'FROM( '
            'SELECT '
                'DATE_TRUNC(\'month\', actual_departure) as \"date\", '
                'DATE_TRUNC(\'day\', actual_departure) as \"day\", '
                'sum(amount) as sum_amount, '
                'extract(day from actual_departure) as \"num_day\", '
                'RANK() OVER(PARTITION BY DATE_TRUNC(\'month\', actual_departure) ORDER BY SUM(amount) DESC) as rank '
            'FROM bookings.ticket_flights '
            'JOIN bookings.flights ON(flights.flight_id = ticket_flights.flight_id) '
            'WHERE aircraft_code = %s AND flights.actual_departure IS NOT NULL '
            'GROUP BY \"date\", \"day\", \"num_day\" ) sub '
    'WHERE rank = 1 '
    'ORDER BY sum_amount DESC ', [aircraft_code])

    results = cur.fetchall()

    cur.close()

    data = {"results": []}

    for item in results:
        data["results"].append({"total_amount": item[0], "month": item[1], "day": item[2]})

    return data




# Zadanie 2

@router.get("/v1/passengers/{passenger_id}/companions")
async def get_companions(passenger_id: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
        't.passenger_id, '
        't.passenger_name, '
        'COUNT(DISTINCT second_boarding_passes.flight_id) as flights_count, '
        'ARRAY_AGG(DISTINCT second_boarding_passes.flight_id ORDER BY second_boarding_passes.flight_id ASC) as flights '
        'FROM bookings.tickets t '
        'JOIN bookings.boarding_passes first_boarding_passes ON t.ticket_no = first_boarding_passes.ticket_no '
        'JOIN (SELECT DISTINCT flight_id '
	           'FROM bookings.boarding_passes bp '
	           'JOIN bookings.tickets t ON t.ticket_no = bp.ticket_no '
	           'WHERE t.passenger_id = %s) '
        'as second_boarding_passes ON first_boarding_passes.flight_id = second_boarding_passes.flight_id '
        'WHERE t.passenger_id != %s '
        'GROUP BY t.passenger_id, t.passenger_name '
        'ORDER BY flights_count DESC, t.passenger_id ASC, flights ASC ', [passenger_id, passenger_id] )

    results = cur.fetchall()

    cur.close()

    data = {"results": []}

    for item in results:
        data['results'].append({"id": item[0], "name": item[1], "flights_count": item[2],
                                "flights": item[3]})

    return data


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
            'COUNT(boarding_passes) as counter '
        'FROM bookings.flights '
        ' JOIN bookings.boarding_passes ON (flights.flight_id = boarding_passes.flight_id) '
        ' WHERE flights.status = \'Arrived\' '
        ' GROUP BY flight_no '
        ' ORDER BY counter DESC '
        ' LIMIT %s', [limit])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append({"flight_no": item[0], "count": item[1]})

    return data


@router.get("/v1/departures")
async def scheduled_flights(airport:str, day:int):
    if(day == 7):
        day = 0

    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'flight_id, '
            ' flight_no, '
            ' scheduled_departure '
        ' FROM bookings.flights '
        ' WHERE(flights.status = \'Scheduled\') AND(extract(dow from scheduled_departure) = %s) '
        ' AND(departure_airport= %s)'
        ' ORDER BY scheduled_departure, flight_id ASC', [day, airport])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append({"flight_id": item[0], "flight_no": item[1], "scheduled_departure": item[2]})

    return data


@router.get("/v1/airlines/{flight_no}/load")
async def flight_utilization(flight_no: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'flights.flight_id, '
            'COUNT(DISTINCT(seats.seat_no)) as aircraft_capacity, '
            'COUNT(DISTINCT(ticket_flights.ticket_no)) as "load", '
            'ROUND(100.0 * (COUNT(DISTINCT(ticket_flights.ticket_no))) / (COUNT(DISTINCT(seats.seat_no))),2) as percentage '
        'FROM bookings.flights '
        'JOIN bookings.seats ON(flights.aircraft_code = seats.aircraft_code) '
        ' JOIN bookings.ticket_flights ON(flights.flight_id = ticket_flights.flight_id) '
        ' WHERE flight_no = %s '
        'GROUP BY flights.flight_id '
        'ORDER BY flights.flight_id ', [flight_no])

    results = cur.fetchall()

    data = {"results": []}

    for item in results:
        data['results'].append({"id": item[0], "aircraft_capacity": item[1],
                                "load": item[2], "percentage_load": Decimal(item[3]).normalize()})

    return data

@router.get("/v1/airlines/{flight_no}/load-week")
async def week_average(flight_no: str):
    conn = connect_database()
    # create a cursor
    cur = conn.cursor()

    cur.execute(
        'SELECT '
            'flights.flight_no, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 1 THEN tf_count * 100.0 / seats_count END),2) as monday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 2 THEN tf_count * 100.0 / seats_count END),2) as tuesday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 3 THEN tf_count * 100.0 / seats_count END),2) as wednesday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 4 THEN tf_count * 100.0 / seats_count END),2) as thursday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 5 THEN tf_count * 100.0 / seats_count END),2) as friday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 6 THEN tf_count * 100.0 / seats_count END),2) as saturday, '
            'ROUND(AVG(CASE WHEN EXTRACT(dow FROM flights.scheduled_departure) = 0 THEN tf_count * 100.0 / seats_count END),2) as sunday '
        'FROM bookings.flights '
        'JOIN (SELECT flight_id, '
	            'COUNT(*) as tf_count '
	            'FROM bookings.ticket_flights '
	            'GROUP BY flight_id) as tf ON tf.flight_id = flights.flight_id '
        'JOIN(SELECT aircraft_code, '
                'COUNT(*) as seats_count '
                'FROM bookings.seats GROUP BY aircraft_code) as s ON s.aircraft_code = flights.aircraft_code '
        'WHERE flights.flight_no = %s '
        'GROUP BY flights.flight_no ', [flight_no])

    results = cur.fetchall()

    data = {"result": {}}

    for item in results:
        data['result'] = {"flight_no": item[0], "monday": item[1],
                            "tuesday": item[2], "wednesday": item[3],
                           "thursday": item[4], "friday": item[5], "saturday": item[6], "sunday": item[7]}

    return data


# Zadanie 1

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


