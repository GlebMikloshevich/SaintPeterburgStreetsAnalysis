import json
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.sql import SQL, Identifier, Placeholder
from psycopg2.pool import ThreadedConnectionPool
from ImageGathering.db_module.db_config import dbname, host


dbpool = ThreadedConnectionPool(host=host,
                                dbname=dbname,
                                user=os.environ["db_user"],
                                password=os.environ["db_password"],
                                minconn=1,
                                maxconn=1)


@contextmanager
def db_cursor():
    """provides a database cursor.
    NOTE: It might fail if you exceed the maximum connections limit

    Raises:
        e: IDK,

    Yields:
        cursor (psycopg2.extensions.cursor): a db cursor
    """
    conn = dbpool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e # NOTE: should I use logger.WARNING or ERROR messages instead of raise?
    finally:
        dbpool.putconn(conn)


def insert_camera(data: dict) -> None:
    columns = data.keys()
    values = [Placeholder(k) for k in columns]
    query = SQL("INSERT INTO cameras ({}) VALUES ({}) RETURNING id").format(
        SQL(', ').join(map(Identifier, columns)), SQL(', ').join(values))

    with db_cursor() as cursor:
        cursor.execute(query, data)


def insert_result(data: dict, save_image: bool = False):
    columns = data.keys()
    values = [Placeholder(k) for k in columns]
    data['labels'] = json.dumps(data['labels'])
    data['bboxes'] = json.dumps(data['bboxes'])
    query = SQL("INSERT INTO results ({}) VALUES ({}) RETURNING id").format(
        SQL(', ').join(map(Identifier, columns)),
        SQL(', ').join(values)
    )
    print(data)
    with db_cursor() as cursor:
        cursor.execute(query, data)


def get_cameras():
    query = SQL("SELECT url, street, crop_list, id FROM cameras")
    with db_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    camera_data = {}
    for row in rows:
        url, street, crop_list, id = row
        camera_data[url] = {
            "street": street,
            "crop_list": crop_list,
            "camera_id": id
        }

    return camera_data


# TODO remove delete_results command
# TODO add autofetching new cameras, soft removing cameras with end_data url, and successor camera if available
def delete_camera(url=None, camera_id=None):
    if not url and not camera_id:
        raise ValueError("Either 'url' or 'camera_id' must be provided.")

    query = "DELETE FROM cameras WHERE "
    params = []

    if url and camera_id:
        query += "url = %s OR id = %s;"
        params = [url, camera_id]
    elif url:
        query += "url = %s;"
        params = [url]
    else:
        query += "id = %s;"
        params = [camera_id]

    with psycopg2.connect(dbname=dbname, user=os.environ["db_user"],
                          password=os.environ["db_password"], host=host) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()


def delete_results(camera_id=None, result_id=None):
    if not camera_id and not result_id:
        raise ValueError("Either 'camera_id' or 'result_id' must be provided.")

    query = "DELETE FROM results WHERE "
    params = []

    if camera_id and result_id:
        query += "camera_id = %s OR id = %s;"
        params = [camera_id, result_id]
    elif camera_id:
        query += "camera_id = %s;"
        params = [camera_id]
    else:
        query += "id = %s;"
        params = [result_id]

    with psycopg2.connect(dbname=dbname, user=os.environ["db_user"],
                          password=os.environ["db_password"], host=host) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()