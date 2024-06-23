import psycopg2
from psycopg2.sql import Placeholder, SQL, Identifier
import json
from ImageGathering.db_module.db_config import dbname, user, password, host


def insert_into_cameras(data):
    columns = data.keys()
    values = [Placeholder(k) for k in columns]

    query = SQL("INSERT INTO cameras ({}) VALUES ({}) RETURNING id").format(
        SQL(', ').join(map(Identifier, columns)),
        SQL(', ').join(values)
    )

    try:
        with psycopg2.connect(dbname=dbname, user=user, password=password, host=host) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                camera_id = cursor.fetchone()[0]
                conn.commit()
                return camera_id
    except Exception as error:
        print("Error adding camera:", error)
        print("url, description:", data.get("url"), data.get("description"))
        raise error


def insert_into_results(data):
    columns = data.keys()
    values = [Placeholder(k) for k in columns]

    # Serialize the labels and bboxes fields
    data['labels'] = json.dumps(data['labels'])
    data['bboxes'] = json.dumps(data['bboxes'])

    query = SQL("INSERT INTO results ({}) VALUES ({}) RETURNING id").format(
        SQL(', ').join(map(Identifier, columns)),
        SQL(', ').join(values)
    )

    try:
        with psycopg2.connect(dbname=dbname, user=user, password=password, host=host) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                result_id = cursor.fetchone()[0]
                conn.commit()
                return result_id
    except Exception as error:
        print("Error adding result:", error)
        print("camera_id, image:", data.get("camera_id"), data.get("image"))
        raise error


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

    with psycopg2.connect(dbname=dbname, user=user, password=password, host=host) as conn:
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

    with psycopg2.connect(dbname=dbname, user=user, password=password, host=host) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()


def get_camera_data():
    query = SQL("SELECT url, street, crop_list, id FROM cameras")

    try:
        with psycopg2.connect(dbname=dbname, user=user, password=password, host=host) as conn:
            with conn.cursor() as cursor:
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
    except Exception as error:
        print("Error fetching camera data:", error)
        raise error


