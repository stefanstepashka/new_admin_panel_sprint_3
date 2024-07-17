from typing import Optional, Iterator, Dict, Any
import psycopg2
import psycopg2.extras
import time

import json


def get_movies_query(load_from: Optional[str]) -> str:
    """
    Формирует SQL запрос для выбора данных фильмов, обновленных после указанной даты
    """
    
    return f"""
    SELECT fw.id, fw.rating, fw.title, fw.description,
    jsonb_agg(DISTINCT jsonb_build_object('id', g.id, 'name', g.name)) AS genres,
    jsonb_agg(DISTINCT CASE WHEN fwp.role = 'director' THEN jsonb_build_object('id', p.id, 'name', p.full_name) ELSE NULL END) AS directors,
    jsonb_agg(DISTINCT CASE WHEN fwp.role = 'actor' THEN jsonb_build_object('id', p.id, 'name', p.full_name) ELSE NULL END) AS actors,
    jsonb_agg(DISTINCT CASE WHEN fwp.role = 'writer' THEN jsonb_build_object('id', p.id, 'name', p.full_name) ELSE NULL END) AS writers
    FROM content.film_work AS fw
    LEFT JOIN content.genre_film_work AS fwg ON fw.id = fwg.film_work_id
    LEFT JOIN content.genre AS g ON g.id = fwg.genre_id
    LEFT JOIN content.person_film_work AS fwp ON fw.id = fwp.film_work_id
    LEFT JOIN content.person AS p ON p.id = fwp.person_id
    GROUP BY fw.id, fw.rating, fw.title, fw.description
    HAVING GREATEST(fw.updated, MAX(g.updated), MAX(p.updated)) > '{load_from}'
    ORDER BY GREATEST(fw.updated, MAX(g.updated), MAX(p.updated)) ASC

    LIMIT 100;
    """

