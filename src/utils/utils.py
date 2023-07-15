from typing import Any

import requests
import psycopg2
from psycopg2 import errors


def data_vacancies_company(company_ids: list) -> list[dict[str, Any]]:
    data = []

    for company_id in company_ids:
        params = {
            "per_page": 100,
            "page": 0,
            'employer_id': company_id
        }
        headers = {
            'HH-User-Agent': 'CourseWork_5_parser_hh_to_SQL (mrlivedance@vk.com)'
        }
        response_company = requests.get(f'https://api.hh.ru/employers/{company_id}', headers=headers)
        data_company = response_company.json()
        all_vacancies = []

        while True:
            response_vacancies = requests.get("https://api.hh.ru/vacancies", params=params, headers=headers)
            data_vacancies = response_vacancies.json()
            vacancies = data_vacancies.get('items', [])
            total_pages = None
            try:
                total_pages = data_vacancies['pages'] if data_vacancies['pages'] < 20 else 19
            except KeyError:
                print(data_vacancies)

            if params['page'] == total_pages:
                data.append({'company': data_company,
                             'vacancies': all_vacancies})

                break

            params['page'] += 1
            # time.sleep(0.01)
            all_vacancies.extend(vacancies)

    return data


# start = time.perf_counter()
# res = data_vacancies_company([1740, 3529, 115, 733, 15478, 1057, 3776, 3127, 2748, 136929])
# end = time.perf_counter()
# print(f'время выполнения кода: {end - start} {[len(i["vacancies"]) for i in res]}')


def create_database_test(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о каналах и видео."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE company (
                    id_company int PRIMARY KEY,
                    name varchar(30),
                    description varchar(300),
                    url_company varchar(100) NOT NULL
                    );
        """)

    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE vacancies (
                    id_vacancies int PRIMARY KEY,
                    id_company INT REFERENCES company(id_company),
                    name varchar(100) NOT NULL ,
                    salary_from int NOT NULL,
                    salary_to int NOT NULL,
                    currency varchar(5),
                    url_vacancies varchar(100),
                    description varchar(300)
                    );
                    """)

    conn.commit()
    conn.close()
