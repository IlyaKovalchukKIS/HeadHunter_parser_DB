from typing import Any
import requests
import psycopg2


def data_vacancies_company(company_ids: list[int], city_id: int = None) -> list[dict[str, Any]]:
    """Подключается по API к HH.ru, собирает компании и все вакансии этой компаний в список data"""
    data = []

    for company_id in company_ids:
        params = {
            'areas': city_id,
            "per_page": 100,
            "page": 0,
            'employer_id': company_id,
            'only_with_salary': True
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


def create_database(database_name: str, params: dict) -> None:
    """Создание базы данных и таблиц для сохранения данных о каналах и видео."""

    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT datname FROM pg_database WHERE datname = %s", (database_name,))
    result = cur.fetchone()
    if result:
        # Если база данных существует, удаляем ее
        cur.execute(f"DROP DATABASE {database_name}")

    cur.execute(f"CREATE DATABASE {database_name}")
    conn.commit()
    cur.close()
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
                    id_company INT REFERENCES company(id_company),
                    id_vacancies int PRIMARY KEY,
                    name varchar(100) NOT NULL ,
                    salary_from int,
                    salary_to int,
                    currency varchar(5),
                    url_vacancies varchar(100),
                    requirement text,
                    responsibility text
                    
                    );
                    """)

    conn.commit()
    conn.close()


def filling_table(database_name: str, params: dict, data_vacancies: list[dict[str, Any]]) -> None:
    """Функция для заполнения базы данных"""
    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        for data in data_vacancies:
            company = data['company']
            vacancies = data['vacancies']
            cur.execute('INSERT INTO company VALUES (%s, %s, %s, %s)',
                        (company['id'], company['name'], company['description'][:300], company['alternate_url']))
            for vacancy in vacancies:
                cur.execute(
                    """INSERT INTO vacancies 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_vacancies) DO NOTHING""",
                    (company['id'], vacancy['id'], vacancy['name'], vacancy['salary'].get('from', None),
                     vacancy['salary'].get('to', None), vacancy['salary'].get('currency', None),
                     vacancy['alternate_url'], vacancy['snippet'].get('requirement', None),
                     vacancy['snippet'].get('responsibility', None)
                     )
                )
    conn.commit()
    conn.close()
