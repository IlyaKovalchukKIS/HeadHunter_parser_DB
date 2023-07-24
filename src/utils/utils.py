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


def create_database(database_name: str, params: dict, sql_scripts: dict) -> None:
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
        cur.execute(sql_scripts.get('cell1'))

    with conn.cursor() as cur:
        cur.execute(sql_scripts.get('cell2'))

    conn.commit()
    conn.close()


def filling_table(database_name: str, params: dict, sql_scripts: dict, data_vacancies: list[dict[str, Any]]) -> None:
    """Функция для заполнения базы данных"""
    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        for data in data_vacancies:
            company = data['company']
            vacancies = data['vacancies']
            cur.execute(sql_scripts.get('cell3'),
                        (company['id'], company['name'], company['description'][:300], company['alternate_url']))
            for vacancy in vacancies:
                cur.execute(sql_scripts.get('cell4'),
                            (company['id'], vacancy['id'], vacancy['name'], vacancy['salary'].get('from', None),
                             vacancy['salary'].get('to', None), vacancy['salary'].get('currency', None),
                             vacancy['alternate_url'], vacancy['snippet'].get('requirement', None),
                             vacancy['snippet'].get('responsibility', None)
                             )
                            )
    conn.commit()
    conn.close()


def get_sql_code(filename: str) -> dict:
    """Функция для считывания файла со скриптами sql и создание на основе их словаря"""
    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()

    cells = {}
    current_cell = None

    for line in content.splitlines():
        if line.startswith('--@'):
            current_cell = line[3:]
            cells[current_cell] = []
        elif current_cell is not None:
            cells[current_cell].append(line.strip())

    for key, value in cells.items():
        cells[key] = " ".join(value[1:])

    return cells
