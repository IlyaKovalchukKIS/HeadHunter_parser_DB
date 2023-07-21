import psycopg2
import pandas as pd
from src.classes.abstract_classes import DataBase


class DBManager:

    def __init__(self, db_name, params):
        self.__conn = psycopg2.connect(dbname=db_name, **params)

    @property
    def conn(self):
        return self.__conn

    @conn.setter
    def conn(self, param: str):
        if param.lower() == 'close':
            print('\nСоединение с базой данных закрыто\n')
            self.__conn.close()

    @property
    def get_companies_and_vacancies_count(self):
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT company.id_company, company.name, (
            SELECT COUNT(*) as count_vacancies 
            FROM vacancies 
            WHERE vacancies.id_company = company.id_company )
            FROM company
            ORDER BY count_vacancies DESC;
            """)
            result = cur.fetchall()
            df = pd.DataFrame(result, columns=['id_company', 'name', 'count_vacancies'])
            return df.to_string(index=False)

    @property
    def get_all_vacancies(self):
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT company.name, vacancies.name, vacancies.salary_from, vacancies.salary_to, vacancies.url_vacancies
            FROM vacancies
            JOIN company USING(id_company)
            ORDER BY company.name
            """)
            result = cur.fetchall()[:400]
            df = pd.DataFrame(result,
                              columns=['company_name', ' vacancies_name', 'salary_from', 'salary_to', 'url_vacancies'])
            return df.to_string(index=False)

    @property
    def get_avg_salary(self):
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT company.name, 
            (SELECT round(AVG(vacancies.salary_from)) 
            FROM vacancies 
            WHERE company.id_company = vacancies.id_company AND vacancies.currency = 'RUR') as salary_from,
            (SELECT round(AVG(vacancies.salary_to)) 
            FROM vacancies 
            WHERE company.id_company = vacancies.id_company AND vacancies.currency = 'RUR') as salary_to,
            company.url_company
            FROM company
            """)
            result = cur.fetchall()
            df = pd.DataFrame(result,
                              columns=['company_name', 'salary_from', 'salary_to', 'url_company'])
            return df.to_string(index=False)

    @property
    def get_vacancies_with_higher_salary(self):
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT id_company, id_vacancies, name, salary_from, salary_to, currency, url_vacancies FROM vacancies
            WHERE (SELECT AVG((vacancies.salary_from + vacancies.salary_to) / 2) FROM vacancies
            WHERE currency = 'RUR'
            ) < (
            (salary_from + salary_to) / 2) AND currency = 'RUR'
            ORDER BY salary_from
            """)
            result = cur.fetchall()
            df = pd.DataFrame(result,
                              columns=['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                       'url_vacancies'])
            return df.to_string(index=False)

    def get_vacancies_with_keyword(self, keywords: str):
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT id_company, id_vacancies, name, salary_from, salary_to, currency, url_vacancies FROM vacancies
            WHERE vacancies.name LIKE(%s) or vacancies.requirement LIKE(%s) or vacancies.responsibility LIKE(%s)
            ORDER BY vacancies.salary_to
            """, (keywords, keywords, keywords))
            result = cur.fetchall()
            df = pd.DataFrame(result,
                              columns=['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                       'url_vacancies'])
            return df.to_string(index=False)
