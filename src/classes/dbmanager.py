from typing import Any

import psycopg2
import pandas as pd
from src.classes.abstract_classes import DataBase


class DBManager(DataBase):
    __slots__ = ['__conn']

    def __init__(self, db_name, params):
        self.__conn = psycopg2.connect(dbname=db_name, **params)

    @property
    def conn(self) -> type(psycopg2):
        return self.__conn

    @conn.setter
    def conn(self, param: str) -> None:
        if param.lower() == 'close':
            print('\nСоединение с базой данных закрыто\n')
            self.__conn.close()

    @staticmethod
    def __table_pd(data: list[tuple[Any, ...]], columns) -> str:
        df = pd.DataFrame(data, columns=columns)
        return df.to_string(index=False)

    @property
    def get_companies_and_vacancies_count(self) -> str:
        """Получает список всех компаний и количество вакансий у каждой компании."""
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
            return self.__table_pd(result, ['id_company', 'name', 'count_vacancies'])

    @property
    def get_all_vacancies(self) -> str:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT company.name, vacancies.name, vacancies.salary_from, vacancies.salary_to, vacancies.url_vacancies
            FROM vacancies
            JOIN company USING(id_company)
            ORDER BY company.name
            """)
            result = cur.fetchall()[:400]
            return self.__table_pd(result,
                                   ['company_name', ' vacancies_name', 'salary_from', 'salary_to', 'url_vacancies'])

    @property
    def get_avg_salary(self) -> str:
        """Получает среднюю зарплату по вакансиям."""
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
            return self.__table_pd(result, ['company_name', 'salary_from', 'salary_to', 'url_company'])

    @property
    def get_vacancies_with_higher_salary(self) -> str:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
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
            return self.__table_pd(result,
                                   ['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                    'url_vacancies'])

    def get_vacancies_with_keyword(self, keywords: str) -> str:
        """Получение списка вакансий по ключевому слову."""
        with self.__conn.cursor() as cur:
            cur.execute("""
            SELECT id_company, id_vacancies, name, salary_from, salary_to, currency, url_vacancies FROM vacancies
            WHERE vacancies.name ILIKE(%s) or vacancies.requirement ILIKE(%s) or vacancies.responsibility ILIKE(%s)
            ORDER BY vacancies.salary_to
            """, (f'%{keywords}%', f'%{keywords}%', f'%{keywords}%'))
            result = cur.fetchall()
            return self.__table_pd(result,
                                   ['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                    'url_vacancies'])
