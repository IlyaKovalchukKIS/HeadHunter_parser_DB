from typing import Any

import psycopg2
import pandas as pd
from src.classes.abstract_classes import DataBase


class DBManager(DataBase):
    __slots__ = ['__conn', '__sql_scripts']

    def __init__(self, db_name: str, params: dict, sql_scripts: dict) -> None:
        """
        Инициализация класса
        :param db_name: Название базы данных для подключения
        :param params: параметры подключения к базе данных
        :param sql_scripts: словарь со скриптами sql
        """
        self.__conn = psycopg2.connect(dbname=db_name, **params)
        self.__sql_scripts = sql_scripts

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
            cur.execute(self.__sql_scripts.get('cell5'))
            result = cur.fetchall()
            return self.__table_pd(result, ['id_company', 'name', 'count_vacancies'])

    @property
    def get_all_vacancies(self) -> str:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        with self.__conn.cursor() as cur:
            cur.execute(self.__sql_scripts.get('cell6'))
            result = cur.fetchall()[:400]
            return self.__table_pd(result,
                                   ['company_name', ' vacancies_name', 'salary_from', 'salary_to', 'url_vacancies'])

    @property
    def get_avg_salary(self) -> str:
        """Получает среднюю зарплату по вакансиям."""
        with self.__conn.cursor() as cur:
            cur.execute(self.__sql_scripts.get('cell7'))
            result = cur.fetchall()
            return self.__table_pd(result, ['company_name', 'salary_from', 'salary_to', 'url_company'])

    @property
    def get_vacancies_with_higher_salary(self) -> str:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with self.__conn.cursor() as cur:
            cur.execute(self.__sql_scripts.get('cell8'))
            result = cur.fetchall()
            return self.__table_pd(result,
                                   ['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                    'url_vacancies'])

    def get_vacancies_with_keyword(self, keywords: str) -> str:
        """Получение списка вакансий по ключевому слову."""
        with self.__conn.cursor() as cur:
            cur.execute(self.__sql_scripts.get('cell9'), (f'%{keywords}%', f'%{keywords}%', f'%{keywords}%'))
            result = cur.fetchall()
            return self.__table_pd(result,
                                   ['id_company', 'id_vacancies', 'name', 'salary_from', 'salary_to', 'currency',
                                    'url_vacancies'])
