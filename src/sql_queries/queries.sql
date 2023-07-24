--@cell1
--Создает таблицу с информацией и компаниях
CREATE TABLE company
(
    id_company  int PRIMARY KEY,
    name        varchar(30),
    description varchar(300),
    url_company varchar(100) NOT NULL
);

--@cell2
--Создает таблицу с информацией о вакансиях
CREATE TABLE vacancies
(
    id_company     INT REFERENCES company (id_company),
    id_vacancies   int PRIMARY KEY,
    name           varchar(100) NOT NULL,
    salary_from    int,
    salary_to      int,
    currency       varchar(5),
    url_vacancies  varchar(100),
    requirement    text,
    responsibility text
);

--@cell3
--Заполнет таблицу данными о компаниях
INSERT INTO company VALUES (%s, %s, %s, %s);

--@cell4
--Заполняет таблицу данными о вакансиях
INSERT INTO vacancies
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_vacancies) DO NOTHING;

--@cell5
--Получает список всех компаний и количество вакансий у каждой компании.
SELECT company.id_company,
       company.name,
       (SELECT COUNT(*) as count_vacancies
        FROM vacancies
        WHERE vacancies.id_company = company.id_company)
FROM company
ORDER BY count_vacancies DESC;

--@cell6
--Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
SELECT company.name, vacancies.name, vacancies.salary_from, vacancies.salary_to, vacancies.url_vacancies
FROM vacancies
         JOIN company USING (id_company)
ORDER BY company.name;

--@cell7
--Получает среднюю зарплату по вакансиям.
SELECT company.name,
       (SELECT round(AVG(vacancies.salary_from))
        FROM vacancies
        WHERE company.id_company = vacancies.id_company
          AND vacancies.currency = 'RUR') as salary_from,
       (SELECT round(AVG(vacancies.salary_to))
        FROM vacancies
        WHERE company.id_company = vacancies.id_company
          AND vacancies.currency = 'RUR') as salary_to,
       company.url_company
FROM company;

--@cell8
--Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
SELECT id_company, id_vacancies, name, salary_from, salary_to, currency, url_vacancies
FROM vacancies
WHERE (SELECT AVG((vacancies.salary_from + vacancies.salary_to) / 2)
       FROM vacancies
       WHERE currency = 'RUR') < (
          (salary_from + salary_to) / 2)
  AND currency = 'RUR'
ORDER BY salary_from;

--@cell9
--Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например Python
SELECT id_company, id_vacancies, name, salary_from, salary_to, currency, url_vacancies
FROM vacancies
WHERE vacancies.name LIKE (%s)
   or vacancies.requirement LIKE (%s)
   or vacancies.responsibility LIKE (%s)
ORDER BY vacancies.salary_to;

