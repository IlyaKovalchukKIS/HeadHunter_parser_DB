from src.classes.dbmanager import DBManager
from config.config_func import config
from src.utils.utils import create_database, data_vacancies_company, filling_table

if __name__ == '__main__':
    id_company = [1740, 3529, 115, 733, 15478, 1057, 3776, 3127, 2748, 136929]
    id_city = None
    name_db = 'vacancies_data'
    config = config()

    print('Добро пожаловать в программу создания базы данных на основе компаний HH.\n'
          'Сбор вакансий занимает некоторое время (~1.5 МИН)')

    # data = data_vacancies_company(id_company, id_city)
    # create_database(database_name='vacancies_data', params=config)
    # filling_table(database_name=name_db, params=config, data_vacancies=data)

    while True:
        db_manager = DBManager(db_name=name_db, params=config)

        user_input = input('1 - отобразить вакансии список всех вакансий у каждой компании\n'
                           '2 - получить первые 400 вакансий в базе данных\n'
                           '3 - получить среднюю зарплату по вакансиям\n'
                           '4 - получить список всех вакансий, у которых зарплата выше средней по вакансиям\n'
                           '5 - получить список вакансий по ключевому слову\n'
                           '0 - выйти из программы\n'
                           '>>> ')

        if user_input == '0':
            db_manager.conn = 'close'
            exit()

        if user_input == '1':
            print(f'\n{db_manager.get_companies_and_vacancies_count}\n')

        if user_input == '2':
            print(f"\n{db_manager.get_all_vacancies}\n")

        if user_input == '3':
            print(f"\n{db_manager.get_avg_salary}\n")

        if user_input == '4':
            print(f"\n{db_manager.get_vacancies_with_higher_salary}\n")

        if user_input == '5':
            user_input = input('Введите ключевое слово для поиска среди вакансий\n'
                               '>>>')
            print(f"\n{db_manager.get_vacancies_with_keyword(user_input)}\n")
