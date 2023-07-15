from config.config_func import config
from src.utils.utils import create_database_test

config = config()
create_database_test(database_name='vacancies', params=config)
