import configparser
import pymysql

# USING pathlib.Path.cwd() DOES NOT WORK BECAUSE AIRFLOW TASK INTERPRETION OF `cwd.()` is `temp/airflow/...`
# SO WE WOULD HAVE TO SET AN ABSOLUTE PATH
 
Path = '/home/oluwatobitobias/airflow_env'

from faker import Faker
faker = Faker()

parser = configparser.ConfigParser()
parser.read(f'{Path}/login_credentials/connection_profile.conf')

postgres_host = parser.get('postgres_connection', 'host')
postgres_port = parser.get('postgres_connection', 'port')
postgres_database = parser.get('postgres_connection', 'database')
postgres_user = parser.get('postgres_connection', 'user')
postgres_password = parser.get('postgres_connection', 'password')
postgres_table1 = parser.get('postgres_connection', 'table1')
postgres_table2 = parser.get('postgres_connection', 'table2')

mysql_host = parser.get('mysql_connection', 'host')
mysql_port = parser.get('mysql_connection', 'port')
mysql_database = parser.get('mysql_connection', 'database')
mysql_user = parser.get('mysql_connection', 'user')
mysql_password = parser.get('mysql_connection', 'password')
mysql_table = parser.get('mysql_connection', 'table')


if __name__ == '__main__':

    import logging 
    logger =  logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.FileHandler(f'{Path}/logs/sourcing_and_settings_up_db.log')
    logger_formatter = logging.Formatter('%(asctime)s -%(name)s %(levelname)s - %(message)s \n')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    logger.info('Logs is instatiated')

    try:
        with pymysql.connect(host=mysql_host,
                            port=int(mysql_port),
                            user=mysql_user,
                            password=mysql_password) as conn:
                            with conn.cursor() as cur:

                                try:
                                    cur.execute(f'CREATE DATABASE IF NOT EXISTS {mysql_database}')
                                except Exception:
                                    logger.error(f'Error creating database-- {mysql_database}', exc_info=True)
                                else:
                                    conn.commit()
                                    print(f'Database-- {mysql_database} created successfully')
                            
                            with conn.cursor() as cur:
                                try:
                                    cur.execute(f'USE {mysql_database}')
                                except Exception:
                                    logger.error('Error using database-- {mysql_database}', exc_info=True)
                                else:
                                    conn.commit()
                                    print(f'Connected to Databse-- {mysql_database} succesfully')

                            query1 =f'DROP TABLE IF EXISTS {mysql_table};'

                            with conn.cursor() as cur:
                                try:
                                    cur.execute(query1)
                                except Exception as err:
                                    logger.error(f'Drop table--{mysql_table}', exc_info=True)
                                    print(err)
                                else:
                                    conn.commit()
                                    print(f'Dropping table-- {mysql_table} if exist')

                            query2 = f'CREATE TABLE {mysql_table} (first_name text(50), last_name text(50), ssn varchar(30),\
                                                                    home_address varchar(250), crypto_owned text(50), wage text(20),\
                                                                    phone_number text(40), company_worked text(150), bank_account text(70),\
                                                                    occupation text(50), company_mail varchar(70), personal_email varchar(70),\
                                                                    birth_date varchar(20));'

                            with conn.cursor() as cur:
                                try:
                                    cur.execute(query2)
                                except Exception as err:
                                    logger.error(f'Error creating table-- {mysql_table}', exc_info=True)
                                    print(err)
                                else:
                                    conn.commit()
                                    print(f'Creating table-- {mysql_table} successful')
                                                
                            data = []
                            for i in range(500):
                                data.append((faker.first_name(), faker.last_name(), faker.ssn(),\
                                            faker.address(), faker.cryptocurrency_name(), faker.pricetag(),\
                                            faker.phone_number(), faker.company(), faker.bban(), faker.job(),\
                                            faker.company_email(), faker.ascii_free_email(), faker.date()))
                                i+= 1
                            
                            data_for_db = tuple(data)

                            query2 = f"INSERT INTO {mysql_table} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                            with conn.cursor() as cur:
                                try:
                                    cur.executemany(query2, data_for_db)
                                except Exception:
                                    logger.error('Error inserting into table-- {mysql_table}', exc_info=True)
                                else:
                                    conn.commit()
                                    print(f'Inserting into table-- {mysql_table} successful')

    except Exception:
        logger.error('Error connecting to database', exc_info=True)

    else:
        print('All execution successful')




    

       
        