
from sourcing_and_setting_up_db import (Path, postgres_host, postgres_port, postgres_user, 
                            postgres_password, postgres_database, postgres_table1, postgres_table2,
                            mysql_host, mysql_port, mysql_user, mysql_password, mysql_database, mysql_table)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import logging

logger =  logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_handler = logging.FileHandler(f'{Path}/logs/ELT_EtLT.log')
logger_formatter = logging.Formatter('%(asctime)s -%(name)s %(levelname)s - %(message)s \n')
logger_handler.setFormatter(logger_formatter)
logger.addHandler(logger_handler)
logger.info('Logs is instatiated')

if __name__ == '__main__':

    try:
        with SparkSession.builder\
            .config('spark.jars', f'{Path}/jars/postgresql-42.3.6.jar,{Path}/jars/mysql-connector-java-8.0.29.jar')\
            .appName('ELT').enableHiveSupport().getOrCreate()\
            as spark:
            try:
                try:
                    jdbcDF = spark.read.format('jdbc')\
                            .option('url', f'jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}')\
                            .option('driver', 'com.mysql.jdbc.Driver')\
                            .option('dbtable', f'{mysql_table}')\
                            .option('user', f'{mysql_user}')\
                            .option('password', f'{mysql_password}').load()
                except:
                    logger.error(f'Eror extracting from {mysql_database}.{mysql_table} with pyspark', exc_info=True)
                else:
                    print(f'Successfully extracted data from database.table-- {mysql_database}.{mysql_table}')

                jdbcDF =  jdbcDF.drop('ssn')

                dbcDF = jdbcDF.withColumn('birth_date', to_date(col('birth_date'), 'yyyy-MM-dd'))

                jdbcDF.createOrReplaceTempView('temp_table')

                personal_info_tbl =  spark.sql("SELECT `first_name` AS `first name`, `last_name` AS `last name`, `birth_date` AS `date_of_birth`, `home_address`, `phone_number`,  `personal_email` FROM TEMP_TABLE")
                business_info_tbl =  spark.sql("SELECT CONCAT(`first_name`,'   ', `last_name`) AS `full_name`, `occupation` AS `job`, `company_worked` AS `company`, `wage` AS `hourly/weekly_wage`, `crypto_owned`, `bank_account`, `company_mail` FROM TEMP_TABLE")

                try:
                    personal_info_tbl.write.format('jdbc').mode('append')\
                                            .option('url', f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}')\
                                            .option('dbtable', f'public.{postgres_table1}')\
                                            .option('user', f'{postgres_user}').option('driver', 'org.postgresql.Driver')\
                                            .option('password', f'{postgres_password}').save()  # THE `append` MODE BE REMOVED OR CHANGE TO `overwrite`
                except:
                    logger.error(f'Eror loading to {postgres_database}.public.{postgres_table1} with pyspark', exc_info=True)
                else:
                    print(f'Successfully loaded data to databse.public.table-- {postgres_database}.public.{postgres_table1}') 

                try:
                    # THE IS THE `jdbc` METHOD, AN ALTERNATIVE TO THE `save` METHOD
                    business_info_tbl.write.jdbc(f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}', f'public.{postgres_table2}', 'append',\
                    properties={'user':f'{postgres_user}', 'password': f'{postgres_password}', 'driver': 'org.postgresql.Driver'}) # THE `append` MODE BE REMOVED OR CHANGE TO `overwrite`
                except:
                    logger.error(f'Eror loading to {postgres_database}.public.{postgres_table2} with pyspark', exc_info=True)
                else:
                    print(f'Successfully loaded data to databse.public.table-- {postgres_database}.public.{postgres_table2}')     

            except:
                logger.exception('SparkSession was not successfully created')

            finally:
                if spark is not None:
                    print('Closing spark session')
                    spark.stop()
                else:
                    print('session already stopped')

    except Exception:
        logger.error('Error creating spark session', exc_info=True)
        print('Error creating spark session')



