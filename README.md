# ETL With Apache Spark, Apache Airflow, PostgreSQL And MySQL (Data Engineering )

## Project Description:
This is a simple project that showcase one of the most predominant ETL/ELT jobs of a Data Engineer -- `DATA REPLICATION`. Data replication should not be confused with `DATA MIGRATION`, which in most case is **carried out once** and the **source database system** is abandoned after the task is finished.

__Data Replication often refers to the `periodic copying of data` from a data source on one platform to a destination on another one `without discarding the data source`.__ 

Both process does advocate for the moving data from a source(s) platform to other destination(s) platform.


 **ETL -- Extraction-Transform-Load, a classical approach.**

 **EtLT -- Extraction-t(minor transformation e.g validation checks, reduplication)-Load-Transformation. Cases including EtLT arises if the Data Analysts is charged instead with implementing/Transforming the business logic into the data schema not the Data Engineer.**

### Level:
Early-Intermediate Friendly :sunglasses: :sunglasses:

## Project Objective:

#### To demonstrate a simple ETL or EtLT for Data Replication job.
![FLOWCHART](/assets/flow.jpg)


*  Use python, to **Populate** (using the **`faker library`**) our source relational database (MySQL)

*  Use python, to **Extract** the data into Apache Spark, as our transformation engine

*  Use python, to **Transform** the data using Pyspark dataframe and spark SQL 

*  Use python, to **Load** the data into a Postgres database

*  Use python, to **Orchestrate** the workflow with Apache Airflow

## Technologies/Packages/Tools used:
*  Relational Database -- Postgres, MySQL
*  Big Data Processing Tool -- Apache Spark
*  Workflow and Orchestration -- Apache Airflow
*  Languages -- Python and SQL

## Language requirement:
A good understanding of these syntax style in python and SQL would help:
* python try/exception block
* SQL DDL syntax
## Setup/How to RUN the program:

1.  Install and setup Apache Airflow, Do check the internet for more information, but I'll recommend this [Medium article](https://link.medium.com/beMBbKPQxqb) for Ubuntu (all Linux actually, I'll assume :nerd_face: :nerd_face:). **Note**, _no need to install extra dependencies with airflow, **`pyspark`** and **`sklearn`** are not needed_.
   
2.  Inside the cloned repo, create your virtual env, and run **`pip install -r dependencies.txt`** in the activated virtual env to set up required libraries and packages.

3.  Edit the **`/login_credentials/connection_profile.conf`** for applicable information. This is important.

4.  Edit line 7 of **`sourcing_and_setting_up_db.py`** to include the **absolute path of the cloned repo**.

5.  Move only **`dag.py`** to  **`~/airflow/dags/`** folder to be detectable by your Airflow Scheduler.
   
6.  If just want to read through, I'll recommend using the jupyter note **`.ipynb`** but if you are interested in the workflow, follow the **`.py`** and do tigger the dag manually in your Airflow web UI or edit **`dag.py`** to schedule your runs.
   
7.  To run your jupyter note in either Jupyter or VS code with your virtual env, you need to run this two lines on your bash while your virtual env is activated:
   
   > **$ pip install ipykernel**
   
   > **$ ipython kernel install --user --name=<any_name>.**
    
 Remember to select the installed kernel in the Anaconda Jupyter Notebook

## Improving our project:
*  We can improve the project by expanding our source dataset and bringing Spark to it power with more complex transformation.
  
*  We can include cloud services (e.g AWS EMR, AWS AURORA) at any stage of our ETL/EtLT


Now go get your cup of coffee/tea and enjoy a good code-read and criticism. :+1: :+1:

