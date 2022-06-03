# Learning Data Engineering

## Project Description:
This is a simple project that showcase one of the most predominant job of a Data Engineer -- `DATA REPLICATION`. Data replication should not be confused with `Data Migration`, which in most case is **carried out once** and the **source database system** is abandoned after the task is finished.

> Data Replication often refers to the **periodic copying of data** from a data source on one platform to a destination on another one **without discarding the data source**. 

Both process does advocate for the moving data from a source(s) platform to other destination(s) platform.

## Project Objective:

#### To demonstrate a simple ETL or EtLT for Data Replication job.
![FLOWCHART](https://cloud.smartdraw.com/share.aspx/?pubDocShare=6156598A5A49A89459277F1DD49204CE23D)

**ETL --** Extraction-Transform-Load, a classical approach.

**EtLT --** Extraction-(minor transformation e.g validation checks, reduplication)-Load-Transformation. Cases including EtLT arises if the Data Analysts is charged with implementing/Transforming the business logic into the data schema.

* > Use python, to **Populate** (using the `faker library`) our source relational database (MySQL)

* > Use python, to **Extract** the data into Apache Spark, as our transformation engine

* > Use python, to **Transform** the data using Pyspark dataframe and spark SQL 

* > Use python, to **Load** the data into a Postgres database

* > Use python, to **Orchestrate** the workflow with Apache Airflow

## Technologies/Packages/Tools:
* > Relational Database -- Postgres, MySQL
* > Processing/Transformation -- Apache Spark
* > Workflow -- Apache Airflow
* > Language -- Python
  
## Setup/How to RUN the program:

1. > Install and setup Apache Airflow, Do check the internet for more information, but I'll recommend this [Medium article](https://link.medium.com/beMBbKPQxqb) for Ubuntu (all Linux actually, I'll assume :nerd_face: :nerd_face:). **Note**, _no need to install extra dependencies with airflow, `pyspark` and `sklearn` are not needed_.
   
2. > Inside the cloned repo, create your virtual env, and run `pip install -r dependencies.txt` in the activated virtual env to set up required libraries and packages.

3. > Edit the `/login_credentials/connection_profile.conf` for applicable information. This is important.

4. > Edit line 7 of `sourcing_and_setting_up_db.py` to include the **absolute path of the cloned repo**.

5. > Move only `dag.py` to `~/airflow/dags/` folder to be detectable by your Airflow Scheduler.
   
6. > If just want to read through, I'll recommend using the jupyter note `.ipynb` but if you are interested in the workflow, follow the `.py` and do tigger the dag manually in your Airflow web UI or edit `dag.py` to schedule your runs.
   
7. > To run your jupyter note in either Jupyter or VS code with your virtual env, you need to run this two lines on your bash while your virtual env is activated:
   
   `$ pip install ipykernel`
   
    `$ ipython kernel install --user --name=<any_name>`. 
    
    Remember to select the installed kernel in the Anaconda Jupyter Notebook

## Improving our project:
* > We can improve the project by expanding our source dataset and bringing Spark to it power with more complex transformation.
  
* > We can include cloud services (e.g AWS EMR, AWS AURORA) at any stage of our ETL/EtLT

Having any issues, reach me at this [email](oluwatobitobias@gmail.com). More is definitely coming, do come back to check them out.

Now go get your cup of coffee/tea and enjoy a good code-read and criticism. :+1: :+1:

