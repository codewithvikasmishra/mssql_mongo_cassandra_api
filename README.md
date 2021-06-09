# mssql_mongo_cassandra_api
created API's for MSSQL, Mongo and Cassandra for create, insert, update, delete, bulk-insert(csv) and download the data into csv.

<!-- How to run this code? -->
To run this code you need to look into build.sh file under db_api folder
first 3 commands will amke the images with name mssql, mongo and cassandra
the last command will create the containers and run the code and using that iplink you can test or work with postman.

<!-- How to find connection string of mssql,mongo and cassandra? -->
# MSSQL
You should have server details of mssql like server details, id and pwd. MSSQL is running on port 1433.
# Mongo
You need to create an account on https://www.mongodb.com/ with Try free.
You will create there id, password, name space and database.
# Cassandra
You need to create an account on https://www.datastax.com/products/datastax-astra with Try For Free.
You will download the secure_connect.zip folder
You will find the id and password there.

<!-- Docker files -->
You need to create docker files for 3 different services (mssql, mongo,cassandra)
for 3 docker files make 3 requiremnts.txt so all packages need not to be installed in every iamge.

Need to provide ENV and CMD in each file so from where it will start. It should contain the API file like below:
ENV FLASK_APP=cassandra_api.py
CMD ["flask", "run"]
from above these commands it will execute the cassandra_api.py

<!-- Pyodbc is challenge for docker -->
I have installed pyodbc in only mssql dockerfile only. There are two ways from where you can install pyodbc in docker.
# 1st way
Use the local image from docker as below
first pull the docker image

docker pull laudio/pyodbc

and, use below dockerfile

FROM python:3.7
FROM laudio/pyodbc:1.0.4
USER root
ENV ACCEPT_EULA=Y
MAINTAINER Vikas Kumar Mishra
COPY requirements.txt ./requirements.txt
ENV FLASK_APP=mssql_api.py
ENV FLASK_RUN_HOST=0.0.0.0
RUN apt-get install -y debconf-utils \
  && apt-get update -y \
  && apt-get -y install mssql-tools unixodbc-dev
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
COPY . /db_api/app
WORKDIR /db_api/app
CMD ["flask", "run"]

In above code ENV ACCEPT_EULA=Y is for acceptin the terms and conditions

# 2nd way
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && apt-get install -y apt-utils && apt-get clean -y
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17 \
    && ACCEPT_EULA=Y apt-get -y install mssql-tools

2nd way is recommended in productionize code.

<!-- Why 3 dockerfile and only 1 docker-compose file -->
We may have multiple docker files but docker-compose should be only one. In single docker compose file we will call every dockerfile with different ports.
