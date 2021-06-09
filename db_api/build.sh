docker build -t sql-nosql-api:mssql /home/vikesh/Documents/ineuron/db_api -f Dockerfile.mssql

docker build -t sql-nosql-api:mongo /home/vikesh/Documents/ineuron/db_api -f Dockerfile.mongo

docker build -t sql-nosql-api:cassandra /home/vikesh/Documents/ineuron/db_api -f Dockerfile.cassandra

docker-compose up