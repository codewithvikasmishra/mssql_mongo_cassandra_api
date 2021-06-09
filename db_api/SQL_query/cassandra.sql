--creating a namespace (Think about it as a schema of RDBMS database)
CREATE KEYSPACE mishra_test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}

--Creating a table
CREATE TABLE mishra_test.emp(emp_id int PRIMARY KEY,emp_name varchar,emp_city varchar,emp_sal int,emp_phone varchar)

--Get the version of cassandra
select release_version from system.local

--Get the all keyspace_name available
SELECT keyspace_name FROM system_schema.keyspaces;

--Get all table names from a schema
SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name';
SELECT keyspace_name,table_name FROM system_schema.tables WHERE keyspace_name = 'keyspace name';

--Get all data from a table
SELECT * FROM mishra_test.insurance_claims

--inserting data into table
BEGIN BATCH
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1001, 'Supriya', 'Greater Noida', 10000, '9876543210')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1002, 'Nisha', 'East Delhi', 2000, '9076542309')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1003, 'Kanak', 'East Delhi', 15000, '9999977777')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1004, 'Shalini', 'East Delhi', 18000, '9888866666')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1005, 'Yutika', 'Benipatti', 19500, '877775555')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1006, 'Nidhi', 'South Delhi', 16000, '900000567')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1007, 'Archna', 'Gwalior', 10800, '9654300042')
INSERT INTO mishra_test.emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) Values (1008, 'Ragini', 'Uttrakhand', 17299, '6666646757')
APPLY BATCH;

--Csv import code but from possible only through cqlsh
COPY instance_name.table_name (id,emp_name,emp_age,sex) FROM '/tmp/tmpl3urcr2w/bulk_insert.csv' WITH DELIMITER=',' AND HEADER=TRUE

--Delete record
DELETE FROM mishra_test.insurance_claims where insured_zip=615688;

----------------------------------------------------------------------------------------------------------------------

SELECT keyspace_name,table_name FROM system_schema.tables WHERE keyspace_name = 'mishra_test';

--Strange but without keyspace_name in select it will not work
select keyspace_name,column_name,type from system_schema.columns where keyspace_name='mishra_test' and table_name='insurance_claims';


-- InvalidRequest: Error from server: code=2200 [Invalid query] message="SELECT DISTINCT queries must
-- only request partition key columns and/or static columns (not insured_sex)"
select distinct insured_sex from mishra_test.insurance_claims;
select distinct insured_zip from mishra_test.insurance_claims; (this will work because I partioned the data on insured_zip)
-- Code for static column
create table bank_emp_record
(
Name text,
bank_name text static,
Id int,
primary key(Name, Id)
); 


-- Adding new column in cassandra with static
ALTER table mishra_test.insurance_claims ADD crm_name text static;

-- InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot execute this query as
-- it might involve data filtering and thus may have unpredictable performance. If you want to
-- execute this query despite the performance unpredictability, use ALLOW FILTERING"
select * from mishra_test.insurance_claims where policy_number='430794'
select * from mishra_test.insurance_claims where policy_number=430794 ALLOW FILTERING;  (This query will work)

-- InvalidRequest: Error from server: code=2200 [Invalid query] message="Some partition key parts are
-- missing: insured_zip"
update mishra_test.insurance_claims set crm_name='Sachin Gupta' where policy_number=430794;

-- InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid restrictions on
-- clustering columns since the UPDATE statement modifies only static columns"
update mishra_test.insurance_claims set crm_name='Sachin Gupta' where policy_number=430794 and insured_zip=615688;

-- (this will work because we partition by insured_zip and clustered by policy_number
-- and after removing clustered by column from where condition then it will work fine)
update mishra_test.insurance_claims set crm_name='Sachin Gupta' where insured_zip=615688;

update mishra_test.insurance_claims set crm_name='Vikas Mishra' where insured_zip=615688 if crm_name='null';
--output
--[applied] | crm_name
-----------+--------------
--     False | Sachin Gupta



