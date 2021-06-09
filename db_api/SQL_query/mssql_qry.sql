CREATE TABLE users (id nvarchar(max), emp_name nvarchar(max), emp_age nvarchar(max), sex nvarchar(max))

INSERT INTO users
VALUES
('U001','Vikas Mishra','31','M'),
('U002','Anuj','19','M'),
('U001','Meenakshi','28','M'),
('U004','Meenakshi','28','F')

UPDATE users
SET sex='F',id='U003'
WHERE emp_name='Meenakshi'

DELETE FROM users Where id='U004'

--/home/vikesh/Documents/ineuron/db_api/csv_dowloads/mssql_users.csv
select * from users