
CREATE DATABASE c360db;
-- Replace 'your_password' with password of your choice
CREATE USER c360user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE c360db TO c360user;
ALTER DATABASE c360db OWNER TO c360user;