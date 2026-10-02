-- Least-privilege account for Debezium: read rows for the initial snapshot, read the binlog.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
    ON *.* TO 'debezium'@'%';
-- The workload generator's account.
CREATE DATABASE IF NOT EXISTS shop;
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY 'app';
GRANT ALL ON shop.* TO 'app'@'%';
GRANT ALL ON shop_test.* TO 'app'@'%';
FLUSH PRIVILEGES;
