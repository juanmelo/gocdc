# gocdc
Capture Change Data Capture events from Shareplex and save into MySQL

# Configuration
- Create a new database in Mysql database
- Execute mysql_script.sql
- Update BROKERS, TOPIC, DATABASE_URL in .env file

# .env
~~~~
BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
TOPIC=gocdc
DATABASE_URL=username:password@tcp(localhost:3306)/gocdc
HTTP_ADDRESS=:8081
~~~~

# Download
- https://github.com/juanmelo/gocdc/releases
