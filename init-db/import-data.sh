#!/bin/bash

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
until mysql -h localhost -u cricket_user -pcricket_password cricket_stats -e "SELECT 1" > /dev/null 2>&1; do
  sleep 1
done

echo "MySQL is ready. Importing CSV files..."

# Import team data
if [ -f /docker-entrypoint-initdb.d/team.csv ]; then
  echo "Importing team data..."
  mysql -h localhost -u cricket_user -pcricket_password cricket_stats -e "
    LOAD DATA INFILE '/docker-entrypoint-initdb.d/team.csv' 
    INTO TABLE team 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '\"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (@col1, @col2, @col3, @col4, @col5, @col6, @col7, @col8, @col9, @col10, @col11, @col12)
    SET 
      Team = @col1,
      ScoreDescending = @col2,
      Overs = @col3,
      RPO = @col4,
      Lead = @col5,
      Inns = @col6,
      Result = @col7,
      Opposition = @col8,
      Ground = @col9,
      \`Start Date\` = STR_TO_DATE(@col10, '%Y-%m-%d'),
      Declared = @col11,
      Wickets = @col12;"
fi

# Import batting data
if [ -f /docker-entrypoint-initdb.d/batting.csv ]; then
  echo "Importing batting data..."
  mysql -h localhost -u cricket_user -pcricket_password cricket_stats -e "
    LOAD DATA INFILE '/docker-entrypoint-initdb.d/batting.csv' 
    INTO TABLE batting 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '\"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (@col1, @col2, @col3, @col4, @col5, @col6, @col7, @col8, @col9, @col10, @col11, @col12)
    SET 
      Player = @col1,
      RunsDescending = @col2,
      BF = @col3,
      \`4s\` = @col4,
      \`6s\` = @col5,
      SR = @col6,
      Inns = @col7,
      Opposition = @col8,
      Ground = @col9,
      \`Start Date\` = STR_TO_DATE(@col10, '%Y-%m-%d'),
      \`Not Out\` = @col11,
      Team = @col12;"
fi

# Import bowling data
if [ -f /docker-entrypoint-initdb.d/bowling.csv ]; then
  echo "Importing bowling data..."
  mysql -h localhost -u cricket_user -pcricket_password cricket_stats -e "
    LOAD DATA INFILE '/docker-entrypoint-initdb.d/bowling.csv' 
    INTO TABLE bowling 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '\"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (@col1, @col2, @col3, @col4, @col5, @col6, @col7, @col8, @col9, @col10, @col11)
    SET 
      Player = @col1,
      Overs = @col2,
      Mdns = @col3,
      Runs = @col4,
      WktsDescending = @col5,
      Econ = @col6,
      Inns = @col7,
      Opposition = @col8,
      Ground = @col9,
      \`Start Date\` = STR_TO_DATE(@col10, '%Y-%m-%d'),
      Team = @col11;"
fi

echo "CSV import complete."