#!/usr/bin/env bash

BETFAIR_FILE_INPUT_DIRECTORY=$1
if [ -z "${BETFAIR_FILE_INPUT_DIRECTORY}" ]
then
  echo "Please provide path to betfair input directory"
  exit 1;
fi

python ./process_betfair_files.py BETFAIR_FILE_INPUT_DIRECTORY

sudo docker run -d \
    --publish=5432:5432 \
    --name betfair_odds_data \
    --volume="$POSTGRES_HISTORICAL_ODDS_DIR":"$POSTGRES_HISTORICAL_ODDS_DIR" \
    --volume="$POSTGRES_BACKUP_DIRECTORY":"$POSTGRES_BACKUP_DIRECTORY" \
    --env POSTGRES_USER="$POSTGRES_USERNAME" \
    --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    --env POSTGRES_DB=betfair_odds_data \
    postgres

sleep 10s

python ./database_build/build_postgres_database.py

python ./database_build/add_files_to_database.py "$POSTGRES_HISTORICAL_ODDS_DIR"
