#!/usr/bin/env bash

sudo docker run -d \
    --publish=5432:5432 \
    --name betfair_odds_data \
    --volume=$POSTGRES_HISTORICAL_ODDS_DIR:$POSTGRES_HISTORICAL_ODDS_DIR \
    --volume=$POSTGRES_BACKUP_DIRECTORY:$POSTGRES_BACKUP_DIRECTORY \
    --env POSTGRES_USER=$POSTGRES_USERNAME \
    --env POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    --env POSTGRES_DB=betfair_odds_data \
    postgres

#  TODO(Mike): run build DB script
#  TODO(Mike): run add files to DB script
