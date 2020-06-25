#!/bin/bash
set -e
# Dump postgres backup
# - cannot do the dump in compose file as it errors out, eg: 'ERROR:  role "postgres" already exists'
# - Removing 'ON_ERROR_STOP=1', continues even if error occurs.
# psql -v --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" < shared/alchemy-data.sql
# Add extension to default database 'template1'
# https://stackoverflow.com/a/11584751/5690875
psql -v -d template1 -c 'create extension "uuid-ossp";'
# Create extension "uuid-ossp"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
   CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
EOSQL
