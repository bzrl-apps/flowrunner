#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER flowrunner WITH PASSWORD 'flowrunner';
	CREATE DATABASE flowrunner;
	GRANT ALL PRIVILEGES ON DATABASE flowrunner TO flowrunner;
EOSQL
