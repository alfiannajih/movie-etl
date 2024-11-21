source .env

for f in db_scripts/init_scripts/*.sql;
do
    docker exec movie-db  psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$POSTGRES_DB" -q -f /$f;
done