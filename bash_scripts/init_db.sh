for f in db_scripts/*.sql;
do
    docker exec movie-db  psql "postgresql://admin:admin123@localhost/postgres" -q -f /$f;
done