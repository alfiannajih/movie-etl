source .env

docker exec movie-db  psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$POSTGRES_DB" -q -f /db_scripts/rollback_scripts/rollback_movie.sql -v movie_id="$1"