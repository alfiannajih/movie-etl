source .env

for movie_id in "$@"
do
    psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$POSTGRES_DB" -q -f ./db_scripts/rollback_scripts/rollback_movie.sql -v movie_id="$movie_id"
done