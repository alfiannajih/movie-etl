include .env

start_db:
	docker start movie-db

init_db:
	bash bash_scripts/init_db.sh

test:
	coverage run --source=./src -m unittest discover -s tests/ && coverage report -m